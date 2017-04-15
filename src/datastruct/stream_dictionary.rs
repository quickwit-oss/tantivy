#![allow(should_implement_trait)]

use std::cmp::max;
use std::io;
use std::io::Write;
use std::io::Read;
use fst;
use fst::raw::Fst;
use common::VInt;
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;
use common::CountingWriter;
use std::cmp::Ordering;
use fst::{IntoStreamer, Streamer};
use std::str;
use fst::raw::Node;
use fst::raw::CompiledAddr;

const BLOCK_SIZE: usize = 1024;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

pub struct StreamDictionaryBuilder<W: Write, V: BinarySerializable + Clone + Default> {
    write: CountingWriter<W>,
    block_index: fst::MapBuilder<Vec<u8>>,
    last_key: Vec<u8>,
    len: usize,
    _phantom_: PhantomData<V>,
}

fn common_prefix_length(left: &[u8], right: &[u8]) -> usize {
    left.iter().cloned()
        .zip(right.iter().cloned())
        .take_while(|&(b1, b2)| b1 == b2)
        .count()
}



fn fill_last<'a>(fst: &'a Fst, mut node: Node<'a>, buffer: &mut Vec<u8>) {
    loop {
        if let Some(transition) = node.transitions().last() {
            buffer.push(transition.inp);
            node = fst.node(transition.addr);
        }
        else {
            break;
        }
    }
}


fn strictly_previous_key<B: AsRef<[u8]>>(fst_map: &fst::Map, key_as_ref: B) -> (Vec<u8>, u64) {
    let key = key_as_ref.as_ref();
    let fst = fst_map.as_fst();
    let mut node = fst.root();
    let mut node_stack: Vec<Node> = vec!(node.clone());

    // first check the longest prefix.
    for &b in &key[..key.len() - 1] {
        node = match node.find_input(b) {
            None => {
                break;
            },
            Some(i) => {
                fst.node(node.transition_addr(i))
            },
        };
        node_stack.push(node);
    }
    
    let len_node_stack = node_stack.len();
    for i in (1..len_node_stack).rev() {
        let cur_node = &node_stack[i];
        let b: u8 = key[i];
        let last_transition_opt = cur_node
            .transitions()
            .take_while(|transition| transition.inp < b)
            .last();
        
        if let Some(last_transition) = last_transition_opt {
            let mut result_buffer = Vec::from(&key[..i]);
            result_buffer.push(last_transition.inp);
            let mut result = Vec::from(&key[..i]);
            result.push(last_transition.inp);
            let fork_node = fst.node(last_transition.addr);
            fill_last(fst, fork_node, &mut result);
            let val = fst_map.get(&result).unwrap();
            return (result, val);
        }
        else if cur_node.is_final() {
            // the previous key is a prefix
            let result_buffer = Vec::from(&key[..i]);
            let val = fst_map.get(&result_buffer).unwrap();
            return (result_buffer, val);
        }
    }

    return (vec!(), 0);
}


impl<W: Write, V: BinarySerializable + Clone + Default> StreamDictionaryBuilder<W, V> {

    pub fn new(write: W) -> io::Result<StreamDictionaryBuilder<W, V>> {
        let buffer: Vec<u8> = vec!();
        Ok(StreamDictionaryBuilder {
            write: CountingWriter::wrap(write),
            block_index: fst::MapBuilder::new(buffer)
                .expect("This cannot fail"),
            last_key: Vec::with_capacity(128),
            len: 0,
            _phantom_: PhantomData,
        })
    }
    
    fn add_index_entry(&mut self) {
        self.block_index.insert(&self.last_key, self.write.written_bytes() as u64).unwrap();
    }

    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()>{
        self.insert_key(key)?;
        self.insert_value(value)
    }

    pub fn insert_key(&mut self, key: &[u8]) -> io::Result<()>{
        if self.len % BLOCK_SIZE == 0 {
            self.add_index_entry();
        }
        self.len += 1;
        let common_len = common_prefix_length(key, &self.last_key);
        VInt(common_len as u64).serialize(&mut self.write)?;
        self.last_key.truncate(common_len);
        self.last_key.extend_from_slice(&key[common_len..]);
        VInt((key.len() - common_len) as u64).serialize(&mut self.write)?;
        self.write.write_all(&key[common_len..])?;
        Ok(())
    }

    pub fn insert_value(&mut self, value: &V) -> io::Result<()>{
        value.serialize(&mut self.write)?;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.add_index_entry();
        let (mut w, split_len) = self.write.finish()?;
        let fst_write = self.block_index
            .into_inner()
            .map_err(convert_fst_error)?;
        w.write(&fst_write)?;
        (split_len as u64).serialize(&mut w)?;
        w.flush()?;
        Ok(w)
    }
}



fn stream_before<'a, V: 'a + Clone + Default + BinarySerializable>(stream_dictionary: &'a StreamDictionary<V>, target_key: &[u8]) -> StreamDictionaryStreamer<'a, V> {
    let (prev_key, offset) = strictly_previous_key(&stream_dictionary.fst_index, target_key.as_ref());
    let offset: usize = offset as usize;
    StreamDictionaryStreamer {
        cursor: &stream_dictionary.stream_data.as_slice()[offset..],
        current_key: Vec::from(prev_key),
        current_value: V::default(),
    }
}


pub struct StreamDictionary<V> where V:BinarySerializable + Default + Clone {
    stream_data: ReadOnlySource,
    fst_index: fst::Map,
    _phantom_: PhantomData<V>,
}

fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    Ok(fst::Map::from(match source {
        ReadOnlySource::Anonymous(data) => try!(Fst::from_shared_bytes(data.data, data.start, data.len).map_err(convert_fst_error)),
        ReadOnlySource::Mmap(mmap_readonly) => try!(Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)),
    }))
}


impl<V: BinarySerializable + Clone + Default> StreamDictionary<V> {
    
    pub fn from_source(source: ReadOnlySource)  -> io::Result<StreamDictionary<V>> {
        let total_len = source.len();
        let length_offset = total_len - 8;
        let split_len: usize = {
            let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
            u64::deserialize(&mut split_len_buffer)? as  usize
        };
        let stream_data = source.slice(0, split_len);
        let fst_data = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_data)?;
        
        Ok(StreamDictionary {
            stream_data: stream_data,
            fst_index: fst_index,
            _phantom_: PhantomData
        })
    }

    pub fn get<K: AsRef<[u8]>>(&self, target_key: K) -> Option<V> {
        let mut streamer = stream_before(self, target_key.as_ref());
        while let Some((iter_key, iter_val)) = streamer.next() {
            match iter_key.cmp(target_key.as_ref()) {
                Ordering::Less => {}
                Ordering::Equal => {
                    let val: V = (*iter_val).clone();
                    return Some(val);
                }
                Ordering::Greater => {
                    return None;
                }
            }
        }
        return None;
    }
    
    pub fn range(&self) -> StreamDictionaryStreamerBuilder<V> {
        let data: &[u8] = &self.stream_data;
        StreamDictionaryStreamerBuilder {
            stream_dictionary: &self,
            offset_from: 0,
            offset_to: (data.as_ptr() as usize) + data.len(),
            current_key: vec!(),
        }
    }

    pub fn stream(&self) -> StreamDictionaryStreamer<V> {
        StreamDictionaryStreamer {
            cursor: &*self.stream_data,
            current_key: Vec::with_capacity(128),
            current_value: V::default(),
        }
    }
}

pub struct StreamDictionaryStreamerBuilder<'a, V: 'a + BinarySerializable + Clone + Default> {
    stream_dictionary: &'a StreamDictionary<V>,
    offset_from: usize,
    offset_to: usize,
    current_key: Vec<u8>,
}

fn get_offset<'a, V, P: Fn(&[u8])->bool>(predicate: P, mut streamer: StreamDictionaryStreamer<V>) -> (usize, Vec<u8>, usize)
    where V: 'a + BinarySerializable + Clone + Default {
    // let mut streamer = stream_dictionary.stream_before(target_key.as_ref());
    let mut prev: &[u8] = streamer.cursor;
    let mut prev_data: Vec<u8> = vec!();
    while let Some((iter_key, _)) = streamer.next() {
        if !predicate(iter_key) {
            return (prev.as_ptr() as usize, prev_data, streamer.cursor.as_ptr() as usize);
        }
        prev = streamer.cursor;
        prev_data.clear();
        prev_data.extend_from_slice(iter_key);
    }
    return (prev.as_ptr() as usize, prev_data, prev.as_ptr() as usize);
}

impl<'a, V: 'a + BinarySerializable + Clone + Default> StreamDictionaryStreamerBuilder<'a, V> {
    pub fn ge(mut self, target_key: &[u8]) -> StreamDictionaryStreamerBuilder<'a, V> {
        let streamer = stream_before(&self.stream_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.lt(target_key) };
        let (offset_before, current_key, _) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before;
        self
    }

    pub fn gt(mut self, target_key: &[u8]) -> StreamDictionaryStreamerBuilder<'a, V> {
        let streamer = stream_before(self.stream_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.le(target_key) };
        let (offset_before, current_key, _) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before;
        self
    }

    pub fn lt(mut self, target_key: &[u8]) -> StreamDictionaryStreamerBuilder<'a, V> {
        let streamer = stream_before(self.stream_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.le(target_key) };
        let (_, _, offset_after) = get_offset(smaller_than, streamer);
        self.offset_to = offset_after;
        self
    }

    pub fn le(mut self, target_key: &[u8]) -> StreamDictionaryStreamerBuilder<'a, V> {
        let streamer = stream_before(self.stream_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.lt(target_key) };
        let (_, _, offset_after) = get_offset(smaller_than, streamer);
        self.offset_to = offset_after;
        self
    }

    pub fn into_stream(self) -> StreamDictionaryStreamer<'a, V> {
        let data: &[u8] = &self.stream_dictionary.stream_data.as_slice()[..];
        let origin = data.as_ptr() as usize;
        let start = self.offset_from - origin;
        let stop = max(self.offset_to - origin, start);
        StreamDictionaryStreamer {
            cursor: &data[start..stop],
            current_key: self.current_key,
            current_value: V::default(),
        }
    }
}

pub struct StreamDictionaryStreamer<'a, V: BinarySerializable> {
    cursor: &'a [u8],
    current_key: Vec<u8>,
    current_value: V,
}

impl<'a, V: BinarySerializable> StreamDictionaryStreamer<'a, V> {
    
    pub fn next(&mut self) -> Option<(&[u8], &V)> {
        if self.cursor.len() == 0 {
            return None;
        }
        let common_length: usize = VInt::deserialize(&mut self.cursor).unwrap().0 as usize;
        let new_length: usize = common_length + VInt::deserialize(&mut self.cursor).unwrap().0 as usize;
        self.current_key.reserve(new_length); // TODO check if we can do better.
        unsafe {
            self.current_key.set_len(new_length);
        }
        self.cursor.read_exact(&mut self.current_key[common_length..new_length]).unwrap();
        self.current_value = V::deserialize(&mut self.cursor).unwrap();
        Some((&self.current_key, &self.current_value))
    }


    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    pub fn value(&self) -> &V {
        &self.current_value
    }
}

#[cfg(test)]
mod test {
    
    use std::str;
    use directory::ReadOnlySource;
    use super::CountingWriter;
    use std::io::Write;
    use super::{BLOCK_SIZE, StreamDictionary, StreamDictionaryBuilder};

    #[test]
    fn test_stream_dictionary() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let buffer: Vec<u8> = {
            let mut stream_dictionary_builder = StreamDictionaryBuilder::new(vec!()).unwrap();
            for &(ref id, ref i) in &ids {
                stream_dictionary_builder.insert(id.as_bytes(), i).unwrap();
            }
            stream_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let stream_dictionary: StreamDictionary<u32> = StreamDictionary::from_source(source).unwrap();
        {
            let mut streamer = stream_dictionary.stream();
            let mut i = 0;
            while let Some((streamer_k, streamer_v)) = streamer.next() {
                let &(ref key, ref v) = &ids[i];
                assert_eq!(streamer_k, key.as_bytes());
                assert_eq!(streamer_v, v);
                i += 1;
            }
        }
        
        let &(ref key, ref _v) = &ids[2047];
        stream_dictionary.get(key.as_bytes());
    }

    #[test]
    fn test_stream_range() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let buffer: Vec<u8> = {
            let mut stream_dictionary_builder = StreamDictionaryBuilder::new(vec!()).unwrap();
            for &(ref id, ref i) in &ids {
                stream_dictionary_builder.insert(id.as_bytes(), i).unwrap();
            }
            stream_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);

        let stream_dictionary: StreamDictionary<u32> = StreamDictionary::from_source(source).unwrap();
        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                let &(ref target_key, _) = &ids[i];
                let mut streamer = stream_dictionary
                    .range()
                    .ge(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j];
                    assert_eq!(str::from_utf8(streamer_k).unwrap(), key);
                    assert_eq!(streamer_v, v);
                }
            }
        }

        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                let &(ref target_key, _) = &ids[i];
                let mut streamer = stream_dictionary
                    .range()
                    .gt(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j + 1];
                    assert_eq!(streamer_k, key.as_bytes());
                    assert_eq!(streamer_v, v);
                }
            }
        }

        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                for j in 0..3 {
                    let &(ref fst_key, _) = &ids[i];
                    let &(ref last_key, _) = &ids[i + 3];
                    let mut streamer = stream_dictionary
                        .range()
                        .ge(fst_key.as_bytes())
                        .lt(last_key.as_bytes())
                        .into_stream();
                    for _ in 0..(j + 1) {
                        assert!(streamer.next().is_some());
                    }
                    assert!(streamer.next().is_some());
                }
            }
        }
    }

    
}
