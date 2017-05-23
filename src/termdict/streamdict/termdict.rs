#![allow(should_implement_trait)]

use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use common::VInt;
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;
use common::CountingWriter;
use std::cmp::Ordering;
use postings::TermInfo;
use fst::raw::Node;
use super::TermStreamerBuilder;
use super::streamer::stream_before;

const BLOCK_SIZE: usize = 1024;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// Builder for the new term dictionary.
///
/// All terms must be inserted in order.
pub struct TermDictionaryBuilder<W, V=TermInfo>
    where W: Write, V: BinarySerializable + Default {
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


impl<W: Write, V: BinarySerializable + Default> TermDictionaryBuilder<W, V> {

    /// Creates a new `TermDictionaryBuilder`
    pub fn new(write: W) -> io::Result<TermDictionaryBuilder<W, V>> {
        let buffer: Vec<u8> = vec!();
        Ok(TermDictionaryBuilder {
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

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()>{
        self.insert_key(key)?;
        self.insert_value(value)
    }

    /// # Warning
    /// Horribly dangerous internal API
    ///
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// Prefer using `.insert(key, value)`
    pub(crate) fn insert_key(&mut self, key: &[u8]) -> io::Result<()>{
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

    pub(crate) fn insert_value(&mut self, value: &V) -> io::Result<()>{
        value.serialize(&mut self.write)?;
        Ok(())
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
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




fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    Ok(fst::Map::from(match source {
        ReadOnlySource::Anonymous(data) => try!(Fst::from_shared_bytes(data.data, data.start, data.len).map_err(convert_fst_error)),
        ReadOnlySource::Mmap(mmap_readonly) => try!(Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)),
    }))
}

/// Datastructure to access the `terms` of a segment.
pub struct TermDictionary<V=TermInfo> where V: BinarySerializable + Default {
    stream_data: ReadOnlySource,
    fst_index: fst::Map,
    _phantom_: PhantomData<V>,
}

impl<V> TermDictionary<V>
    where V: BinarySerializable + Default {
    
    /// Opens a `TermDictionary` given a data source.
    pub fn from_source(source: ReadOnlySource)  -> io::Result<TermDictionary<V>> {
        let total_len = source.len();
        let length_offset = total_len - 8;
        let split_len: usize = {
            let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
            u64::deserialize(&mut split_len_buffer)? as  usize
        };
        let stream_data = source.slice(0, split_len);
        let fst_data = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_data)?;
        
        Ok(TermDictionary {
            stream_data: stream_data,
            fst_index: fst_index,
            _phantom_: PhantomData
        })
    }

    pub(crate) fn stream_data(&self) -> &[u8] {
        self.stream_data.as_slice()
    }

    pub(crate) fn strictly_previous_key(&self, key: &[u8]) -> (Vec<u8>, u64) {
        let fst_map = &self.fst_index;    
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

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, target_key: K) -> Option<V> {
        let mut streamer = stream_before(self, target_key.as_ref());
        while streamer.advance() {
            let position = streamer.key().cmp(target_key.as_ref());
            match position {
                Ordering::Less => {}
                Ordering::Equal => {
                    return Some(streamer.extract_value())
                }
                Ordering::Greater => {
                    return None;
                }
            }
        }
        return None;
    }


    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range(&self) -> TermStreamerBuilder<V> {
        TermStreamerBuilder::new(self)
    }
}
