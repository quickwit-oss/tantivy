#![allow(should_implement_trait)]

use std::io::{self, Write};
use fst;

use fst::raw::Fst;
use directory::ReadOnlySource;
use common::BinarySerializable;
use common::CountingWriter;
use postings::TermInfo;
use schema::FieldType;
use super::{TermDeltaEncoder, TermInfoDeltaEncoder, DeltaTermInfo};
use fst::raw::Node;
use termdict::{TermDictionary, TermDictionaryBuilder, TermStreamer};
use super::{TermStreamerImpl, TermStreamerBuilderImpl};
use termdict::TermStreamerBuilder;
use std::mem::transmute;

const PADDING_SIZE: usize = 16;
const INDEX_INTERVAL: usize = 1024;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

fn has_positions(field_type: &FieldType) -> bool {
    match *field_type {
        FieldType::Str(ref text_options) => {
            let indexing_options = text_options.get_indexing_options();
            if indexing_options.is_position_enabled() {
                true
            }
                else {
                    false
                }
        }
        _ => {
            false
        }
    }
}

/// See [`TermDictionaryBuilder`](./trait.TermDictionaryBuilder.html)
pub struct TermDictionaryBuilderImpl<W>
{
    write: CountingWriter<W>,
    term_delta_encoder: TermDeltaEncoder,
    term_info_encoder: TermInfoDeltaEncoder,
    block_index: fst::MapBuilder<Vec<u8>>,
    len: usize,
}


fn fill_last<'a>(fst: &'a Fst, mut node: Node<'a>, buffer: &mut Vec<u8>) {
    while let Some(transition) = node.transitions().last() {
        buffer.push(transition.inp);
        node = fst.node(transition.addr);
    }
}

impl<W> TermDictionaryBuilderImpl<W>
    where W: Write
{
    fn add_index_entry(&mut self) {
        self.block_index
            .insert(&self.term_delta_encoder.term(), self.write.written_bytes() as u64)
            .unwrap();
    }

    /// # Warning
    /// Horribly dangerous internal API
    ///
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// Prefer using `.insert(key, value)`
    pub(crate) fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        if self.len % INDEX_INTERVAL == 0 {
            self.add_index_entry();
        }
        self.term_delta_encoder.encode(key);
        Ok(())
    }

    pub(crate) fn insert_value(&mut self, term_info: &TermInfo) -> io::Result<()> {
        let delta_term_info = self.term_info_encoder.encode(term_info.clone());
        let (prefix_len, suffix) = self.term_delta_encoder.prefix_suffix();
        write_term_kv(prefix_len, suffix, &delta_term_info, self.term_info_encoder.has_positions, &mut self.write)?;
        self.len += 1;
        Ok(())
    }
}

fn num_bytes_required(mut n: u32) -> u8 {
    for i in 1u8..5u8 {
        if n < 256u32 {
            return i;
        }
        else {
            n /= 256;
        }
    }
    0u8
}

fn write_term_kv<W: Write>(prefix_len: usize,
                           suffix: &[u8],
                           delta_term_info: &DeltaTermInfo,
                           has_positions: bool,
                           write: &mut W) -> io::Result<()> {
    let suffix_len = suffix.len();
    let mut code = 0u8;
    let num_bytes_docfreq = num_bytes_required(delta_term_info.doc_freq);
    let num_bytes_postings_offset = num_bytes_required(delta_term_info.delta_postings_offset);
    let num_bytes_positions_offset = num_bytes_required(delta_term_info.delta_positions_offset);
    code |= (num_bytes_docfreq - 1) << 1u8;
    code |= (num_bytes_postings_offset - 1) << 3u8;
    code |= (num_bytes_positions_offset - 1) << 5u8;
    if (prefix_len < 16) && (suffix_len < 16) {
        code |= 1u8;
        write.write_all(&[code, (prefix_len as u8) | ((suffix_len as u8) << 4u8)])?;
    }
    else {
        write.write_all(&[code])?;
        (prefix_len as u32).serialize(write)?;
        (suffix_len as u32).serialize(write)?;
    }
    write.write_all(suffix)?;
    {
        let bytes: [u8; 4] = unsafe { transmute(delta_term_info.doc_freq) };
        write.write_all(&bytes[0..num_bytes_docfreq as usize])?;
    }
    {
        let bytes: [u8; 4] = unsafe { transmute(delta_term_info.delta_postings_offset) };
        write.write_all(&bytes[0..num_bytes_postings_offset as usize])?;
    }
    if has_positions {
        let bytes: [u8; 4] = unsafe { transmute(delta_term_info.delta_positions_offset) };
        write.write_all(&bytes[0..num_bytes_positions_offset as usize])?;
        write.write_all(&[delta_term_info.positions_inner_offset])?;
    }
    Ok(())

}

impl<W> TermDictionaryBuilder<W> for TermDictionaryBuilderImpl<W>
    where W: Write
{
    /// Creates a new `TermDictionaryBuilder`
    fn new(mut write: W, field_type: FieldType) -> io::Result<Self> {
        let has_positions = has_positions(&field_type);
        let has_positions_code = if has_positions { 255u8 } else { 0u8 };
        write.write_all(&[has_positions_code])?;
        Ok(TermDictionaryBuilderImpl {
            write: CountingWriter::wrap(write),
            term_delta_encoder: TermDeltaEncoder::default(),
            term_info_encoder: TermInfoDeltaEncoder::new(has_positions),
            block_index: fst::MapBuilder::new(vec![]).expect("This cannot fail"),
            len: 0,
        })
    }

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
        let key = key_ref.as_ref();
        self.insert_key(key)?;
        self.insert_value(value)?;
        Ok(())
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    fn finish(mut self) -> io::Result<W> {
        self.write.write_all(&[0u8; PADDING_SIZE])?;
        // self.add_index_entry();
        let (mut w, split_len) = self.write.finish()?;
        let fst_write = self.block_index.into_inner().map_err(convert_fst_error)?;
        w.write_all(&fst_write)?;
        (split_len as u64).serialize(&mut w)?;
        w.flush()?;
        Ok(w)
    }
}


fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    use self::ReadOnlySource::*;
    let fst_result = match source {
        Anonymous(data) => {
            Fst::from_shared_bytes(data.data, data.start, data.len)
        }
        Mmap(mmap_readonly) => {
            Fst::from_mmap(mmap_readonly)
        }
    };
    let fst = fst_result.map_err(convert_fst_error)?;
    Ok(fst::Map::from(fst))
}

/// See [`TermDictionary`](./trait.TermDictionary.html)
pub struct TermDictionaryImpl
{
    stream_data: ReadOnlySource,
    fst_index: fst::Map,
    has_positions: bool,
}

impl TermDictionaryImpl
{
    pub(crate) fn stream_data(&self) -> &[u8] {
        self.stream_data.as_slice()
    }

    pub(crate) fn strictly_previous_key(&self, key: &[u8]) -> (Vec<u8>, u64) {
        let fst_map = &self.fst_index;
        let fst = fst_map.as_fst();
        let mut node = fst.root();
        let mut node_stack: Vec<Node> = vec![node];

        // first check the longest prefix.
        for &b in &key[..key.len() - 1] {
            node = match node.find_input(b) {
                None => {
                    break;
                }
                Some(i) => fst.node(node.transition_addr(i)),
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
            } else if cur_node.is_final() {
                // the previous key is a prefix
                let result_buffer = Vec::from(&key[..i]);
                let val = fst_map.get(&result_buffer).unwrap();
                return (result_buffer, val);
            }
        }
        (vec![], 0)
    }
}



impl<'a> TermDictionary<'a> for TermDictionaryImpl
{
    type Streamer = TermStreamerImpl<'a>;

    type StreamBuilder = TermStreamerBuilderImpl<'a>;

    /// Opens a `TermDictionary` given a data source.
    fn from_source(mut source: ReadOnlySource) -> io::Result<Self> {
        let has_positions = source.slice(0, 1).as_ref()[0] == 255u8;
        source = source.slice_from(1);

        let total_len = source.len();
        let length_offset = total_len - 8;
        let split_len: usize = {
            let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
            u64::deserialize(&mut split_len_buffer)? as usize
        };
        let stream_data = source.slice(0, split_len);
        let fst_data = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_data)?;

        let len_without_padding = stream_data.len() - PADDING_SIZE;
        Ok(TermDictionaryImpl {
            has_positions: has_positions,
            stream_data: stream_data.slice(0, len_without_padding),
            fst_index: fst_index,
        })
    }

    /// Lookups the value corresponding to the key.
    fn get<K: AsRef<[u8]>>(&self, target_key: K) -> Option<TermInfo> {
        let mut streamer = self.range()
            .ge(&target_key)
            .into_stream();
        if streamer.advance() && streamer.key() == target_key.as_ref() {
            Some(streamer.value().clone())
        }
        else {
            None
        }
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    fn range(&'a self) -> Self::StreamBuilder {
        Self::StreamBuilder::new(self, self.has_positions)
    }
}


#[cfg(test)]
mod tests {
    use super::num_bytes_required;

    #[test]
    fn test_num_bytes_required() {
        assert_eq!(num_bytes_required(0), 1);
        assert_eq!(num_bytes_required(1), 1);
        assert_eq!(num_bytes_required(255), 1);
        assert_eq!(num_bytes_required(256), 2);
        assert_eq!(num_bytes_required(u32::max_value()), 4);
    }
}