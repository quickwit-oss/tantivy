#![allow(should_implement_trait)]

use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use directory::ReadOnlySource;
use common::BinarySerializable;
use common::CountingWriter;
use bincode;
use std::cmp::Ordering;
use postings::TermInfo;
use schema::FieldType;
use fst::raw::Node;
use compression::NUM_DOCS_PER_BLOCK;
use super::make_deserializer_options;
use super::TermDeserializerOption;
use super::streamer::stream_before;
use termdict::{TermDictionary, TermDictionaryBuilder, TermStreamer};
use super::{TermBlockEncoder, TermInfoBlockEncoder};
use super::{TermStreamerImpl, TermStreamerBuilderImpl};

const INDEX_INTERVAL: usize = 1024;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// See [`TermDictionaryBuilder`](./trait.TermDictionaryBuilder.html)
pub struct TermDictionaryBuilderImpl<W>
{
    write: CountingWriter<W>,

    term_block_encoder: TermBlockEncoder,
    terminfo_block_encoder: TermInfoBlockEncoder,

    block_index: fst::MapBuilder<Vec<u8>>,
    last_key: Vec<u8>,

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
            .insert(&self.last_key, self.write.written_bytes() as u64)
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
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.term_block_encoder.encode(key);
        self.len += 1;
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        let block_size = self.term_block_encoder.len();
        if block_size > 0 {
            self.write.write(&[block_size as u8])?;
            self.term_block_encoder.flush(&mut self.write)?;
            self.terminfo_block_encoder.flush(&mut self.write)?;
        }
        Ok(())
    }

    pub(crate) fn insert_value(&mut self, value: &TermInfo) -> io::Result<()> {
        self.terminfo_block_encoder.encode(value);
        if self.len % NUM_DOCS_PER_BLOCK == 0 {
            self.flush_block()?;
        }
        Ok(())
    }
}

impl<W> TermDictionaryBuilder<W> for TermDictionaryBuilderImpl<W>
    where W: Write
{
    /// Creates a new `TermDictionaryBuilder`
    fn new(mut write: W, field_type: FieldType) -> io::Result<Self> {
        let deserializer_options = make_deserializer_options(&field_type);
        {
            // serialize the field type.
            let data: Vec<u8> = bincode::serialize(&deserializer_options, bincode::Bounded(256u64))
               .expect("Failed to serialize field type within 256 bytes. This should never be a problem.");
            write.write_all(&[data.len() as u8])?;
            write.write_all(&data[..])?;
        }
        let has_positions = deserializer_options.has_positions();
        Ok(TermDictionaryBuilderImpl {
            term_block_encoder: TermBlockEncoder::new(),
            terminfo_block_encoder: TermInfoBlockEncoder::new(has_positions),
            write: CountingWriter::wrap(write),
            block_index: fst::MapBuilder::new(vec![]).expect("This cannot fail"),
            last_key: Vec::with_capacity(128),
            len: 0,
        })
    }

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
        let key = key_ref.as_ref();
        self.insert_key(key)?;
        self.insert_value(value)
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    fn finish(mut self) -> io::Result<W> {
        self.flush_block()?;
        self.add_index_entry();
        self.write.write_all(&[0u8])?;
        let (mut w, split_len) = self.write.finish()?;
        let fst_write = self.block_index.into_inner().map_err(convert_fst_error)?;
        w.write_all(&fst_write)?;
        (split_len as u64).serialize(&mut w)?;
        w.flush()?;
        Ok(w)
    }
}


fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    Ok(fst::Map::from(match source {
                          ReadOnlySource::Anonymous(data) => {
                              try!(Fst::from_shared_bytes(data.data, data.start, data.len)
                                       .map_err(convert_fst_error))
                          }
                          ReadOnlySource::Mmap(mmap_readonly) => {
                              try!(Fst::from_mmap(mmap_readonly).map_err(convert_fst_error))
                          }
                      }))
}

/// See [`TermDictionary`](./trait.TermDictionary.html)
pub struct TermDictionaryImpl
{
    stream_data: ReadOnlySource,
    fst_index: fst::Map,
    deserializer_option: TermDeserializerOption,
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
        // it won't take more than 100 bytes
        let deserialize_option_len = source.slice(0, 1).as_slice()[0] as usize;
        let deserialize_option_source = source.slice(1, 1 + deserialize_option_len);
        let deserialize_option_buffer: &[u8] = deserialize_option_source.as_slice();
        let deserializer_option: TermDeserializerOption = bincode::deserialize(deserialize_option_buffer)
            .expect("Field dictionary data is corrupted. Failed to deserialize field type.");
        source = source.slice_from(1 + deserialize_option_len);

        let total_len = source.len();
        let length_offset = total_len - 8;
        let split_len: usize = {
            let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
            u64::deserialize(&mut split_len_buffer)? as usize
        };
        let stream_data = source.slice(0, split_len);
        let fst_data = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_data)?;

        Ok(TermDictionaryImpl {
            stream_data: stream_data,
            fst_index: fst_index,
            deserializer_option: deserializer_option,
        })
    }

    /// Lookups the value corresponding to the key.
    fn get<K: AsRef<[u8]>>(&self, target_key: K) -> Option<TermInfo> {
        let mut streamer = stream_before(self, target_key.as_ref(), self.deserializer_option);
        while streamer.advance() {
            let position = streamer.key().cmp(target_key.as_ref());
            match position {
                Ordering::Less => {}
                Ordering::Equal => return Some(streamer.value().clone()),
                Ordering::Greater => {
                    return None;
                }
            }
        }
        None
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    fn range(&'a self) -> Self::StreamBuilder {
        Self::StreamBuilder::new(self, self.deserializer_option)
    }
}
