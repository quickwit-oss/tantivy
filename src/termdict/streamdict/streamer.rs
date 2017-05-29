#![allow(should_implement_trait)]

use std::cmp::max;
use common::BinarySerializable;
use super::TermDictionaryImpl;
use termdict::{TermStreamerBuilder, TermStreamer};

pub(crate) fn stream_before<'a, V>(term_dictionary: &'a TermDictionaryImpl<V>,
                                   target_key: &[u8])
                                   -> TermStreamerImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    let (prev_key, offset) = term_dictionary.strictly_previous_key(target_key.as_ref());
    let offset: usize = offset as usize;
    TermStreamerImpl {
        cursor: &term_dictionary.stream_data()[offset..],
        current_key: Vec::from(prev_key),
        current_value: V::default(),
    }
}

/// See [`TermStreamerBuilder`](./trait.TermStreamerBuilder.html)
pub struct TermStreamerBuilderImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    term_dictionary: &'a TermDictionaryImpl<V>,
    origin: usize,
    offset_from: usize,
    offset_to: usize,
    current_key: Vec<u8>,
}

impl<'a, V> TermStreamerBuilder<V> for TermStreamerBuilderImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    type Streamer = TermStreamerImpl<'a, V>;

    /// Limit the range to terms greater or equal to the bound
    fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms strictly greater than the bound
    fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| k.le(target_key);
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| k.le(target_key);
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
    }

    /// Build the streamer.
    fn into_stream(self) -> Self::Streamer {
        let data: &[u8] = self.term_dictionary.stream_data();
        let start = self.offset_from;
        let stop = max(self.offset_to, start);
        TermStreamerImpl {
            cursor: &data[start..stop],
            current_key: self.current_key,
            current_value: V::default(),
        }
    }
}

/// Returns offset information for the first
/// key in the stream matching a given predicate.
///
/// returns (start offset, the data required to load the value)
fn get_offset<'a, V, P: Fn(&[u8]) -> bool>(predicate: P,
                                           mut streamer: TermStreamerImpl<V>)
                                           -> (usize, Vec<u8>)
    where V: 'a + BinarySerializable + Default
{
    let mut prev: &[u8] = streamer.cursor;

    let mut prev_data: Vec<u8> = streamer.current_key.clone();

    while let Some((iter_key, _)) = streamer.next() {
        if !predicate(iter_key.as_ref()) {
            return (prev.as_ptr() as usize, prev_data);
        }
        prev = streamer.cursor;
        prev_data.clear();
        prev_data.extend_from_slice(iter_key.as_ref());
    }
    (prev.as_ptr() as usize, prev_data)
}

impl<'a, V> TermStreamerBuilderImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    pub(crate) fn new(term_dictionary: &'a TermDictionaryImpl<V>) -> Self {
        let data = term_dictionary.stream_data();
        let origin = data.as_ptr() as usize;
        TermStreamerBuilderImpl {
            term_dictionary: term_dictionary,
            origin: origin,
            offset_from: 0,
            offset_to: data.len(),
            current_key: Vec::with_capacity(300)
        }
    }
}

/// See [`TermStreamer`](./trait.TermStreamer.html)
pub struct TermStreamerImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    cursor: &'a [u8],
    current_key: Vec<u8>,
    current_value: V,
}


impl<'a, V: BinarySerializable> TermStreamerImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    pub(crate) fn extract_value(self) -> V {
        self.current_value
    }
}

fn deserialize_vint(data: &mut &[u8]) -> u64 {
    let mut res = 0;
    let mut shift = 0;
    for i in 0.. {
        let b = data[i];
        res |= ((b % 128u8) as u64) << shift;
        if b & 128u8 != 0u8 {
            *data = &data[(i + 1)..];
            break;
        }
        shift += 7;
    }
    res
}

impl<'a, V> TermStreamer<V> for TermStreamerImpl<'a, V>
    where V: BinarySerializable + Default
{
    fn advance(&mut self) -> bool {
        if self.cursor.is_empty() {
            return false;
        }
        let common_length: usize = deserialize_vint(&mut self.cursor) as usize;
        self.current_key.truncate(common_length);
        let added_length: usize = deserialize_vint(&mut self.cursor) as usize;
        self.current_key.extend(&self.cursor[..added_length]);
        
        self.cursor = &self.cursor[added_length..];
        self.current_value =
            V::deserialize(&mut self.cursor)
                .expect("Term dictionary corrupted. Failed to deserialize a value");
        true
    }

    fn key(&self) -> &[u8] {
        &self.current_key
    }

    fn value(&self) -> &V {
        &self.current_value
    }
}
