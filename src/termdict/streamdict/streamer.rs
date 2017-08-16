#![allow(should_implement_trait)]

use std::cmp::max;
use super::TermDictionaryImpl;
use termdict::{TermStreamerBuilder, TermStreamer};
use postings::TermInfo;
use super::TermDeserializerOption;


pub(crate) fn stream_before<'a>(term_dictionary: &'a TermDictionaryImpl,
                                target_key: &[u8],
                                deserializer_option: TermDeserializerOption)
                                   -> TermStreamerImpl<'a>
{
    let (prev_key, offset) = term_dictionary.strictly_previous_key(target_key.as_ref());
    let offset: usize = offset as usize;
    TermStreamerImpl {
        cursor: &term_dictionary.stream_data()[offset..],
        current_key: Vec::from(prev_key),
        current_value: TermInfo::default(),
        term_deserializer_option: deserializer_option,
    }
}


/// See [`TermStreamerBuilder`](./trait.TermStreamerBuilder.html)
pub struct TermStreamerBuilderImpl<'a>
{
    term_dictionary: &'a TermDictionaryImpl,
    origin: usize,
    offset_from: usize,
    offset_to: usize,
    current_key: Vec<u8>,
    deserializer_option: TermDeserializerOption,
}

impl<'a> TermStreamerBuilder for TermStreamerBuilderImpl<'a>
{
    type Streamer = TermStreamerImpl<'a>;

    /// Limit the range to terms greater or equal to the bound
    fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms strictly greater than the bound
    fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.le(target_key);
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
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
            current_value: TermInfo::default(),
            term_deserializer_option: self.deserializer_option,
        }
    }
}

/// Returns offset information for the first
/// key in the stream matching a given predicate.
///
/// returns (start offset, the data required to load the value)
fn get_offset<'a, P: Fn(&[u8]) -> bool>(predicate: P,
                                        mut streamer: TermStreamerImpl<'a>)
                                           -> (usize, Vec<u8>)
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

impl<'a> TermStreamerBuilderImpl<'a>
{
    pub(crate) fn new(
        term_dictionary: &'a TermDictionaryImpl,
        deserializer_option: TermDeserializerOption) -> Self {
        let data = term_dictionary.stream_data();
        let origin = data.as_ptr() as usize;
        TermStreamerBuilderImpl {
            term_dictionary: term_dictionary,
            origin: origin,
            offset_from: 0,
            offset_to: data.len(),
            current_key: Vec::with_capacity(300),
            deserializer_option: deserializer_option,
        }
    }
}



/// See [`TermStreamer`](./trait.TermStreamer.html)
pub struct TermStreamerImpl<'a>
{
    cursor: &'a [u8],
    current_key: Vec<u8>,
    current_value: TermInfo,
    term_deserializer_option: TermDeserializerOption,
}

impl<'a> TermStreamerImpl<'a>
{
    pub(crate) fn extract_value(self) -> TermInfo {
        self.current_value
    }

    fn deserialize_value(&mut self) {
        self.current_value.doc_freq = deserialize_vint(&mut self.cursor) as u32;
        self.current_value.postings_offset = deserialize_vint(&mut self.cursor) as u32;
        if self.term_deserializer_option == TermDeserializerOption::StrWithPositions {
            self.current_value.positions_offset = deserialize_vint(&mut self.cursor) as u32;
            self.current_value.positions_inner_offset = self.cursor[0];
            self.cursor = &self.cursor[1..];
        }
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

impl<'a> TermStreamer for TermStreamerImpl<'a>
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
        self.deserialize_value();
        true
    }

    fn key(&self) -> &[u8] {
        &self.current_key
    }

    fn value(&self) -> &TermInfo {
        &self.current_value
    }
}

