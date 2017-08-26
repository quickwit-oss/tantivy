#![allow(should_implement_trait)]

use std::cmp::max;
use super::TermDictionaryImpl;
use termdict::{TermStreamerBuilder, TermStreamer};
use postings::TermInfo;
use super::delta_encoder::DeltaDecoder;


fn stream_before<'a>(term_dictionary: &'a TermDictionaryImpl,
                                target_key: &[u8],
                                has_positions: bool)
                                   -> TermStreamerImpl<'a>
{
    let (prev_key, offset) = term_dictionary.strictly_previous_key(target_key.as_ref());
    let offset: usize = offset as usize;
    TermStreamerImpl {
        cursor: &term_dictionary.stream_data()[offset..],
        delta_decoder: DeltaDecoder::with_previous_term(prev_key),
        term_info: TermInfo::default(),
        has_positions: has_positions,
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
    has_positions: bool,
}

impl<'a> TermStreamerBuilder for TermStreamerBuilderImpl<'a>
{
    type Streamer = TermStreamerImpl<'a>;

    /// Limit the range to terms greater or equal to the bound
    fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.has_positions);
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms strictly greater than the bound
    fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.has_positions);
        let smaller_than = |k: &[u8]| k.le(target_key);
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.has_positions);
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.has_positions);
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
            delta_decoder: DeltaDecoder::with_previous_term(self.current_key),
            term_info: TermInfo::default(),
            has_positions: self.has_positions,
        }
    }
}

/// Returns offset information for the first
/// key in the stream matching a given predicate.
///
/// returns
///     - the block start
///     - the index within this block
///     - the term_buffer state to initialize the block)
fn get_offset<'a, P: Fn(&[u8]) -> bool>(predicate: P,
                                        mut streamer: TermStreamerImpl<'a>)
                                           -> (usize, Vec<u8>)
{
    let mut prev: &[u8] = streamer.cursor;

    let mut prev_data: Vec<u8> = Vec::from(streamer.delta_decoder.term());

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
        has_positions: bool) -> Self {
        let data = term_dictionary.stream_data();
        let origin = data.as_ptr() as usize;
        TermStreamerBuilderImpl {
            term_dictionary: term_dictionary,
            origin: origin,
            offset_from: 0,
            offset_to: data.len(),
            current_key: Vec::with_capacity(300),
            has_positions: has_positions,
        }
    }
}



/// See [`TermStreamer`](./trait.TermStreamer.html)
pub struct TermStreamerImpl<'a>
{
    cursor: &'a [u8],
    delta_decoder: DeltaDecoder,
    term_info: TermInfo,
    has_positions: bool
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
        let suffix_length: usize = deserialize_vint(&mut self.cursor) as usize;
        self.delta_decoder.decode(common_length, &self.cursor[..suffix_length]);
        self.cursor = &self.cursor[suffix_length..];

        self.term_info.doc_freq = deserialize_vint(&mut self.cursor) as u32;
        self.term_info.postings_offset = deserialize_vint(&mut self.cursor) as u32;

        if self.has_positions {
            self.term_info.positions_offset = deserialize_vint(&mut self.cursor) as u32;
            self.term_info.positions_inner_offset = self.cursor[0];
            self.cursor = &self.cursor[1..];
        }
        true
    }

    fn key(&self) -> &[u8] {
        self.delta_decoder.term()
    }

    fn value(&self) -> &TermInfo {
        &self.term_info
    }
}

