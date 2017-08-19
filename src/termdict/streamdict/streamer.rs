#![allow(should_implement_trait)]

use std::cmp::max;
use super::TermDictionaryImpl;
use termdict::{TermStreamerBuilder, TermStreamer};
use super::{TermBlockDecoder, TermInfoBlockDecoder};
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
        remaining_in_block: 0,
        term_block_decoder: TermBlockDecoder::given_previous_term(&prev_key[..]),
        terminfo_block_decoder: TermInfoBlockDecoder::new(deserializer_option.has_positions()),
        cursor: &term_dictionary.stream_data()[offset..],
    }
}


/// See [`TermStreamerBuilder`](./trait.TermStreamerBuilder.html)
pub struct TermStreamerBuilderImpl<'a>
{
    term_dictionary: &'a TermDictionaryImpl,
    block_start: &'a [u8],
    origin: usize,
    cursor: usize,
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
        unimplemented!();
        /*
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (block_start, cursor,  current_key) = get_offset(smaller_than, streamer);
        self.block_start = block_start;
        self.current_key = current_key;
        self.cursor = cursor;
        //self.offset_from = ;
        */
        self
    }

    /// Limit the range to terms strictly greater than the bound
    fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        unimplemented!();
        /*
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.le(target_key);
        let (block_start, cursor,  current_key) = get_offset(smaller_than, streamer);
        self.block_start = block_start;
        self.current_key = current_key;
        self.cursor = cursor;
        //self.offset_from = offset_before - self.origin;
        */
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        unimplemented!();
        /*
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.lt(target_key);
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
        */
    }

    /// Limit the range to terms lesser or equal to the bound
    fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        unimplemented!();
        /*
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref(), self.deserializer_option);
        let smaller_than = |k: &[u8]| k.le(target_key);
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
        */
    }

    /// Build the streamer.
    fn into_stream(self) -> Self::Streamer {
        let data: &[u8] = self.term_dictionary.stream_data();
        let start = self.offset_from;
        let stop = max(self.offset_to, start);
        println!("current_key {:?}", self.current_key);
        TermStreamerImpl {
            remaining_in_block: 0,
            cursor: &data[start..stop],
            term_block_decoder: TermBlockDecoder::given_previous_term(&self.current_key),
            terminfo_block_decoder: TermInfoBlockDecoder::new(self.deserializer_option.has_positions()),
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
                                           -> (&'a [u8], usize, Vec<u8>)
{//&'a [u8]
    let mut block_start: &[u8] = streamer.cursor;
    let mut cursor = 0;
    let mut term_buffer: Vec<u8> = vec!();

    while streamer.advance() {
        let iter_key = streamer.key();
        if !predicate(iter_key.as_ref()) {
            return (block_start, streamer.term_block_decoder.cursor() - 1,  term_buffer);
        }
        if streamer.remaining_in_block == 0 {
            block_start = streamer.cursor;
            term_buffer.clear();
            term_buffer.extend_from_slice(iter_key.as_ref());
        }
    }
    (block_start, streamer.term_block_decoder.cursor() - 1,  term_buffer)
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
            block_start: term_dictionary.stream_data().as_ref(),
            cursor: 0,
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
    remaining_in_block: usize,
    term_block_decoder: TermBlockDecoder<'a>,
    terminfo_block_decoder: TermInfoBlockDecoder<'a>,
    cursor: &'a [u8],
}

impl<'a> TermStreamerImpl<'a>
{
    fn load_block(&mut self) -> bool {
        self.remaining_in_block = self.cursor[0] as usize;
        if self.remaining_in_block == 0 {
            false
        }
        else {
            self.cursor = &self.cursor[1..];
            self.cursor = self.term_block_decoder.decode_block(self.cursor);
            self.cursor = self.terminfo_block_decoder.decode_block(self.cursor, self.remaining_in_block);
            true
        }
    }
}


impl<'a> TermStreamer for TermStreamerImpl<'a>
{
    fn advance(&mut self) -> bool {
        if self.remaining_in_block == 0 {
            if !self.load_block() {
                return false;
            }
        }
        self.remaining_in_block -= 1;
        self.term_block_decoder.advance();
        self.terminfo_block_decoder.advance();
        true
    }

    fn key(&self) -> &[u8] {
        self.term_block_decoder.term()
    }

    fn value(&self) -> &TermInfo {
        self.terminfo_block_decoder.term_info()
    }
}

