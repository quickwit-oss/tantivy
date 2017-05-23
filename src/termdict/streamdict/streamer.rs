#![allow(should_implement_trait)]

use std::cmp::max;
use std::io::Read;
use common::VInt;
use common::BinarySerializable;
use super::TermDictionary;
use fst::Streamer;

pub(crate) fn stream_before<'a, V>(term_dictionary: &'a TermDictionary<V>, target_key: &[u8]) -> TermStreamer<'a, V>
    where V: 'a + BinarySerializable + Default {
    let (prev_key, offset) = term_dictionary.strictly_previous_key(target_key.as_ref());
    let offset: usize = offset as usize;
    TermStreamer {
        cursor: &term_dictionary.stream_data()[offset..],
        current_key: Vec::from(prev_key),
        current_value: V::default(),
    }
}

/// `TermStreamerBuilder` is an helper object used to define
/// a range of terms that should be streamed.
pub struct TermStreamerBuilder<'a, V>
    where V: 'a + BinarySerializable + Default {
    term_dictionary: &'a TermDictionary<V>,
    origin: usize,
    offset_from: usize,
    offset_to: usize,
    current_key: Vec<u8>,
}

/// Returns offset information for the first 
/// key in the stream matching a given predicate.
///
/// returns (start offset, the data required to load the value)
fn get_offset<'a, V, P: Fn(&[u8])->bool>(predicate: P, mut streamer: TermStreamer<V>) -> (usize, Vec<u8>)
    where V: 'a + BinarySerializable + Default {
    let mut prev: &[u8] = streamer.cursor;
    
    let mut prev_data: Vec<u8> = streamer.current_key.clone();
    
    while let Some((iter_key, _)) = streamer.next() {
        if !predicate(iter_key) {
            return (prev.as_ptr() as usize, prev_data);
        }
        prev = streamer.cursor;
        prev_data.clear();
        prev_data.extend_from_slice(iter_key);
    }
    return (prev.as_ptr() as usize, prev_data);
}

impl<'a, V> TermStreamerBuilder<'a, V>
    where V: 'a + BinarySerializable + Default {

    /// Limit the range to terms greater or equal to the bound
    pub fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> TermStreamerBuilder<'a, V> {
        let target_key = bound.as_ref();
        let streamer = stream_before(&self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.lt(target_key) };
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms strictly greater than the bound
    pub fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> TermStreamerBuilder<'a, V> {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.le(target_key) };
        let (offset_before, current_key) = get_offset(smaller_than, streamer);
        self.current_key = current_key;
        self.offset_from = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> TermStreamerBuilder<'a, V> {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.lt(target_key) };
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn le<T: AsRef<[u8]>>(mut self, bound: T) -> TermStreamerBuilder<'a, V> {
        let target_key = bound.as_ref();
        let streamer = stream_before(self.term_dictionary, target_key.as_ref());
        let smaller_than = |k: &[u8]| { k.le(target_key) };
        let (offset_before, _) = get_offset(smaller_than, streamer);
        self.offset_to = offset_before - self.origin;
        self
    }

    pub(crate) fn new(term_dictionary: &'a TermDictionary<V>) -> TermStreamerBuilder<'a, V> {
        let data = term_dictionary.stream_data();
        let origin = data.as_ptr() as usize;
        TermStreamerBuilder {
            term_dictionary: term_dictionary,
            origin: origin,
            offset_from: 0,
            offset_to: data.len(),
            current_key: vec!(),
        }
    }

    /// Build the streamer.
    pub fn into_stream(self) -> TermStreamer<'a, V> {
        let data: &[u8] = self.term_dictionary.stream_data();
        let start = self.offset_from;
        let stop = max(self.offset_to, start);
        TermStreamer {
            cursor: &data[start..stop],
            current_key: self.current_key,
            current_value: V::default(),
        }
    }
}

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct TermStreamer<'a, V>
    where V: 'a + BinarySerializable + Default {
    cursor: &'a [u8],
    current_key: Vec<u8>,
    current_value: V,
}



impl<'a, V: BinarySerializable> TermStreamer<'a, V> 
    where V: 'a + BinarySerializable + Default {
    
    /// Advance position the stream on the next item.
    /// Before the first call to `.advance()`, the stream
    /// is an unitialized state.
    pub fn advance(&mut self) -> bool {
        if self.cursor.len() == 0 {
            return false;
        }
        let common_length: usize = VInt::deserialize(&mut self.cursor).unwrap().0 as usize;
        let new_length: usize = common_length + VInt::deserialize(&mut self.cursor).unwrap().0 as usize;
        self.current_key.reserve(new_length);
        unsafe {
            self.current_key.set_len(new_length);
        }
        self.cursor.read_exact(&mut self.current_key[common_length..new_length]).unwrap();
        self.current_value = V::deserialize(&mut self.cursor).unwrap();
        return true;
    }

    /// Accesses the current key.
    ///
    /// `.key()` should return the key that was returned
    /// by the `.next()` method.
    ///
    /// If the end of the stream as been reached, and `.next()`
    /// has been called and returned `None`, `.key()` remains
    /// the value of the last key encounterred.
    ///
    /// Before any call to `.next()`, `.key()` returns an empty array.
    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    /// Accesses the current value.
    /// 
    /// Calling `.value()` after the end of the stream will return the
    /// last `.value()` encounterred.
    /// 
    /// # Panics
    ///
    /// Calling `.value()` before the first call to `.advance()` returns
    /// `V::default()`.
    pub fn value(&self) -> &V {
        &self.current_value
    }

    pub(crate) fn extract_value(self) -> V {
        self.current_value
    }
}
