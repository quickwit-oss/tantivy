use fst::{IntoStreamer, Streamer};
use fst::map::{StreamBuilder, Stream};
use common::BinarySerializable;
use super::TermDictionary;

/// `TermStreamerBuilder` is an helper object used to define
/// a range of terms that should be streamed.
pub struct TermStreamerBuilder<'a, V>
    where V: 'a + BinarySerializable + Default
{
    fst_map: &'a TermDictionary<V>,
    stream_builder: StreamBuilder<'a>,
}

impl<'a, V> TermStreamerBuilder<'a, V>
    where V: 'a + BinarySerializable + Default
{
    /// Limit the range to terms greater or equal to the bound
    pub fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.ge(bound);
        self
    }

    /// Limit the range to terms strictly greater than the bound
    pub fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.gt(bound);
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.le(bound);
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.lt(bound);
        self
    }

    /// Creates the stream corresponding to the range
    /// of terms defined using the `TermStreamerBuilder`.
    pub fn into_stream(self) -> TermStreamer<'a, V> {
        TermStreamer {
            fst_map: self.fst_map,
            stream: self.stream_builder.into_stream(),
            offset: 0u64,
            current_key: Vec::with_capacity(100),
            current_value: V::default(),
        }
    }
    
    pub(crate) fn new(fst_map: &'a TermDictionary<V>,
                      stream_builder: StreamBuilder<'a>)
                      -> TermStreamerBuilder<'a, V> {
        TermStreamerBuilder {
            fst_map: fst_map,
            stream_builder: stream_builder,
        }
    }
}




/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct TermStreamer<'a, V>
    where V: 'a + BinarySerializable + Default
{
    fst_map: &'a TermDictionary<V>,
    stream: Stream<'a>,
    offset: u64,
    current_key: Vec<u8>,
    current_value: V,
}



impl<'a, V> TermStreamer<'a, V>
    where V: 'a + BinarySerializable + Default
{
    /// Advance position the stream on the next item.
    /// Before the first call to `.advance()`, the stream
    /// is an unitialized state.
    pub fn advance(&mut self) -> bool {
        if let Some((term, offset)) = self.stream.next() {
            self.current_key.clear();
            self.current_key.extend_from_slice(term);
            self.offset = offset;
            self.current_value = self.fst_map
                .read_value(self.offset)
                .expect("Fst data is corrupted. Failed to deserialize a value.");
            true
        } else {
            false
        }
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
}
