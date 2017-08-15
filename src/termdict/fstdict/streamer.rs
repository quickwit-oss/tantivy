use fst::{IntoStreamer, Streamer};
use fst::map::{StreamBuilder, Stream};
use common::BinarySerializable;
use super::TermDictionaryImpl;
use termdict::{TermStreamerBuilder, TermStreamer};

/// See [`TermStreamerBuilder`](./trait.TermStreamerBuilder.html)
pub struct TermStreamerBuilderImpl<'a, V: 'a>
{
    fst_map: &'a TermDictionaryImpl<V>,
    stream_builder: StreamBuilder<'a>,
}

impl<'a, V> TermStreamerBuilderImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    pub(crate) fn new(fst_map: &'a TermDictionaryImpl<V>,
                      stream_builder: StreamBuilder<'a>)
                      -> Self {
        TermStreamerBuilderImpl {
            fst_map: fst_map,
            stream_builder: stream_builder,
        }
    }
}

impl<'a, V> TermStreamerBuilder<V> for TermStreamerBuilderImpl<'a, V>
    where V: 'a + BinarySerializable + Default
{
    type Streamer = TermStreamerImpl<'a, V>;

    fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.ge(bound);
        self
    }

    fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.gt(bound);
        self
    }

    fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.le(bound);
        self
    }

    fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.lt(bound);
        self
    }

    fn into_stream(self) -> Self::Streamer {
        TermStreamerImpl {
            fst_map: self.fst_map,
            stream: self.stream_builder.into_stream(),
            offset: 0u64,
            current_key: Vec::with_capacity(100),
            current_value: V::default(),
        }
    }
}


/// See [`TermStreamer`](./trait.TermStreamer.html)
pub struct TermStreamerImpl<'a, V: 'a>
{
    fst_map: &'a TermDictionaryImpl<V>,
    stream: Stream<'a>,
    offset: u64,
    current_key: Vec<u8>,
    current_value: V,
}

impl<'a, V> TermStreamer<V> for TermStreamerImpl<'a, V>
    where V: BinarySerializable + Default
{
    fn advance(&mut self) -> bool {
        if let Some((term, offset)) = self.stream.next() {
            self.current_key.clear();
            self.current_key.extend_from_slice(term);
            self.offset = offset;
            self.current_value =
                self.fst_map
                    .read_value(self.offset)
                    .expect("Fst data is corrupted. Failed to deserialize a value.");
            true
        } else {
            false
        }
    }

    fn key(&self) -> &[u8] {
        &self.current_key
    }

    fn value(&self) -> &V {
        &self.current_value
    }
}
