use fst::{self, IntoStreamer, Streamer};
use fst::map::{StreamBuilder, Stream};
use common::BinarySerializable;
use super::FstMap;

pub struct FstMapStreamerBuilder<'a, V> where V: 'a + BinarySerializable {
    fst_map: &'a FstMap<V>,
    stream_builder: StreamBuilder<'a>,
}

impl<'a, V> FstMapStreamerBuilder<'a, V> where V: 'a + BinarySerializable {

    pub fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.ge(bound);
        self
    }
    
    pub fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.gt(bound);
        self
    }

    pub fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.le(bound);
        self
    }

    pub fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.stream_builder = self.stream_builder.lt(bound);
        self
    }

    pub fn into_stream(self) -> FstMapStreamer<'a, V> {
        FstMapStreamer {
            fst_map: self.fst_map,
            stream: self.stream_builder.into_stream(),
            buffer: Vec::with_capacity(100),
            offset: 0u64,
        }
    }

    pub fn new(fst_map: &'a FstMap<V>, stream_builder: StreamBuilder<'a>) -> FstMapStreamerBuilder<'a, V> {
        FstMapStreamerBuilder {
            fst_map: fst_map,
            stream_builder: stream_builder,
        }
    }
}





pub struct FstMapStreamer<'a, V> where V: 'a + BinarySerializable {
    fst_map: &'a FstMap<V>,
    stream: Stream<'a>,
    offset: u64,
    buffer: Vec<u8>,
}


impl<'a, V> fst::Streamer<'a> for FstMapStreamer<'a, V> where V: 'a + BinarySerializable {
    
    type Item = &'a [u8];
    
    fn next<'b>(&'b mut self) -> Option<&'b [u8]> {
        if self.advance() {
            Some(&self.buffer)
        }
        else {
            None
        }
    }
}

impl<'a, V> FstMapStreamer<'a, V> where V: 'a + BinarySerializable {

    pub fn advance(&mut self) -> bool {
        if let Some((term, offset)) = self.stream.next() {
            self.buffer.clear();
            self.buffer.extend_from_slice(term);
            self.offset = offset;
            true
        }
        else {
            false
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.buffer
    }

    pub fn value(&self) -> V {
        self.fst_map.read_value(self.offset)
    }
}

