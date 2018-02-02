use fst::{IntoStreamer, Streamer};
use fst::map::{Stream, StreamBuilder};
use postings::TermInfo;
use super::TermDictionaryImpl;
use termdict::{TermDictionary, TermOrdinal, TermStreamer, TermStreamerBuilder};

/// See [`TermStreamerBuilder`](./trait.TermStreamerBuilder.html)
pub struct TermStreamerBuilderImpl<'a> {
    fst_map: &'a TermDictionaryImpl,
    stream_builder: StreamBuilder<'a>,
}

impl<'a> TermStreamerBuilderImpl<'a> {
    pub(crate) fn new(fst_map: &'a TermDictionaryImpl, stream_builder: StreamBuilder<'a>) -> Self {
        TermStreamerBuilderImpl {
            fst_map: fst_map,
            stream_builder: stream_builder,
        }
    }
}

impl<'a> TermStreamerBuilder for TermStreamerBuilderImpl<'a> {
    type Streamer = TermStreamerImpl<'a>;

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
            term_ord: 0u64,
            current_key: Vec::with_capacity(100),
            current_value: TermInfo::default(),
        }
    }
}

/// See [`TermStreamer`](./trait.TermStreamer.html)
pub struct TermStreamerImpl<'a> {
    fst_map: &'a TermDictionaryImpl,
    stream: Stream<'a>,
    term_ord: TermOrdinal,
    current_key: Vec<u8>,
    current_value: TermInfo,
}

impl<'a> TermStreamer for TermStreamerImpl<'a> {
    fn advance(&mut self) -> bool {
        if let Some((term, term_ord)) = self.stream.next() {
            self.current_key.clear();
            self.current_key.extend_from_slice(term);
            self.term_ord = term_ord;
            self.current_value = self.fst_map.term_info_from_ord(term_ord);
            true
        } else {
            false
        }
    }

    fn term_ord(&self) -> TermOrdinal {
        self.term_ord
    }

    fn key(&self) -> &[u8] {
        &self.current_key
    }

    fn value(&self) -> &TermInfo {
        &self.current_value
    }
}
