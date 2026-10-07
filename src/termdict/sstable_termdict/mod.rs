use std::io;

mod merger;

use std::iter::ExactSizeIterator;

use common::VInt;
use sstable::value::{ValueReader, ValueWriter};
use sstable::SSTable;
use tantivy_fst::automaton::AlwaysMatch;
use tantivy_fst::Automaton;

pub use self::merger::TermMerger;
use crate::postings::TermInfo;

/// The term dictionary contains all of the terms in
/// `tantivy index` in a sorted manner.
///
/// The `Fst` crate is used to associate terms to their
/// respective `TermOrdinal`. The `TermInfoStore` then makes it
/// possible to fetch the associated `TermInfo`.
pub type TermDictionary = sstable::Dictionary<TermSSTable>;

/// Builder for the new term dictionary.
pub type TermDictionaryBuilder<W> = sstable::Writer<W, TermInfoValueWriter>;

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct TermStreamer<'a, A = AlwaysMatch>
where
    A: Automaton,
    A::State: Clone,
{
    inner: sstable::Streamer<TermSSTable, A>,
    // needed to share the same interface as FST-based term dictionary
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a, A> TermStreamer<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    pub(crate) fn new(inner: sstable::Streamer<TermSSTable, A>) -> Self {
        TermStreamer {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, A> std::ops::Deref for TermStreamer<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Target = sstable::Streamer<TermSSTable, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, A> std::ops::DerefMut for TermStreamer<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'a, A> From<sstable::Streamer<TermSSTable, A>> for TermStreamer<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    fn from(inner: sstable::Streamer<TermSSTable, A>) -> Self {
        TermStreamer::new(inner)
    }
}

/// `TermStreamerBuilder` is a helper object used to define a range of terms that should be
/// streamed.
pub struct TermStreamerBuilder<'a, A = AlwaysMatch>(sstable::StreamerBuilder<'a, TermSSTable, A>)
where
    A: Automaton,
    A::State: Clone;

impl<'a, A> From<sstable::StreamerBuilder<'a, TermSSTable, A>> for TermStreamerBuilder<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    fn from(inner: sstable::StreamerBuilder<'a, TermSSTable, A>) -> Self {
        TermStreamerBuilder(inner)
    }
}

impl<'a, A> TermStreamerBuilder<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    /// Limit the range to terms greater or equal to the bound
    pub fn ge<T: AsRef<[u8]>>(self, bound: T) -> Self {
        TermStreamerBuilder(self.0.ge(bound))
    }

    /// Limit the range to terms strictly greater than the bound
    pub fn gt<T: AsRef<[u8]>>(self, bound: T) -> Self {
        TermStreamerBuilder(self.0.gt(bound))
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn le<T: AsRef<[u8]>>(self, bound: T) -> Self {
        TermStreamerBuilder(self.0.le(bound))
    }

    /// Limit the range to terms strictly lesser than the bound
    pub fn lt<T: AsRef<[u8]>>(self, bound: T) -> Self {
        TermStreamerBuilder(self.0.lt(bound))
    }

    /// Load no more data than what's required to get `limit`
    /// matching entries.
    ///
    /// The resulting [`TermStreamer`] can still return marginally
    /// more than `limit` elements.
    pub fn limit(self, limit: u64) -> Self {
        TermStreamerBuilder(self.0.limit(limit))
    }

    /// Creates the stream corresponding to the range
    /// of terms defined using the `TermStreamerBuilder`.
    pub fn into_stream(self) -> io::Result<TermStreamer<'a, A>> {
        Ok(TermStreamer::new(self.0.into_stream()?))
    }

    /// Async version of [`TermStreamerBuilder::into_stream`].
    pub async fn into_stream_async(self) -> io::Result<TermStreamer<'a, A>> {
        Ok(TermStreamer::new(self.0.into_stream_async().await?))
    }

    /// Same as [`TermStreamerBuilder::into_stream_async`], but tries to issue a
    /// single io operation when requesting blocks that are not consecutive,
    /// but also less than `merge_holes_under_bytes` bytes apart.
    pub async fn into_stream_async_merging_holes(
        self,
        merge_holes_under_bytes: usize,
    ) -> io::Result<TermStreamer<'a, A>> {
        Ok(TermStreamer::new(
            self.0
                .into_stream_async_merging_holes(merge_holes_under_bytes)
                .await?,
        ))
    }
}

/// SSTable used to store TermInfo objects.
#[derive(Clone)]
pub struct TermSSTable;

impl SSTable for TermSSTable {
    type Value = TermInfo;
    type ValueReader = TermInfoValueReader;
    type ValueWriter = TermInfoValueWriter;
}

#[derive(Default)]
pub struct TermInfoValueReader {
    term_infos: Vec<TermInfo>,
}

impl ValueReader for TermInfoValueReader {
    type Value = TermInfo;

    #[inline(always)]
    fn value(&self, idx: usize) -> &TermInfo {
        &self.term_infos[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let len_before = data.len();
        self.term_infos.clear();
        let num_els = VInt::deserialize_u64(&mut data)?;
        let mut postings_start = VInt::deserialize_u64(&mut data)? as usize;
        let mut positions_start = VInt::deserialize_u64(&mut data)? as usize;
        for _ in 0..num_els {
            let doc_freq = VInt::deserialize_u64(&mut data)? as u32;
            let postings_num_bytes = VInt::deserialize_u64(&mut data)?;
            let positions_num_bytes = VInt::deserialize_u64(&mut data)?;
            let postings_end = postings_start + postings_num_bytes as usize;
            let positions_end = positions_start + positions_num_bytes as usize;
            let term_info = TermInfo {
                doc_freq,
                postings_range: postings_start..postings_end,
                positions_range: positions_start..positions_end,
            };
            self.term_infos.push(term_info);
            postings_start = postings_end;
            positions_start = positions_end;
        }
        let consumed_len = len_before - data.len();
        Ok(consumed_len)
    }
}

#[derive(Default)]
pub struct TermInfoValueWriter {
    term_infos: Vec<TermInfo>,
}

impl ValueWriter for TermInfoValueWriter {
    type Value = TermInfo;

    fn write(&mut self, term_info: &TermInfo) {
        self.term_infos.push(term_info.clone());
    }

    fn serialize_block(&self, buffer: &mut Vec<u8>) {
        VInt(self.term_infos.len() as u64).serialize_into_vec(buffer);
        if self.term_infos.is_empty() {
            return;
        }
        VInt(self.term_infos[0].postings_range.start as u64).serialize_into_vec(buffer);
        VInt(self.term_infos[0].positions_range.start as u64).serialize_into_vec(buffer);
        for term_info in &self.term_infos {
            VInt(term_info.doc_freq as u64).serialize_into_vec(buffer);
            VInt(term_info.postings_range.len() as u64).serialize_into_vec(buffer);
            VInt(term_info.positions_range.len() as u64).serialize_into_vec(buffer);
        }
    }

    fn clear(&mut self) {
        self.term_infos.clear();
    }
}

#[cfg(test)]
mod tests {
    use sstable::value::{ValueReader, ValueWriter};

    use crate::postings::TermInfo;
    use crate::termdict::sstable_termdict::TermInfoValueReader;

    #[test]
    fn test_block_terminfos() {
        let mut term_info_writer = super::TermInfoValueWriter::default();
        term_info_writer.write(&TermInfo {
            doc_freq: 120u32,
            postings_range: 17..45,
            positions_range: 10..122,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 10u32,
            postings_range: 45..450,
            positions_range: 122..1100,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 17u32,
            postings_range: 450..462,
            positions_range: 1100..1302,
        });
        let mut buffer = Vec::new();
        term_info_writer.serialize_block(&mut buffer);
        let mut term_info_reader = TermInfoValueReader::default();
        let num_bytes: usize = term_info_reader.load(&buffer[..]).unwrap();
        assert_eq!(
            term_info_reader.value(0),
            &TermInfo {
                doc_freq: 120u32,
                postings_range: 17..45,
                positions_range: 10..122
            }
        );
        assert_eq!(buffer.len(), num_bytes);
    }
}
