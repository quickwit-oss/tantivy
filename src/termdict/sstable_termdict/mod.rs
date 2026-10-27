use std::io;

mod merger;
mod short_postings;

use std::iter::ExactSizeIterator;

use common::VInt;
use sstable::value::{ValueReader, ValueWriter};
use sstable::SSTable;
use tantivy_fst::automaton::AlwaysMatch;

pub use self::merger::TermMerger;
use self::short_postings::{invalid, read_usize};
use crate::postings::TermInfo;

/// The term dictionary contains all of the terms in
/// `tantivy index` in a sorted manner.
///
/// The `Fst` crate is used to associate terms to their
/// respective `TermOrdinal`. The `TermInfoStore` then makes it
/// possible to fetch the associated `TermInfo`.
pub type TermDictionary = sstable::Dictionary<TermSSTable>;

/// Builder for the new term dictionary.
pub type TermDictionaryBuilder<W> = sstable::Writer<W, TermInfoBlock>;

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub type TermStreamer<'a, A = AlwaysMatch, K = Vec<u8>> = sstable::Streamer<'a, TermSSTable, A, K>;

/// SSTable used to store TermInfo objects.
#[derive(Clone)]
pub struct TermSSTable;

pub type TermStreamerBuilder<'a, A = AlwaysMatch, K = Vec<u8>> =
    sstable::StreamerBuilder<'a, TermSSTable, A, K>;

impl SSTable for TermSSTable {
    type Value = TermInfo;
    type ValueReader = TermInfoBlock;
    type ValueWriter = TermInfoBlock;
}

/// Term metadata and inline postings for one dictionary block.
#[derive(Default)]
pub struct TermInfoBlock {
    term_infos: Vec<TermInfo>,
    pub(super) max_inline_docs: u32,
}

impl TermInfoBlock {
    pub(super) fn new(max_inline_docs: u32) -> io::Result<Self> {
        Ok(Self {
            term_infos: Vec::new(),
            max_inline_docs: super::validate_max_inline_docs(max_inline_docs)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
        })
    }

    fn read_term_infos(
        &mut self,
        data: &mut &[u8],
        num_terms: usize,
        has_short_postings: bool,
    ) -> io::Result<usize> {
        let mut postings_start = read_usize(data)?;
        let mut positions_start = read_usize(data)?;
        let minimum_bytes = if has_short_postings { 1 } else { 3 };
        if num_terms == 0 || num_terms > data.len() / minimum_bytes {
            return Err(invalid("invalid term count"));
        }
        let mut short_count = 0;
        for _ in 0..num_terms {
            let doc_freq = u32::try_from(VInt::deserialize_u64(data)?)
                .map_err(|_| invalid("document count overflow"))?;
            let (postings_len, positions_len) = if has_short_postings && doc_freq == 0 {
                short_count += 1;
                (0, 0)
            } else {
                (read_usize(data)?, read_usize(data)?)
            };
            let postings_end = postings_start
                .checked_add(postings_len)
                .ok_or_else(|| invalid("postings offset overflow"))?;
            let positions_end = positions_start
                .checked_add(positions_len)
                .ok_or_else(|| invalid("positions offset overflow"))?;
            self.term_infos.push(TermInfo {
                doc_freq,
                postings_range: postings_start..postings_end,
                positions_range: positions_start..positions_end,
                short_postings: None,
            });
            postings_start = postings_end;
            positions_start = positions_end;
        }
        Ok(short_count)
    }
}

impl ValueReader for TermInfoBlock {
    type Value = TermInfo;

    fn configure(&mut self, maximum: u8) -> io::Result<()> {
        *self = Self::new(u32::from(maximum))?;
        Ok(())
    }

    #[inline(always)]
    fn value(&self, idx: usize) -> &TermInfo {
        &self.term_infos[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let len_before = data.len();
        self.term_infos.clear();
        let num_terms = read_usize(&mut data)?;
        if num_terms != 0 {
            self.read_term_infos(&mut data, num_terms, false)?;
        } else if !data.is_empty() {
            return short_postings::read_block(data, self)
                .map(|consumed| consumed + len_before - data.len());
        }
        Ok(len_before - data.len())
    }
}

impl ValueWriter for TermInfoBlock {
    type Value = TermInfo;

    fn write(&mut self, term_info: &TermInfo) {
        self.term_infos.push(term_info.clone());
    }

    fn serialize_block_with_keys(&self, keys: &[u8], buffer: &mut Vec<u8>) -> io::Result<bool> {
        if self
            .term_infos
            .iter()
            .all(|term| term.short_postings.is_none())
        {
            return Ok(false);
        }
        short_postings::write_block(self, keys, buffer)?;
        Ok(true)
    }

    fn serialize_block(&self, buffer: &mut Vec<u8>) {
        assert!(self
            .term_infos
            .iter()
            .all(|term| term.short_postings.is_none()));
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

    use super::TermInfoBlock;
    use crate::postings::TermInfo;

    #[test]
    fn test_block_terminfos() {
        let mut term_info_writer = TermInfoBlock::default();
        term_info_writer.write(&TermInfo {
            doc_freq: 120u32,
            postings_range: 17..45,
            positions_range: 10..122,
            short_postings: None,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 10u32,
            postings_range: 45..450,
            positions_range: 122..1100,
            short_postings: None,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 17u32,
            postings_range: 450..462,
            positions_range: 1100..1302,
            short_postings: None,
        });
        let mut buffer = Vec::new();
        term_info_writer.serialize_block(&mut buffer);
        let mut term_info_reader = TermInfoBlock::default();
        let num_bytes: usize = term_info_reader.load(&buffer[..]).unwrap();
        assert_eq!(
            term_info_reader.value(0),
            &TermInfo {
                doc_freq: 120u32,
                postings_range: 17..45,
                positions_range: 10..122,
                short_postings: None,
            }
        );
        assert_eq!(buffer.len(), num_bytes);
    }
}
