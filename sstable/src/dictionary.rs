use std::io;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use common::file_slice::FileSlice;
use common::{BinarySerializable, OwnedBytes};
use tantivy_fst::automaton::AlwaysMatch;
use tantivy_fst::Automaton;

use crate::streamer::{Streamer, StreamerBuilder};
use crate::{BlockAddr, DeltaReader, Reader, SSTable, SSTableIndex, TermOrdinal};

/// An SSTable is a sorted map that associates sorted `&[u8]` keys
/// to any kind of typed values.
///
/// The SSTable is organized in blocks.
/// In each block, keys and values are encoded separately.
///
/// The keys are encoded using incremental encoding.
/// The values on the other hand, are encoded according to a value-specific
/// codec defined in the TSSTable generic argument.
///
/// Finally, an index is joined to the Dictionary to make it possible,
/// given a key to identify which block contains this key.
///
/// The codec was designed in such a way that the sstable
/// reader is not aware of block, and yet can read any sequence of blocks,
/// as long as the slice of bytes it is given starts and stops at
/// block boundary.
///
/// (See also README.md)
pub struct Dictionary<TSSTable: SSTable> {
    pub sstable_slice: FileSlice,
    pub sstable_index: SSTableIndex,
    num_terms: u64,
    phantom_data: PhantomData<TSSTable>,
}

impl<TSSTable: SSTable> Dictionary<TSSTable> {
    pub fn builder<W: io::Write>(wrt: W) -> io::Result<crate::Writer<W, TSSTable::ValueWriter>> {
        Ok(TSSTable::writer(wrt))
    }

    pub(crate) fn sstable_reader(&self) -> io::Result<Reader<'static, TSSTable::ValueReader>> {
        let data = self.sstable_slice.read_bytes()?;
        Ok(TSSTable::reader(data))
    }

    pub(crate) fn sstable_reader_block(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<Reader<'static, TSSTable::ValueReader>> {
        let data = self.sstable_slice.read_bytes_slice(block_addr.byte_range)?;
        Ok(TSSTable::reader(data))
    }

    pub(crate) async fn sstable_reader_block_async(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<Reader<'static, TSSTable::ValueReader>> {
        let data = self
            .sstable_slice
            .read_bytes_slice_async(block_addr.byte_range)
            .await?;
        Ok(TSSTable::reader(data))
    }

    pub(crate) fn sstable_delta_reader_for_key_range(
        &self,
        key_range: impl RangeBounds<[u8]>,
    ) -> io::Result<DeltaReader<'static, TSSTable::ValueReader>> {
        let slice = self.file_slice_for_range(key_range);
        let data = slice.read_bytes()?;
        Ok(TSSTable::delta_reader(data))
    }

    /// This function returns a file slice covering a set of sstable blocks
    /// that include the key range passed in arguments.
    ///
    /// It works by identifying
    /// - `first_block`: the block containing the start boudary key
    /// - `last_block`: the block containing the end boundary key.
    ///
    /// And then returning the range that spans over all blocks between.
    /// and including first_block and last_block, aka:
    /// `[first_block.start_offset .. last_block.end_offset)`
    ///
    /// Technically this function does not provide the tightest fit, as
    /// for simplification, it treats the start bound of the `key_range`
    /// as if it was inclusive, even if it is exclusive.
    /// On the rare edge case where a user asks for `(start_key, end_key]`
    /// and `start_key` happens to be the last key of a block, we return a
    /// slice that is the first block was not necessary.
    fn file_slice_for_range(&self, key_range: impl RangeBounds<[u8]>) -> FileSlice {
        let start_bound: Bound<usize> = match key_range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => {
                let Some(first_block_addr) = self.sstable_index.search_block(key) else {
                    return FileSlice::empty();
                };
                Bound::Included(first_block_addr.byte_range.start)
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound: Bound<usize> = match key_range.end_bound() {
            Bound::Included(key) | Bound::Excluded(key) => {
                if let Some(block_addr) = self.sstable_index.search_block(key) {
                    Bound::Excluded(block_addr.byte_range.end)
                } else {
                    Bound::Unbounded
                }
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        self.sstable_slice.slice((start_bound, end_bound))
    }

    /// Opens a `TermDictionary`.
    pub fn open(term_dictionary_file: FileSlice) -> io::Result<Self> {
        let (main_slice, footer_len_slice) = term_dictionary_file.split_from_end(16);
        let mut footer_len_bytes: OwnedBytes = footer_len_slice.read_bytes()?;
        let index_offset = u64::deserialize(&mut footer_len_bytes)?;
        let num_terms = u64::deserialize(&mut footer_len_bytes)?;
        let (sstable_slice, index_slice) = main_slice.split(index_offset as usize);
        let sstable_index_bytes = index_slice.read_bytes()?;
        let sstable_index = SSTableIndex::load(sstable_index_bytes.as_slice())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "SSTable corruption"))?;
        Ok(Dictionary {
            sstable_slice,
            sstable_index,
            num_terms,
            phantom_data: PhantomData,
        })
    }

    /// Creates a term dictionary from the supplied bytes.
    pub fn from_bytes(owned_bytes: OwnedBytes) -> io::Result<Self> {
        Dictionary::open(FileSlice::new(Arc::new(owned_bytes)))
    }

    /// Creates an empty term dictionary which contains no terms.
    pub fn empty() -> Self {
        let term_dictionary_data: Vec<u8> = Self::builder(Vec::<u8>::new())
            .expect("Creating a TermDictionaryBuilder in a Vec<u8> should never fail")
            .finish()
            .expect("Writing in a Vec<u8> should never fail");
        let empty_dict_file = FileSlice::from(term_dictionary_data);
        Dictionary::open(empty_dict_file).unwrap()
    }

    /// Returns the number of terms in the dictionary.
    /// Term ordinals range from 0 to `num_terms() - 1`.
    pub fn num_terms(&self) -> usize {
        self.num_terms as usize
    }

    /// Returns the ordinal associated with a given term.
    pub fn term_ord<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermOrdinal>> {
        let mut term_ord = 0u64;
        let key_bytes = key.as_ref();
        let mut sstable_reader = self.sstable_reader()?;
        while sstable_reader.advance().unwrap_or(false) {
            if sstable_reader.key() == key_bytes {
                return Ok(Some(term_ord));
            }
            term_ord += 1;
        }
        Ok(None)
    }

    /// Returns the term associated with a given term ordinal.
    ///
    /// Term ordinals are defined as the position of the term in
    /// the sorted list of terms.
    ///
    /// Returns true if and only if the term has been found.
    ///
    /// Regardless of whether the term is found or not,
    /// the buffer may be modified.
    pub fn ord_to_term(&self, ord: TermOrdinal, bytes: &mut Vec<u8>) -> io::Result<bool> {
        let mut sstable_reader = self.sstable_reader()?;
        bytes.clear();
        for _ in 0..(ord + 1) {
            if !sstable_reader.advance().unwrap_or(false) {
                return Ok(false);
            }
        }
        bytes.extend_from_slice(sstable_reader.key());
        Ok(true)
    }

    /// Returns the number of terms in the dictionary.
    pub fn term_info_from_ord(&self, term_ord: TermOrdinal) -> io::Result<Option<TSSTable::Value>> {
        let mut sstable_reader = self.sstable_reader()?;
        for _ in 0..(term_ord + 1) {
            if !sstable_reader.advance().unwrap_or(false) {
                return Ok(None);
            }
        }
        Ok(Some(sstable_reader.value().clone()))
    }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TSSTable::Value>> {
        if let Some(block_addr) = self.sstable_index.search_block(key.as_ref()) {
            let mut sstable_reader = self.sstable_reader_block(block_addr)?;
            let key_bytes = key.as_ref();
            while sstable_reader.advance().unwrap_or(false) {
                if sstable_reader.key() == key_bytes {
                    let value = sstable_reader.value().clone();
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    /// Lookups the value corresponding to the key.
    pub async fn get_async<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TSSTable::Value>> {
        if let Some(block_addr) = self.sstable_index.search_block(key.as_ref()) {
            let mut sstable_reader = self.sstable_reader_block_async(block_addr).await?;
            let key_bytes = key.as_ref();
            while sstable_reader.advance().unwrap_or(false) {
                if sstable_reader.key() == key_bytes {
                    let value = sstable_reader.value().clone();
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range(&self) -> StreamerBuilder<'_, TSSTable> {
        StreamerBuilder::new(self, AlwaysMatch)
    }

    /// A stream of all the sorted terms.
    pub fn stream(&self) -> io::Result<Streamer<'_, TSSTable>> {
        self.range().into_stream()
    }

    /// Returns a search builder, to stream all of the terms
    /// within the Automaton
    pub fn search<'a, A: Automaton + 'a>(
        &'a self,
        automaton: A,
    ) -> StreamerBuilder<'a, TSSTable, A>
    where
        A::State: Clone,
    {
        StreamerBuilder::<TSSTable, A>::new(self, automaton)
    }

    #[doc(hidden)]
    pub async fn warm_up_dictionary(&self) -> io::Result<()> {
        self.sstable_slice.read_bytes_async().await?;
        Ok(())
    }
}
