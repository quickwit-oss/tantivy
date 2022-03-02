use std::io;

use common::BinarySerializable;
use once_cell::sync::Lazy;
use tantivy_fst::automaton::AlwaysMatch;
use tantivy_fst::Automaton;

use crate::directory::{FileSlice, OwnedBytes};
use crate::postings::TermInfo;
use crate::termdict::sstable_termdict::sstable::sstable_index::BlockAddr;
use crate::termdict::sstable_termdict::sstable::{
    DeltaReader, Reader, SSTable, SSTableIndex, Writer,
};
use crate::termdict::sstable_termdict::{
    TermInfoReader, TermInfoWriter, TermSSTable, TermStreamer, TermStreamerBuilder,
};
use crate::termdict::TermOrdinal;
use crate::AsyncIoResult;

pub struct TermInfoSSTable;
impl SSTable for TermInfoSSTable {
    type Value = TermInfo;
    type Reader = TermInfoReader;
    type Writer = TermInfoWriter;
}
pub struct TermDictionaryBuilder<W: io::Write> {
    sstable_writer: Writer<W, TermInfoWriter>,
}

impl<W: io::Write> TermDictionaryBuilder<W> {
    /// Creates a new `TermDictionaryBuilder`
    pub fn create(w: W) -> io::Result<Self> {
        let sstable_writer = TermSSTable::writer(w);
        Ok(TermDictionaryBuilder { sstable_writer })
    }

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    pub fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
        let key = key_ref.as_ref();
        self.insert_key(key)?;
        self.insert_value(value)?;
        Ok(())
    }

    /// # Warning
    /// Horribly dangerous internal API
    ///
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// Prefer using `.insert(key, value)`
    #[allow(clippy::clippy::clippy::unnecessary_wraps)]
    pub(crate) fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        self.sstable_writer.write_key(key);
        Ok(())
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert_key(...)`.
    pub(crate) fn insert_value(&mut self, term_info: &TermInfo) -> io::Result<()> {
        self.sstable_writer.write_value(term_info)
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    pub fn finish(self) -> io::Result<W> {
        self.sstable_writer.finalize()
    }
}

static EMPTY_TERM_DICT_FILE: Lazy<FileSlice> = Lazy::new(|| {
    let term_dictionary_data: Vec<u8> = TermDictionaryBuilder::create(Vec::<u8>::new())
        .expect("Creating a TermDictionaryBuilder in a Vec<u8> should never fail")
        .finish()
        .expect("Writing in a Vec<u8> should never fail");
    FileSlice::from(term_dictionary_data)
});

/// The term dictionary contains all of the terms in
/// `tantivy index` in a sorted manner.
///
/// The `Fst` crate is used to associate terms to their
/// respective `TermOrdinal`. The `TermInfoStore` then makes it
/// possible to fetch the associated `TermInfo`.
pub struct TermDictionary {
    sstable_slice: FileSlice,
    sstable_index: SSTableIndex,
    num_terms: u64,
}

impl TermDictionary {
    pub(crate) fn sstable_reader(&self) -> io::Result<Reader<'static, TermInfoReader>> {
        let data = self.sstable_slice.read_bytes()?;
        Ok(TermInfoSSTable::reader(data))
    }

    pub(crate) fn sstable_reader_block(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<Reader<'static, TermInfoReader>> {
        let data = self.sstable_slice.read_bytes_slice(block_addr.byte_range)?;
        Ok(TermInfoSSTable::reader(data))
    }

    pub(crate) async fn sstable_reader_block_async(
        &self,
        block_addr: BlockAddr,
    ) -> AsyncIoResult<Reader<'static, TermInfoReader>> {
        let data = self
            .sstable_slice
            .read_bytes_slice_async(block_addr.byte_range)
            .await?;
        Ok(TermInfoSSTable::reader(data))
    }

    pub(crate) fn sstable_delta_reader(&self) -> io::Result<DeltaReader<'static, TermInfoReader>> {
        let data = self.sstable_slice.read_bytes()?;
        Ok(TermInfoSSTable::delta_reader(data))
    }

    /// Opens a `TermDictionary`.
    pub fn open(term_dictionary_file: FileSlice) -> crate::Result<Self> {
        let (main_slice, footer_len_slice) = term_dictionary_file.split_from_end(16);
        let mut footer_len_bytes: OwnedBytes = footer_len_slice.read_bytes()?;
        let index_offset = u64::deserialize(&mut footer_len_bytes)?;
        let num_terms = u64::deserialize(&mut footer_len_bytes)?;
        let (sstable_slice, index_slice) = main_slice.split(index_offset as usize);
        let sstable_index_bytes = index_slice.read_bytes()?;
        let sstable_index = SSTableIndex::load(sstable_index_bytes.as_slice())?;
        Ok(TermDictionary {
            sstable_slice,
            sstable_index,
            num_terms,
        })
    }

    pub fn from_bytes(owned_bytes: OwnedBytes) -> crate::Result<TermDictionary> {
        TermDictionary::open(FileSlice::new(Box::new(owned_bytes)))
    }

    /// Creates an empty term dictionary which contains no terms.
    pub fn empty() -> Self {
        TermDictionary::open(EMPTY_TERM_DICT_FILE.clone()).unwrap()
    }

    /// Returns the number of terms in the dictionary.
    /// Term ordinals range from 0 to `num_terms() - 1`.
    pub fn num_terms(&self) -> usize {
        self.num_terms as usize
    }

    /// Returns the ordinal associated to a given term.
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

    /// Returns the term associated to a given term ordinal.
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
    pub fn term_info_from_ord(&self, term_ord: TermOrdinal) -> io::Result<TermInfo> {
        let mut sstable_reader = self.sstable_reader()?;
        for _ in 0..(term_ord + 1) {
            if !sstable_reader.advance().unwrap_or(false) {
                return Ok(TermInfo::default());
            }
        }
        Ok(sstable_reader.value().clone())
    }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermInfo>> {
        if let Some(block_addr) = self.sstable_index.search(key.as_ref()) {
            let mut sstable_reader = self.sstable_reader_block(block_addr)?;
            let key_bytes = key.as_ref();
            while sstable_reader.advance().unwrap_or(false) {
                if sstable_reader.key() == key_bytes {
                    let term_info = sstable_reader.value().clone();
                    return Ok(Some(term_info));
                }
            }
        }
        Ok(None)
    }

    /// Lookups the value corresponding to the key.
    pub async fn get_async<K: AsRef<[u8]>>(&self, key: K) -> AsyncIoResult<Option<TermInfo>> {
        if let Some(block_addr) = self.sstable_index.search(key.as_ref()) {
            let mut sstable_reader = self.sstable_reader_block_async(block_addr).await?;
            let key_bytes = key.as_ref();
            while sstable_reader.advance().unwrap_or(false) {
                if sstable_reader.key() == key_bytes {
                    let term_info = sstable_reader.value().clone();
                    return Ok(Some(term_info));
                }
            }
        }
        Ok(None)
    }

    // Returns a range builder, to stream all of the terms
    // within an interval.
    pub fn range(&self) -> TermStreamerBuilder<'_> {
        TermStreamerBuilder::new(self, AlwaysMatch)
    }

    // A stream of all the sorted terms. [See also `.stream_field()`](#method.stream_field)
    pub fn stream(&self) -> io::Result<TermStreamer<'_>> {
        self.range().into_stream()
    }

    // Returns a search builder, to stream all of the terms
    // within the Automaton
    pub fn search<'a, A: Automaton + 'a>(&'a self, automaton: A) -> TermStreamerBuilder<'a, A>
    where A::State: Clone {
        TermStreamerBuilder::<A>::new(self, automaton)
    }

    #[doc(hidden)]
    pub async fn warm_up_dictionary(&self) -> AsyncIoResult<()> {
        self.sstable_slice.read_bytes_async().await?;
        Ok(())
    }
}
