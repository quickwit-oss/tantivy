//! The term dictionary main role is to associate the sorted [`Term`s](crate::Term) to
//! a [`TermInfo`] struct that contains some meta-information
//! about the term.
//!
//! Internally, the term dictionary relies on the `fst` crate to store
//! a sorted mapping that associate each term to its rank in the lexicographical order.
//! For instance, in a dictionary containing the sorted terms "abba", "bjork", "blur" and "donovan",
//! the [`TermOrdinal`] are respectively `0`, `1`, `2`, and `3`.
//!
//! For `u64`-terms, tantivy explicitly uses a `BigEndian` representation to ensure that the
//! lexicographical order matches the natural order of integers.
//!
//! `i64`-terms are transformed to `u64` using a continuous mapping `val ⟶ val - i64::MIN`
//! and then treated as a `u64`.
//!
//! `f64`-terms are transformed to `u64` using a mapping that preserve order, and are then treated
//! as `u64`.
//!
//! A second datastructure makes it possible to access a [`TermInfo`].

#[cfg(not(feature = "quickwit"))]
mod fst_termdict;
#[cfg(not(feature = "quickwit"))]
use fst_termdict as termdict;

#[cfg(feature = "quickwit")]
mod sstable_termdict;
#[cfg(feature = "quickwit")]
use sstable_termdict as termdict;

#[cfg(test)]
mod tests;

/// Position of the term in the sorted list of terms.
pub type TermOrdinal = u64;

use std::io;

use common::file_slice::FileSlice;
use common::{BinarySerializable, HasLen};
pub use common::{KeyTracking, WithoutKeys};
use tantivy_fst::Automaton;

use self::termdict::{
    TermDictionary as InnerTermDict, TermDictionaryBuilder as InnerTermDictBuilder,
    TermStreamerBuilder,
};
pub use self::termdict::{TermMerger, TermStreamer};
use crate::postings::TermInfo;

#[cfg(feature = "quickwit")]
pub(crate) fn validate_max_inline_docs(maximum: u32) -> Result<u32, &'static str> {
    match maximum {
        0..=32 => Ok(maximum),
        _ => Err("max_inline_docs must be in 0..=32"),
    }
}

#[derive(Debug, Eq, PartialEq)]
#[repr(u32)]
enum DictionaryType {
    Fst = 1,
    SSTable = 2,
    SSTableWithPostings = 3,
    SSTableWithInlineDocLimit = 4,
}

impl TryFrom<u32> for DictionaryType {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(DictionaryType::Fst),
            2 => Ok(DictionaryType::SSTable),
            3 => Ok(DictionaryType::SSTableWithPostings),
            4 => Ok(DictionaryType::SSTableWithInlineDocLimit),
            _ => Err("Invalid value for DictionaryType"),
        }
    }
}

#[cfg(not(feature = "quickwit"))]
const CURRENT_TYPE: DictionaryType = DictionaryType::Fst;

#[cfg(feature = "quickwit")]
const CURRENT_TYPE: DictionaryType = DictionaryType::SSTableWithInlineDocLimit;

// TODO in the future this should become an enum of supported dictionaries
/// A TermDictionary wrapping either an FST based dictionary or a SSTable based one.
#[derive(Clone)]
pub struct TermDictionary(InnerTermDict);

impl TermDictionary {
    /// Opens a `TermDictionary`.
    pub fn open(file: FileSlice) -> io::Result<Self> {
        if file.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing dictionary type",
            ));
        }
        let trailer = file.slice(file.len().saturating_sub(5)..).read_bytes()?;
        let (main_slice, _) = file.split_from_end(4);
        let dict_type = u32::deserialize(&mut &trailer[trailer.len() - 4..])?;
        let dict_type = DictionaryType::try_from(dict_type).map_err(|_| {
            io::Error::other(format!("Unsupported dictionary type, found {dict_type}"))
        })?;

        let legacy_sstable = cfg!(feature = "quickwit")
            && matches!(
                dict_type,
                DictionaryType::SSTable | DictionaryType::SSTableWithPostings
            );
        if dict_type != CURRENT_TYPE && !legacy_sstable {
            return Err(io::Error::other(format!(
                "Unsupported dictionary type, compiled tantivy with {CURRENT_TYPE:?}, but got \
                 {dict_type:?}",
            )));
        }

        #[cfg(feature = "quickwit")]
        {
            let (sstable, maximum) = match dict_type {
                DictionaryType::SSTableWithInlineDocLimit => {
                    let maximum = u8::deserialize(&mut &trailer[..trailer.len() - 4])?;
                    (main_slice.split_from_end(1).0, maximum)
                }
                DictionaryType::SSTableWithPostings => (main_slice, 8),
                _ => (main_slice, 0),
            };
            InnerTermDict::open(sstable)?
                .with_value_config(maximum)
                .map(TermDictionary)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
        }
        #[cfg(not(feature = "quickwit"))]
        InnerTermDict::open(main_slice).map(TermDictionary)
    }

    /// Creates an empty term dictionary which contains no terms.
    pub fn empty() -> Self {
        TermDictionary(InnerTermDict::empty())
    }

    /// Returns the number of terms in the dictionary.
    /// Term ordinals range from 0 to `num_terms() - 1`.
    pub fn num_terms(&self) -> usize {
        self.0.num_terms()
    }

    /// Returns the ordinal associated with a given term.
    pub fn term_ord<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermOrdinal>> {
        self.0.term_ord(key)
    }

    /// Stores the term associated with a given term ordinal in
    /// a `bytes` buffer.
    ///
    /// Term ordinals are defined as the position of the term in
    /// the sorted list of terms.
    ///
    /// Returns true if and only if the term has been found.
    ///
    /// Regardless of whether the term is found or not,
    /// the buffer may be modified.
    pub fn ord_to_term(&self, ord: TermOrdinal, bytes: &mut Vec<u8>) -> io::Result<bool> {
        self.0.ord_to_term(ord, bytes)
    }

    // this isn't used, and has different prototype in Fst and SSTable
    // Returns the number of terms in the dictionary.
    // pub fn term_info_from_ord(&self, term_ord: TermOrdinal) -> TermInfo {
    // self.0.term_info_from_ord(term_ord)
    // }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermInfo>> {
        self.0.get(key)
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range(&self) -> TermStreamerBuilder<'_> {
        self.0.range()
    }

    /// A stream of all the sorted terms.
    pub fn stream(&self) -> io::Result<TermStreamer<'_>> {
        self.0.stream()
    }

    /// Returns a search builder, to stream all of the terms
    /// within the Automaton
    pub fn search<'a, A: Automaton + 'a>(&'a self, automaton: A) -> TermStreamerBuilder<'a, A>
    where A::State: Clone {
        self.0.search(automaton)
    }

    #[cfg(feature = "quickwit")]
    /// Lookups the value corresponding to the key.
    pub async fn get_async<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermInfo>> {
        self.0.get_async(key).await
    }

    #[cfg(feature = "quickwit")]
    #[doc(hidden)]
    pub async fn warm_up_dictionary(&self) -> io::Result<()> {
        self.0.warm_up_dictionary().await
    }

    #[cfg(feature = "quickwit")]
    /// Returns a file slice covering a set of sstable blocks
    /// that includes the key range passed in arguments.
    pub fn file_slice_for_range(
        &self,
        key_range: impl std::ops::RangeBounds<[u8]>,
        limit: Option<u64>,
    ) -> FileSlice {
        self.0.file_slice_for_range(key_range, limit)
    }
}

/// A TermDictionaryBuilder wrapping either an FST or a SSTable dictionary builder.
pub struct TermDictionaryBuilder<W: io::Write>(InnerTermDictBuilder<W>);

impl<W: io::Write> TermDictionaryBuilder<W> {
    /// Creates a new `TermDictionaryBuilder`
    pub fn create(w: W) -> io::Result<Self> {
        InnerTermDictBuilder::create(w).map(TermDictionaryBuilder)
    }

    /// Creates a dictionary embedding lists of at most `max_inline_docs` documents.
    /// Accepts 0 through 32. Zero disables embedding.
    #[cfg(feature = "quickwit")]
    pub fn create_with_max_inline_docs(w: W, max_inline_docs: u32) -> io::Result<Self> {
        Ok(Self(InnerTermDictBuilder::with_value_writer(
            w,
            termdict::TermInfoBlock::new(max_inline_docs)?,
        )))
    }

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    pub fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
        if !cfg!(feature = "quickwit") && value.short_postings.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "short postings require an SSTable",
            ));
        }
        self.0.insert(key_ref, value)
    }

    /// # Warning
    /// Horribly dangerous internal API
    ///
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// Prefer using `.insert(key, value)`
    pub fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        self.0.insert_key(key)
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert_key(...)`.
    pub fn insert_value(&mut self, term_info: &TermInfo) -> io::Result<()> {
        if !cfg!(feature = "quickwit") && term_info.short_postings.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "short postings require an SSTable",
            ));
        }
        self.0.insert_value(term_info)
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    pub fn finish(self) -> io::Result<W> {
        #[cfg(feature = "quickwit")]
        let max_inline_docs = self.0.value_writer().max_inline_docs;
        let mut writer = self.0.finish()?;
        #[cfg(feature = "quickwit")]
        writer.write_all(&[max_inline_docs as u8])?;
        (CURRENT_TYPE as u32).serialize(&mut writer)?;
        Ok(writer)
    }
}
