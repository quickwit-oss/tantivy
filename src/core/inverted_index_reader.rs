use std::io;

use crate::common::BinarySerializable;
use crate::directory::FileSlice;
use crate::positions::PositionReader;
use crate::postings::TermInfo;
use crate::postings::{BlockSegmentPostings, SegmentPostings};
use crate::schema::IndexRecordOption;
use crate::schema::Term;
use crate::termdict::TermDictionary;

/// The inverted index reader is in charge of accessing
/// the inverted index associated to a specific field.
///
/// # Note
///
/// It is safe to delete the segment associated to
/// an `InvertedIndexReader`. As long as it is open,
/// the `FileSlice` it is relying on should
/// stay available.
///
///
/// `InvertedIndexReader` are created by calling
/// the `SegmentReader`'s [`.inverted_index(...)`] method
pub struct InvertedIndexReader {
    termdict: TermDictionary,
    postings_file_slice: FileSlice,
    positions_file_slice: FileSlice,
    positions_idx_file_slice: FileSlice,
    record_option: IndexRecordOption,
    total_num_tokens: u64,
}

impl InvertedIndexReader {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))] // for symmetry
    pub(crate) fn new(
        termdict: TermDictionary,
        postings_file_slice: FileSlice,
        positions_file_slice: FileSlice,
        positions_idx_file_slice: FileSlice,
        record_option: IndexRecordOption,
    ) -> io::Result<InvertedIndexReader> {
        let (total_num_tokens_slice, postings_body) = postings_file_slice.split(8);
        let total_num_tokens = u64::deserialize(&mut total_num_tokens_slice.read_bytes()?)?;
        Ok(InvertedIndexReader {
            termdict,
            postings_file_slice: postings_body,
            positions_file_slice,
            positions_idx_file_slice,
            record_option,
            total_num_tokens,
        })
    }

    /// Creates an empty `InvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(record_option: IndexRecordOption) -> InvertedIndexReader {
        InvertedIndexReader {
            termdict: TermDictionary::empty(),
            postings_file_slice: FileSlice::empty(),
            positions_file_slice: FileSlice::empty(),
            positions_idx_file_slice: FileSlice::empty(),
            record_option,
            total_num_tokens: 0u64,
        }
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> io::Result<Option<TermInfo>> {
        self.termdict.get(term.value_bytes())
    }

    /// Return the term dictionary datastructure.
    pub fn terms(&self) -> &TermDictionary {
        &self.termdict
    }

    /// Resets the block segment to another position of the postings
    /// file.
    ///
    /// This is useful for enumerating through a list of terms,
    /// and consuming the associated posting lists while avoiding
    /// reallocating a `BlockSegmentPostings`.
    ///
    /// # Warning
    ///
    /// This does not reset the positions list.
    pub fn reset_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        block_postings: &mut BlockSegmentPostings,
    ) -> io::Result<()> {
        let postings_slice = self
            .postings_file_slice
            .slice(term_info.postings_range.clone());
        block_postings.reset(term_info.doc_freq, postings_slice.read_bytes()?);
        Ok(())
    }

    /// Returns a block postings given a `Term`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_block_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<BlockSegmentPostings>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_block_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    /// Returns a block postings given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        requested_option: IndexRecordOption,
    ) -> io::Result<BlockSegmentPostings> {
        let postings_data = self
            .postings_file_slice
            .slice(term_info.postings_range.clone());
        BlockSegmentPostings::open(
            term_info.doc_freq,
            postings_data,
            self.record_option,
            requested_option,
        )
    }

    /// Returns a posting object given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<SegmentPostings> {
        let block_postings = self.read_block_postings_from_terminfo(term_info, option)?;
        let position_stream = {
            if option.has_positions() {
                let position_reader = self.positions_file_slice.clone();
                let skip_reader = self.positions_idx_file_slice.clone();
                let position_reader =
                    PositionReader::new(position_reader, skip_reader, term_info.positions_idx)?;
                Some(position_reader)
            } else {
                None
            }
        };
        Ok(SegmentPostings::from_block_postings(
            block_postings,
            position_stream,
        ))
    }

    /// Returns the total number of tokens recorded for all documents
    /// (including deleted documents).
    pub fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }

    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encountered and indexed.
    ///
    /// If the field was not indexed with the indexing options that cover
    /// the requested options, the returned `SegmentPostings` the method does not fail
    /// and returns a `SegmentPostings` with as much information as possible.
    ///
    /// For instance, requesting `IndexRecordOption::Freq` for a
    /// `TextIndexingOptions` that does not index position will return a `SegmentPostings`
    /// with `DocId`s and frequencies.
    pub fn read_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<SegmentPostings>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    pub(crate) fn read_postings_no_deletes(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<SegmentPostings>> {
        self.get_term_info(term)?
            .map(|term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    /// Returns the number of documents containing the term.
    pub fn doc_freq(&self, term: &Term) -> io::Result<u32> {
        Ok(self
            .get_term_info(term)?
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32))
    }
}
