use common::BinarySerializable;
use directory::ReadOnlySource;
use postings::TermInfo;
use postings::{BlockSegmentPostings, SegmentPostings};
use schema::FieldType;
use schema::IndexRecordOption;
use schema::Term;
use termdict::TermDictionary;
use owned_read::OwnedRead;
use positions::PositionReader;

/// The inverted index reader is in charge of accessing
/// the inverted index associated to a specific field.
///
/// # Note
///
/// It is safe to delete the segment associated to
/// an `InvertedIndexReader`. As long as it is open,
/// the `ReadOnlySource` it is relying on should
/// stay available.
///
///
/// `InvertedIndexReader` are created by calling
/// the `SegmentReader`'s [`.inverted_index(...)`] method
pub struct InvertedIndexReader {
    termdict: TermDictionary,
    postings_source: ReadOnlySource,
    positions_source: ReadOnlySource,
    positions_idx_source: ReadOnlySource,
    record_option: IndexRecordOption,
    total_num_tokens: u64,
}

impl InvertedIndexReader {
    pub(crate) fn new(
        termdict: TermDictionary,
        postings_source: ReadOnlySource,
        positions_source: ReadOnlySource,
        positions_idx_source: ReadOnlySource,
        record_option: IndexRecordOption,
    ) -> InvertedIndexReader {
        let total_num_tokens_data = postings_source.slice(0, 8);
        let mut total_num_tokens_cursor = total_num_tokens_data.as_slice();
        let total_num_tokens = u64::deserialize(&mut total_num_tokens_cursor).unwrap_or(0u64);
        InvertedIndexReader {
            termdict,
            postings_source: postings_source.slice_from(8),
            positions_source,
            positions_idx_source,
            record_option,
            total_num_tokens,
        }
    }

    /// Creates an empty `InvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(field_type: FieldType) -> InvertedIndexReader {
        let record_option = field_type
            .get_index_record_option()
            .unwrap_or(IndexRecordOption::Basic);
        InvertedIndexReader {
            termdict: TermDictionary::empty(field_type),
            postings_source: ReadOnlySource::empty(),
            positions_source: ReadOnlySource::empty(),
            positions_idx_source: ReadOnlySource::empty(),
            record_option,
            total_num_tokens: 0u64,
        }
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> Option<TermInfo> {
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
    ) {
        let offset = term_info.postings_offset as usize;
        let end_source = self.postings_source.len();
        let postings_slice = self.postings_source.slice(offset, end_source);
        let postings_reader = OwnedRead::new(postings_slice);
        block_postings.reset(term_info.doc_freq, postings_reader);
    }

    /// Returns a block postings given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        requested_option: IndexRecordOption,
    ) -> BlockSegmentPostings {
        let offset = term_info.postings_offset as usize;
        let postings_data = self.postings_source.slice_from(offset);
        BlockSegmentPostings::from_data(
            term_info.doc_freq,
            OwnedRead::new(postings_data),
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
    ) -> SegmentPostings {
        let block_postings = self.read_block_postings_from_terminfo(term_info, option);
        let position_stream = {
            if option.has_positions() {
                let position_reader = self.positions_source.clone();
                let skip_reader = self.positions_idx_source.clone();
                let position_reader = PositionReader::new(position_reader, skip_reader, term_info.positions_idx);
                Some(position_reader)
            } else {
                None
            }
        };
        SegmentPostings::from_block_postings(block_postings, position_stream)
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
    pub fn read_postings(&self, term: &Term, option: IndexRecordOption) -> Option<SegmentPostings> {
        let term_info = get!(self.get_term_info(term));
        Some(self.read_postings_from_terminfo(&term_info, option))
    }

    pub(crate) fn read_postings_no_deletes(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> Option<SegmentPostings> {
        let term_info = get!(self.get_term_info(term));
        Some(self.read_postings_from_terminfo(&term_info, option))
    }

    /// Returns the number of documents containing the term.
    pub fn doc_freq(&self, term: &Term) -> u32 {
        self.get_term_info(term)
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32)
    }
}
