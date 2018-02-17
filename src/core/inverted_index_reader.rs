use directory::{ReadOnlySource, SourceRead};
use termdict::{TermDictionary, TermDictionaryImpl};
use postings::{BlockSegmentPostings, SegmentPostings};
use postings::TermInfo;
use schema::IndexRecordOption;
use schema::Term;
use std::cmp;
use fastfield::DeleteBitSet;
use schema::Schema;
use compression::CompressedIntStream;
use postings::FreqReadingOption;

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
    termdict: TermDictionaryImpl,
    postings_source: ReadOnlySource,
    positions_source: ReadOnlySource,
    delete_bitset: DeleteBitSet,
    record_option: IndexRecordOption
}

impl InvertedIndexReader {
    pub(crate) fn new(
        termdict_source: ReadOnlySource,
        postings_source: ReadOnlySource,
        positions_source: ReadOnlySource,
        delete_bitset: DeleteBitSet,
        record_option: IndexRecordOption,
    ) -> InvertedIndexReader {
        InvertedIndexReader {
            termdict: TermDictionaryImpl::from_source(termdict_source),
            postings_source,
            positions_source,
            delete_bitset,
            record_option
        }
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> Option<TermInfo> {
        self.termdict.get(term.value_bytes())
    }

    /// Return the term dictionary datastructure.
    pub fn terms(&self) -> &TermDictionaryImpl {
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
        let postings_reader = SourceRead::from(postings_slice);
        block_postings.reset(term_info.doc_freq as usize, postings_reader);
    }

    /// Returns a block postings given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        requested_option: IndexRecordOption
    ) -> BlockSegmentPostings {
        let offset = term_info.postings_offset as usize;
        let postings_data = self.postings_source.slice_from(offset);
        let freq_reading_option = match (self.record_option, requested_option) {
            (IndexRecordOption::Basic, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq
        };
        BlockSegmentPostings::from_data(
            term_info.doc_freq as usize,
            SourceRead::from(postings_data),
            freq_reading_option
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
        let delete_bitset = self.delete_bitset.clone();
        let position_stream = {
            if option.has_positions() {
                let position_offset = term_info.positions_offset;
                let positions_source = self.positions_source.slice_from(position_offset as usize);
                let mut stream = CompressedIntStream::wrap(positions_source);
                stream.skip(term_info.positions_inner_offset as usize);
                Some(stream)
            } else {
                None
            }
        };
        SegmentPostings::from_block_postings(block_postings, delete_bitset, position_stream)
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
        let field = term.field();
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
