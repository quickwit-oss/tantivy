use directory::{SourceRead, ReadOnlySource};
use termdict::{TermDictionary, TermDictionaryImpl};
use std::io;
use postings::{SegmentPostings, BlockSegmentPostings};
use postings::TermInfo;
use postings::SegmentPostingsOption;
use schema::Term;
use std::cmp;
use fastfield::DeleteBitSet;
use schema::Schema;
use compression::CompressedIntStream;

pub struct FieldReader {
    termdict: TermDictionaryImpl,
    postings_source: ReadOnlySource,
    positions_source: ReadOnlySource,
    delete_bitset: DeleteBitSet,
    schema: Schema,
}

impl FieldReader {

    pub(crate) fn new(
        termdict_source: ReadOnlySource,
        postings_source: ReadOnlySource,
        positions_source: ReadOnlySource,
        delete_bitset: DeleteBitSet,
        schema: Schema,
    ) -> io::Result<FieldReader> {

        Ok(FieldReader {
            termdict: TermDictionaryImpl::from_source(termdict_source)?,
            postings_source: postings_source,
            positions_source: positions_source,
            delete_bitset: delete_bitset,
            schema: schema,
        })
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> Option<TermInfo> {
        self.termdict.get(term.as_slice())
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
    pub fn reset_block_postings_from_terminfo(&self,
                                                  term_info: &TermInfo,
                                                  block_postings: &mut BlockSegmentPostings) {
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
    pub fn read_block_postings_from_terminfo(&self,
                                             term_info: &TermInfo,
                                             option: SegmentPostingsOption)
                                             -> BlockSegmentPostings {
        let offset = term_info.postings_offset as usize;
        let postings_data = self.postings_source.slice_from(offset);
        let has_freq = option.has_freq();
        BlockSegmentPostings::from_data(
            term_info.doc_freq as usize,
            SourceRead::from(postings_data),
            has_freq)
    }

    /// Returns a posting object given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_postings_from_terminfo(&self,
                                       term_info: &TermInfo,
                                       option: SegmentPostingsOption)
                                       -> SegmentPostings {
        let block_postings = self.read_block_postings_from_terminfo(term_info, option);
        let delete_bitset = self.delete_bitset.clone();
        let position_stream = {
            if option.has_positions() {
                let position_offset = term_info.positions_offset;
                let positions_reader = SourceRead::from(self.positions_source.slice_from(position_offset as usize));
                let mut stream = CompressedIntStream::wrap(positions_reader);
                stream.skip(term_info.positions_inner_offset as usize);
                Some(stream)
            }
            else {
                None
            }
        };
        SegmentPostings::from_block_postings(
            block_postings,
            delete_bitset,
            position_stream
        )
    }

    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encountered and indexed.
    ///
    /// If the field was not indexed with the indexing options that cover
    /// the requested options, the returned `SegmentPostings` the method does not fail
    /// and returns a `SegmentPostings` with as much information as possible.
    ///
    /// For instance, requesting `SegmentPostingsOption::FreqAndPositions` for a
    /// `TextIndexingOptions` that does not index position will return a `SegmentPostings`
    /// with `DocId`s and frequencies.
    pub fn read_postings(&self,
                         term: &Term,
                         option: SegmentPostingsOption)
                         -> Option<SegmentPostings> {
        let field = term.field();
        let field_entry = self.schema.get_field_entry(field);
        let term_info = get!(self.get_term_info(term));
        let maximum_option = get!(field_entry.field_type().get_segment_postings_option());
        let best_effort_option = cmp::min(maximum_option, option);
        Some(self.read_postings_from_terminfo(&term_info, best_effort_option))
    }

    /// Returns the number of documents containing the term.
    pub fn doc_freq(&self, term: &Term) -> u32 {
        match self.get_term_info(term) {
            Some(term_info) => term_info.doc_freq,
            None => 0,
        }
    }
}
