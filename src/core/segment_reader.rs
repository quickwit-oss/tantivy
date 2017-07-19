use Result;
use core::Segment;
use core::SegmentId;
use core::SegmentComponent;
use schema::Term;
use common::HasLen;
use core::SegmentMeta;
use fastfield::{self, FastFieldNotAvailableError};
use fastfield::DeleteBitSet;
use store::StoreReader;
use schema::Document;
use directory::ReadOnlySource;
use DocId;
use std::str;
use termdict::TermDictionary;
use std::cmp;
use postings::TermInfo;
use termdict::TermDictionaryImpl;
use std::sync::Arc;
use std::fmt;
use schema::Field;
use postings::SegmentPostingsOption;
use postings::{SegmentPostings, BlockSegmentPostings};
use fastfield::{FastFieldsReader, FastFieldReader, U64FastFieldReader};
use schema::Schema;
use postings::FreqHandler;



/// Entry point to access all of the datastructures of the `Segment`
///
/// - term dictionary
/// - postings
/// - store
/// - fast field readers
/// - field norm reader
///
/// The segment reader has a very low memory footprint,
/// as close to all of the memory data is mmapped.
///
#[derive(Clone)]
pub struct SegmentReader {
    segment_id: SegmentId,
    segment_meta: SegmentMeta,
    terms: Arc<TermDictionaryImpl>,
    postings_data: ReadOnlySource,
    store_reader: StoreReader,
    fast_fields_reader: Arc<FastFieldsReader>,
    fieldnorms_reader: Arc<FastFieldsReader>,
    delete_bitset: DeleteBitSet,
    positions_data: ReadOnlySource,
    schema: Schema,
}

impl SegmentReader {
    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    /// Today, `tantivy` does not handle deletes, so it happens
    /// to also be the number of documents in the index.
    pub fn max_doc(&self) -> DocId {
        self.segment_meta.max_doc()
    }

    /// Returns the number of documents.
    /// Deleted documents are not counted.
    ///
    /// Today, `tantivy` does not handle deletes so max doc and
    /// num_docs are the same.
    pub fn num_docs(&self) -> DocId {
        self.segment_meta.num_docs()
    }

    /// Return the number of documents that have been
    /// deleted in the segment.
    pub fn num_deleted_docs(&self) -> DocId {
        self.delete_bitset.len() as DocId
    }

    #[doc(hidden)]
    pub fn fast_fields_reader(&self) -> &FastFieldsReader {
        &*self.fast_fields_reader
    }

    /// Accessor to a segment's fast field reader given a field.
    ///
    /// Returns the u64 fast value reader if the field
    /// is a u64 field indexed as "fast".
    ///
    /// Return a FastFieldNotAvailableError if the field is not
    /// declared as a fast field in the schema.
    ///
    /// # Panics
    /// May panic if the index is corrupted.
    pub fn get_fast_field_reader<TFastFieldReader: FastFieldReader>
        (&self,
         field: Field)
         -> fastfield::Result<TFastFieldReader> {
        let field_entry = self.schema.get_field_entry(field);
        if !TFastFieldReader::is_enabled(field_entry.field_type()) {
            Err(FastFieldNotAvailableError::new(field_entry))
        } else {
            Ok(self.fast_fields_reader
                   .open_reader(field)
                   .expect("Fast field file corrupted."))
        }
    }

    /// Accessor to the segment's `Field norms`'s reader.
    ///
    /// Field norms are the length (in tokens) of the fields.
    /// It is used in the computation of the [TfIdf]
    /// (https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html).
    ///
    /// They are simply stored as a fast field, serialized in
    /// the `.fieldnorm` file of the segment.
    pub fn get_fieldnorms_reader(&self, field: Field) -> Option<U64FastFieldReader> {
        self.fieldnorms_reader.open_reader(field)
    }

    /// Returns the number of documents containing the term.
    pub fn doc_freq(&self, term: &Term) -> u32 {
        match self.get_term_info(term) {
            Some(term_info) => term_info.doc_freq,
            None => 0,
        }
    }

    /// Accessor to the segment's `StoreReader`.
    pub fn get_store_reader(&self) -> &StoreReader {
        &self.store_reader
    }

    /// Open a new segment for reading.
    pub fn open(segment: Segment) -> Result<SegmentReader> {

        let source = segment.open_read(SegmentComponent::TERMS)?;
        let terms = TermDictionaryImpl::from_source(source)?;

        let store_source = segment.open_read(SegmentComponent::STORE)?;
        let store_reader = StoreReader::from_source(store_source);

        let postings_shared_mmap = segment.open_read(SegmentComponent::POSTINGS)?;

        let fast_field_data = segment.open_read(SegmentComponent::FASTFIELDS)?;
        let fast_fields_reader = FastFieldsReader::from_source(fast_field_data)?;

        let fieldnorms_data = segment.open_read(SegmentComponent::FIELDNORMS)?;
        let fieldnorms_reader = FastFieldsReader::from_source(fieldnorms_data)?;

        let positions_data = segment
            .open_read(SegmentComponent::POSITIONS)
            .unwrap_or_else(|_| ReadOnlySource::empty());

        let delete_bitset = if segment.meta().has_deletes() {
            let delete_data = segment.open_read(SegmentComponent::DELETE)?;
            DeleteBitSet::open(delete_data)
        } else {
            DeleteBitSet::empty()
        };

        let schema = segment.schema();
        Ok(SegmentReader {
               segment_meta: segment.meta().clone(),
               postings_data: postings_shared_mmap,
               terms: Arc::new(terms),
               segment_id: segment.id(),
               store_reader: store_reader,
               fast_fields_reader: Arc::new(fast_fields_reader),
               fieldnorms_reader: Arc::new(fieldnorms_reader),
               delete_bitset: delete_bitset,
               positions_data: positions_data,
               schema: schema,
           })
    }

    /// Return the term dictionary datastructure.
    pub fn terms(&self) -> &TermDictionaryImpl {
        &self.terms
    }

    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn doc(&self, doc_id: DocId) -> Result<Document> {
        self.store_reader.get(doc_id)
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
        SegmentPostings::from_block_postings(block_postings, delete_bitset)
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
        let postings_data = &self.postings_data[offset..];
        let freq_handler = match option {
            SegmentPostingsOption::NoFreq => FreqHandler::new_without_freq(),
            SegmentPostingsOption::Freq => FreqHandler::new_with_freq(),
            SegmentPostingsOption::FreqAndPositions => {
                let offset = term_info.positions_offset as usize;
                let offseted_position_data = &self.positions_data[offset..];
                FreqHandler::new_with_freq_and_position(offseted_position_data)
            }
        };
        BlockSegmentPostings::from_data(term_info.doc_freq as usize, postings_data, freq_handler)
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
    pub fn reset_block_postings_from_terminfo<'a>(&'a self,
                                                  term_info: &TermInfo,
                                                  block_postings: &mut BlockSegmentPostings<'a>) {
        let offset = term_info.postings_offset as usize;
        let postings_data: &'a [u8] = &self.postings_data[offset..];
        block_postings.reset(term_info.doc_freq as usize, postings_data);
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> Option<TermInfo> {
        self.terms.get(term.as_slice())
    }

    /// Returns the segment id
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Returns the bitset representing
    /// the documents that have been deleted.
    pub fn delete_bitset(&self) -> &DeleteBitSet {
        &self.delete_bitset
    }


    /// Returns true iff the `doc` is marked
    /// as deleted.
    pub fn is_deleted(&self, doc: DocId) -> bool {
        self.delete_bitset.is_deleted(doc)
    }
}


impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}
