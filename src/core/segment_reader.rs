use Result;
use core::Segment;
use core::SegmentId;
use core::SegmentComponent;
use schema::Term;
use common::HasLen;
use core::SegmentMeta;
use fastfield::{self, FastFieldNotAvailableError};
use fastfield::DeleteBitSet;
use postings::BlockSegmentPostings;
use store::StoreReader;
use schema::Document;
use directory::ReadOnlySource;
use DocId;
use std::str;
use postings::TermInfo;
use datastruct::TermDictionary;
use std::sync::Arc;
use std::fmt;
use schema::Field;
use postings::SegmentPostingsOption;
use postings::SegmentPostings;
use fastfield::{FastFieldsReader, FastFieldReader, U64FastFieldReader};
use schema::Schema;
use schema::FieldType;
use postings::FreqHandler;
use schema::TextIndexingOptions;



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
    term_infos: Arc<TermDictionary<TermInfo>>,
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
    

    pub fn schema(&self) -> &Schema {
        &self.schema
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
    pub fn get_fast_field_reader<TFastFieldReader: FastFieldReader>(&self, field: Field) -> fastfield::Result<TFastFieldReader> {
        let field_entry = self.schema.get_field_entry(field);
        if !TFastFieldReader::is_enabled(field_entry.field_type()) {
            Err(FastFieldNotAvailableError::new(field_entry))
        }
        else {
            Ok(
                self.fast_fields_reader
                .open_reader(field)
                .expect("Fast field file corrupted.")
            )
        }
    }
    
    /// Accessor to the segment's `Field norms`'s reader.
    ///
    /// Field norms are the length (in tokens) of the fields.
    /// It is used in the computation of the [TfIdf](https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html).
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

        let source = try!(segment.open_read(SegmentComponent::TERMS));
        let term_infos = try!(TermDictionary::from_source(source));
        let store_reader = StoreReader::from(try!(segment.open_read(SegmentComponent::STORE)));
        let postings_shared_mmap = try!(segment.open_read(SegmentComponent::POSTINGS));
        
        let fast_field_data = try!(segment.open_read(SegmentComponent::FASTFIELDS));

        let fast_fields_reader = try!(FastFieldsReader::open(fast_field_data));
        
        let fieldnorms_data = try!(segment.open_read(SegmentComponent::FIELDNORMS));
        let fieldnorms_reader = try!(FastFieldsReader::open(fieldnorms_data));
        
        let positions_data = segment
            .open_read(SegmentComponent::POSITIONS)
            .unwrap_or_else(|_| ReadOnlySource::empty());
        
        let delete_bitset =
            if segment.meta().has_deletes() {
                let delete_data = segment.open_read(SegmentComponent::DELETE)?;
                DeleteBitSet::open(delete_data)
            }
            else {
                DeleteBitSet::empty()
            };
        
        let schema = segment.schema();
        Ok(SegmentReader {
            segment_meta: segment.meta().clone(),
            postings_data: postings_shared_mmap,
            term_infos: Arc::new(term_infos),
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
    pub fn term_infos(&self) -> &TermDictionary<TermInfo> {
        &self.term_infos
    }
       
    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn doc(&self, doc_id: DocId) -> Result<Document> {
        self.store_reader.get(doc_id)
    }


    pub fn postings_data(&self, offset: usize) -> &[u8] {
        &self.postings_data[offset..]
    }

    pub fn get_block_postings(&self) -> BlockSegmentPostings {
        BlockSegmentPostings::from_data(0, &self.postings_data[..], FreqHandler::new_without_freq())
    }

    pub fn read_block_postings_from_terminfo(&self, term_info: &TermInfo, field_type: &FieldType) -> Option<BlockSegmentPostings> {
        let offset = term_info.postings_offset as usize;
        let postings_data = &self.postings_data[offset..];
        let freq_handler = match *field_type {
            FieldType::Str(_) => {
                FreqHandler::new_without_freq()
            }
            _ => {
                FreqHandler::new_without_freq()
            }
        };
        Some(BlockSegmentPostings::from_data(term_info.doc_freq as usize, postings_data, freq_handler))
    }
    
    pub fn read_block_postings(&self, term: &Term, option: SegmentPostingsOption) -> Option<BlockSegmentPostings> {
        let field = term.field();
        let field_entry = self.schema.get_field_entry(field);
        let term_info = get!(self.get_term_info(&term));
        let offset = term_info.postings_offset as usize;
        let postings_data = &self.postings_data[offset..];
        let freq_handler = match *field_entry.field_type() {
            FieldType::Str(ref options) => {
                let indexing_options = options.get_indexing_options();
                match option {
                    SegmentPostingsOption::NoFreq => {
                        FreqHandler::new_without_freq()
                    }
                    SegmentPostingsOption::Freq => {
                        if indexing_options.is_termfreq_enabled() {
                            FreqHandler::new_with_freq()
                        }
                        else {
                            FreqHandler::new_without_freq()
                        }
                    }
                    SegmentPostingsOption::FreqAndPositions => {
                        if indexing_options == TextIndexingOptions::TokenizedWithFreqAndPosition {
                            let offseted_position_data = &self.positions_data[term_info.positions_offset as usize ..];
                            FreqHandler::new_with_freq_and_position(offseted_position_data)
                        }
                        else if indexing_options.is_termfreq_enabled() 
                        {
                            FreqHandler::new_with_freq()
                        }
                        else {
                            FreqHandler::new_without_freq()
                        }
                    }
                }
            }
            _ => {
                FreqHandler::new_without_freq()
            }
        };
        Some(BlockSegmentPostings::from_data(term_info.doc_freq as usize, postings_data, freq_handler))
    }

    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encounterred and indexed. 
    /// 
    /// If the field was not indexed with the indexing options that cover 
    /// the requested options, the returned `SegmentPostings` the method does not fail
    /// and returns a `SegmentPostings` with as much information as possible.
    ///
    /// For instance, requesting `SegmentPostingsOption::FreqAndPositions` for a `TextIndexingOptions`
    /// that does not index position will return a `SegmentPostings` with `DocId`s and frequencies.
    pub fn read_postings(&self, term: &Term, option: SegmentPostingsOption) -> Option<SegmentPostings> {
        self.read_block_postings(term, option)
            .map(|block_postings| {
                 SegmentPostings::from_block_postings(block_postings, self.delete_bitset.clone())
            })
    }
    

    /// Returns the posting list associated with a term.
    ///
    /// If the term is not found, return None.
    /// Even when non-null, because of deletes, the posting object 
    /// returned by this method may contain no documents.
    pub fn read_postings_all_info(&self, term: &Term) -> Option<SegmentPostings> {
        let field_entry = self.schema.get_field_entry(term.field());
        let segment_posting_option = match *field_entry.field_type() {
            FieldType::Str(ref text_options) => {
                match text_options.get_indexing_options() {
                    TextIndexingOptions::TokenizedWithFreq => SegmentPostingsOption::Freq,
                    TextIndexingOptions::TokenizedWithFreqAndPosition => SegmentPostingsOption::FreqAndPositions,
                    _ => SegmentPostingsOption::NoFreq,
                }
            }
            FieldType::U64(_) | FieldType::I64(_) => SegmentPostingsOption::NoFreq
        };
        self.read_postings(term, segment_posting_option)
    }
    
    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> Option<TermInfo> {
        self.term_infos.get(term.as_slice())
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
