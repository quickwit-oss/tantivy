use Result;
use core::Segment;
use core::SegmentId;
use core::SegmentComponent;
use schema::Term;
use common::HasLen;
use fastfield::delete::DeleteBitSet;
use store::StoreReader;
use schema::Document;
use directory::ReadOnlySource;
use DocId;
use std::str;
use postings::TermInfo;
use datastruct::FstMap;
use std::sync::Arc;
use std::fmt;
use rustc_serialize::json;
use core::SegmentInfo;
use schema::Field;
use postings::SegmentPostingsOption;
use postings::SegmentPostings;
use fastfield::{U32FastFieldsReader, U32FastFieldReader};
use schema::Schema;
use schema::FieldType;
use postings::FreqHandler;
use schema::TextIndexingOptions;
use error::Error;


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
    segment_info: SegmentInfo,
    segment_id: SegmentId,
    term_infos: Arc<FstMap<TermInfo>>,
    postings_data: ReadOnlySource,
    store_reader: StoreReader,
    fast_fields_reader: Arc<U32FastFieldsReader>,
    fieldnorms_reader: Arc<U32FastFieldsReader>,
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
        self.segment_info.max_doc
    }
    
    /// Returns the number of documents.
    /// Deleted documents are not counted.
    ///
    /// Today, `tantivy` does not handle deletes so max doc and
    /// num_docs are the same.
    pub fn num_docs(&self) -> DocId {
        self.segment_info.max_doc - self.num_deleted_docs()
    }
    
    /// Return the number of documents that have been
    /// deleted in the segment.
    pub fn num_deleted_docs(&self) -> DocId {
        self.delete_bitset.len() as DocId
    }

    /// Accessor to a segment's fast field reader given a field.
    pub fn get_fast_field_reader(&self, field: Field) -> Option<U32FastFieldReader> {
        /// Returns the u32 fast value reader if the field
        /// is a u32 field indexed as "fast".
        ///
        /// Return None if the field is not a u32 field
        /// indexed with the fast option.
        ///
        /// # Panics
        /// May panic if the index is corrupted.
        let field_entry = self.schema.get_field_entry(field);
        match field_entry.field_type() {
            &FieldType::Str(_) => {
                warn!("Field <{}> is not a fast field. It is a text field, and fast text fields are not supported yet.", field_entry.name());
                None
            },
            &FieldType::U32(ref u32_options) => {
                if u32_options.is_fast() {
                    self.fast_fields_reader.get_field(field)
                }
                else {
                    warn!("Field <{}> is not defined as a fast field.", field_entry.name());
                    None
                }
            },
        }
    }
    
    /// Accessor to the segment's `Field norms`'s reader.
    ///
    /// Field norms are the length (in tokens) of the fields.
    /// It is used in the computation of the [TfIdf](https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html).
    ///
    /// They are simply stored as a fast field, serialized in 
    /// the `.fieldnorm` file of the segment. 
    pub fn get_fieldnorms_reader(&self, field: Field) -> Option<U32FastFieldReader> {
        self.fieldnorms_reader.get_field(field) 
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
        let segment_info_reader = try!(segment.open_read(SegmentComponent::INFO));
        let segment_info_data = try!(
            str::from_utf8(&*segment_info_reader)
                .map_err(|err| {
                    let segment_info_filepath = segment.relative_path(SegmentComponent::INFO);
                    Error::CorruptedFile(segment_info_filepath, Box::new(err))
                })
         );
        let segment_info: SegmentInfo = try!(
            json::decode(&segment_info_data)
            .map_err(|err| {
                let file_path = segment.relative_path(SegmentComponent::INFO);
                Error::CorruptedFile(file_path, Box::new(err))
            })
        );
        let source = try!(segment.open_read(SegmentComponent::TERMS));
        let term_infos = try!(FstMap::from_source(source));
        let store_reader = StoreReader::from(try!(segment.open_read(SegmentComponent::STORE)));
        let postings_shared_mmap = try!(segment.open_read(SegmentComponent::POSTINGS));
        
        let fast_field_data = try!(segment.open_read(SegmentComponent::FASTFIELDS));
        let fast_fields_reader = try!(U32FastFieldsReader::open(fast_field_data));
        
        let fieldnorms_data = try!(segment.open_read(SegmentComponent::FIELDNORMS));
        let fieldnorms_reader = try!(U32FastFieldsReader::open(fieldnorms_data));
        
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
            segment_info: segment_info,
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
    pub fn term_infos(&self) -> &FstMap<TermInfo> {
        &self.term_infos
    }
       
    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn doc(&self, doc_id: DocId) -> Result<Document> {
        self.store_reader.get(doc_id)
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
        Some(SegmentPostings::from_data(term_info.doc_freq, postings_data, &self.delete_bitset, freq_handler))
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
            FieldType::U32(_) => SegmentPostingsOption::NoFreq
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
