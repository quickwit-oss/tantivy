use core::index::{Segment, SegmentId};
use schema::Term;
use store::StoreReader;
use schema::Document;
use directory::ReadOnlySource;
use DocId;
use core::index::SegmentComponent;
use std::io;
use std::str;
use postings::TermInfo;
use datastruct::FstMap;
use std::fmt;
use rustc_serialize::json;
use core::index::SegmentInfo;
use schema::Field;
use core::convert_to_ioerror;
use postings::SegmentPostings;
use postings::Postings;
use fastfield::{U32FastFieldsReader, U32FastFieldReader};
use schema::FieldEntry;
use schema::Schema;
use postings::FreqHandler;

pub struct SegmentReader {
    segment_info: SegmentInfo,
    segment_id: SegmentId,
    term_infos: FstMap<TermInfo>,
    postings_data: ReadOnlySource,
    store_reader: StoreReader,
    fast_fields_reader: U32FastFieldsReader,
    schema: Schema,
}

impl SegmentReader {


    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    /// Today, `tantivy` does not handle deletes so, it happens
    /// to also be the number of documents in the index.
    pub fn max_doc(&self) -> DocId {
        self.segment_info.max_doc
    }

    pub fn get_fast_field_reader(&self, field: Field) -> io::Result<U32FastFieldReader> {
        let field_entry = self.schema.get_field_entry(field);
        match *field_entry {
            FieldEntry::Text(_, _) => {
                Err(io::Error::new(io::ErrorKind::Other, "fast field are not yet supported for text fields."))
            },
            FieldEntry::U32(_, _) => {
                // TODO check that the schema allows that
                //Err(io::Error::new(io::ErrorKind::Other, "fast field are not yet supported for text fields."))
                self.fast_fields_reader.get_field(field)
            },
        }
        
    }

    pub fn get_store_reader(&self) -> &StoreReader {
        &self.store_reader
    }

    /// Open a new segment for reading.
    pub fn open(segment: Segment) -> io::Result<SegmentReader> {
        let segment_info_reader = try!(segment.open_read(SegmentComponent::INFO));
        let segment_info_data = try!(str::from_utf8(&*segment_info_reader)
                                         .map_err(convert_to_ioerror));
        let segment_info: SegmentInfo = try!(json::decode(&segment_info_data)
                                                 .map_err(convert_to_ioerror));
        let source = try!(segment.open_read(SegmentComponent::TERMS));
        let term_infos = try!(FstMap::from_source(source));
        let store_reader = StoreReader::new(try!(segment.open_read(SegmentComponent::STORE)));
        let postings_shared_mmap = try!(segment.open_read(SegmentComponent::POSTINGS));
        let fast_field_data = try!(segment.open_read(SegmentComponent::FASTFIELDS));
        let fast_fields_reader = try!(U32FastFieldsReader::open(fast_field_data));
        
        let schema = segment.schema();
        Ok(SegmentReader {
            segment_info: segment_info,
            postings_data: postings_shared_mmap,
            term_infos: term_infos,
            segment_id: segment.id(),
            store_reader: store_reader,
            fast_fields_reader: fast_fields_reader,
            schema: schema,
        })
    }

    pub fn term_infos(&self) -> &FstMap<TermInfo> {
        &self.term_infos
    }

    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn doc(&self, doc_id: &DocId) -> io::Result<Document> {
        self.store_reader.get(doc_id)
    }

    pub fn read_postings(&self, term: &Term) -> Option<SegmentPostings> {
        let field = term.get_field();
        let field_entry = self.schema.get_field_entry(field);
        let term_info = get!(self.get_term_info(&term));
        let offset = term_info.postings_offset as usize;
        let postings_data = &self.postings_data[offset..];
        let freq_handler = match field_entry {
            &FieldEntry::Text(_, ref options) => {
                if options.get_indexing_options().is_termfreq_enabled() {
                    FreqHandler::new_freq_reader()
                }
                else {
                    FreqHandler::NoFreq
                }
            }
            _ => {
                panic!("Expected text field, got {:?}", field_entry);
            }
        };
        Some(SegmentPostings::from_data(term_info.doc_freq, &postings_data, freq_handler))
    }
    
    pub fn get_term_info<'a>(&'a self, term: &Term) -> Option<TermInfo> {
        self.term_infos.get(term.as_slice())
    }
}


impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}
