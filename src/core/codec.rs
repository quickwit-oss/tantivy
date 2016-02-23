use core::serial::*;
use std::io::Write;
use fst::MapBuilder;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use byteorder::{BigEndian,  WriteBytesExt};
use core::directory::Segment;
use core::directory::SegmentComponent;
use core::schema::Term;
use core::schema::DocId;
use core::store::StoreWriter;
use std::fs::File;
use fst;
use core::simdcompression;
use std::convert::From;
use core::schema::FieldValue;

pub struct SimpleCodec;

pub struct SimpleSegmentSerializer {
    segment: Segment,
    written_bytes_postings: usize,
    postings_write: File,
    store_writer: StoreWriter,
    term_fst_builder: MapBuilder<File>, // TODO find an alternative to work around the "move"
    cur_term_num_docs: DocId,
    encoder: simdcompression::Encoder,
}


fn convert_fst_error(e: fst::Error) -> IOError {
    IOError::new(IOErrorKind::Other, e)
}

impl SimpleSegmentSerializer {
    pub fn segment(&self,) -> Segment {
        self.segment.clone()
    }
}

impl SegmentSerializer<()> for SimpleSegmentSerializer {

    fn store_doc(&mut self, field_values_it: &mut Iterator<Item=&FieldValue>) {
        let field_values: Vec<&FieldValue> = field_values_it.collect();
        self.store_writer.store(&field_values);
    }

    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<(), IOError> {
        self.term_fst_builder.insert(term.as_slice(), self.written_bytes_postings as u64);
        self.cur_term_num_docs = doc_freq;
        // writing the size of the posting list
        try!(self.postings_write.write_u32::<BigEndian>(doc_freq));
        self.written_bytes_postings +=  4;
        Ok(())
    }

    fn write_docs(&mut self, doc_ids: &[DocId]) -> Result<(), IOError> {
        // TODO write_all transmuted [u8]
        let docs_data = self.encoder.encode(doc_ids);
        try!(self.postings_write.write_u32::<BigEndian>(docs_data.len() as u32));
        self.written_bytes_postings += 4;
        for num in docs_data {
            try!(self.postings_write.write_u32::<BigEndian>(num.clone() as u32));
        }
        Ok(())
    }

    fn close(mut self,) -> Result<(), IOError> {
        // TODO handle errors on close
        try!(self.term_fst_builder
                 .finish()
                 .map_err(convert_fst_error));
        self.store_writer.close()
    }
}

impl SimpleCodec {
    // TODO impl packed int
    // TODO skip lists
    // TODO make that part of the codec API
    pub fn serializer(segment: &Segment) -> Result<SimpleSegmentSerializer, IOError>  {
        let term_write = try!(segment.open_writable(SegmentComponent::TERMS));
        let postings_write = try!(segment.open_writable(SegmentComponent::POSTINGS));
        let store_write = try!(segment.open_writable(SegmentComponent::STORE));
        let term_fst_builder_result = MapBuilder::new(term_write);
        let term_fst_builder = term_fst_builder_result.unwrap();
        Ok(SimpleSegmentSerializer {
            segment: segment.clone(),
            written_bytes_postings: 0,
            postings_write: postings_write,
            store_writer: StoreWriter::new(store_write),
            term_fst_builder: term_fst_builder,
            cur_term_num_docs: 0,
            encoder: simdcompression::Encoder::new(),
        })
    }


    pub fn write<I: SerializableSegment>(index: &I, segment: &Segment) -> Result<(), IOError> {
        let mut serializer = try!(SimpleCodec::serializer(segment));
        index.write(&mut serializer)
    }
}
