use core::serial::*;
use fst::MapBuilder;
use std::io;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use byteorder::{BigEndian,  WriteBytesExt};
use core::index::Segment;
use core::index::SegmentComponent;
use core::schema::Term;
use core::schema::DocId;
use core::fstmap::{FstMapBuilder, FstMap};
use core::store::StoreWriter;
use std::fs::File;
use core::serialize::BinarySerializable;
use fst;
use core::simdcompression;
use std::convert::From;
use core::schema::FieldValue;
use std::io::{Read, Write};

#[derive(Debug)]
pub struct TermInfo {
    pub doc_freq: u32,
    pub postings_offset: u32,
}

impl BinarySerializable for TermInfo {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        self.doc_freq.serialize(writer);
        self.postings_offset.serialize(writer)
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let doc_freq = try!(u32::deserialize(reader));
        let offset = try!(u32::deserialize(reader));
        Ok(TermInfo {
            doc_freq: doc_freq,
            postings_offset: offset,
        })
    }
}

pub struct SimpleCodec;

pub struct SimpleSegmentSerializer {
    segment: Segment,
    written_bytes_postings: usize,
    postings_write: File,
    store_writer: StoreWriter,
    term_fst_builder: FstMapBuilder<File, TermInfo>, // TODO find an alternative to work around the "move"
    encoder: simdcompression::Encoder,
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
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.term_fst_builder.insert(term.as_slice(), &term_info);
        // writing the size of the posting list
        Ok(())
    }

    fn write_docs(&mut self, doc_ids: &[DocId]) -> Result<(), IOError> {
        // TODO write_all transmuted [u8]
        let docs_data = self.encoder.encode_sorted(doc_ids);
        self.written_bytes_postings += try!((docs_data.len() as u32).serialize(&mut self.postings_write));
        for num in docs_data {
            self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
        }
        Ok(())
    }

    fn close(mut self,) -> Result<(), IOError> {
        // TODO handle errors on close
        try!(self.term_fst_builder
                 .finish());
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
        let term_fst_builder_result = FstMapBuilder::new(term_write);
        let term_fst_builder = term_fst_builder_result.unwrap();
        Ok(SimpleSegmentSerializer {
            segment: segment.clone(),
            written_bytes_postings: 0,
            postings_write: postings_write,
            store_writer: StoreWriter::new(store_write),
            term_fst_builder: term_fst_builder,
            encoder: simdcompression::Encoder::new(),
        })
    }


    pub fn write<I: SerializableSegment>(index: &I, segment: &Segment) -> Result<(), IOError> {
        let mut serializer = try!(SimpleCodec::serializer(segment));
        index.write(serializer)
    }
}
