use std::io;
use std::io::{Read, Write};
use rustc_serialize::json;
use core::directory::WritePtr;
use core::index::Segment;
use core::index::SegmentInfo;
use core::index::SegmentComponent;
use core::schema::Term;
use core::schema::DocId;
use core::fstmap::FstMapBuilder;
use core::fastfield::FastFieldSerializer;
use core::store::StoreWriter;
use core::serialize::BinarySerializable;
use core::simdcompression;
use core::schema::TextFieldValue;
use core::convert_to_ioerror;


#[derive(Debug)]
pub struct TermInfo {
    pub doc_freq: u32,
    pub postings_offset: u32,
}


impl BinarySerializable for TermInfo {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        Ok(
            try!(self.doc_freq.serialize(writer)) +
            try!(self.postings_offset.serialize(writer))
        )
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


pub struct SegmentSerializer {
    segment: Segment,
    written_bytes_postings: usize,
    postings_write: WritePtr,
    store_writer: StoreWriter,
    fast_field_serializer: FastFieldSerializer,
    term_fst_builder: FstMapBuilder<WritePtr, TermInfo>, // TODO find an alternative to work around the "move"
    encoder: simdcompression::Encoder,
}



impl SegmentSerializer {

    pub fn for_segment(segment: &Segment) -> io::Result<SegmentSerializer>  {
        let term_write = try!(segment.open_write(SegmentComponent::TERMS));
        let postings_write = try!(segment.open_write(SegmentComponent::POSTINGS));
        let store_write = try!(segment.open_write(SegmentComponent::STORE));
        let fast_field_write = try!(segment.open_write(SegmentComponent::FASTFIELDS));
        let term_fst_builder_result = FstMapBuilder::new(term_write);
        let term_fst_builder = term_fst_builder_result.unwrap();
        let fast_field_serializer = try!(FastFieldSerializer::new(fast_field_write));
        Ok(SegmentSerializer {
            segment: segment.clone(),
            written_bytes_postings: 0,
            postings_write: postings_write,
            store_writer: StoreWriter::new(store_write),
            fast_field_serializer: fast_field_serializer,
            term_fst_builder: term_fst_builder,
            encoder: simdcompression::Encoder::new(),
        })
    }

    pub fn get_fast_field_serializer(&mut self,) -> &mut FastFieldSerializer {
        &mut self.fast_field_serializer
    }

    pub fn segment(&self,) -> Segment {
        self.segment.clone()
    }

    pub fn store_doc(&mut self, field_values_it: &mut Iterator<Item=&TextFieldValue>) -> io::Result<()> {
        let field_values: Vec<&TextFieldValue> = field_values_it.collect();
        try!(self.store_writer.store(&field_values));
        Ok(())
    }

    pub fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()> {
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.term_fst_builder
            .insert(term.as_slice(), &term_info)
    }

    pub fn write_docs(&mut self, doc_ids: &[DocId]) -> io::Result<()> {
        let docs_data = self.encoder.encode_sorted(doc_ids);
        self.written_bytes_postings += try!((docs_data.len() as u32).serialize(&mut self.postings_write));
        for num in docs_data {
            self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
        }
        Ok(())
    }

    pub fn write_segment_info(&mut self, segment_info: &SegmentInfo) -> io::Result<()> {
        let mut write = try!(self.segment.open_write(SegmentComponent::INFO));
        let json_data = try!(json::encode(segment_info).map_err(convert_to_ioerror));
        try!(write.write_all(json_data.as_bytes()));
        try!(write.flush());
        Ok(())
    }

    pub fn close(mut self,) -> io::Result<()> {
        try!(self.term_fst_builder.finish());
        self.store_writer.close()
    }
}
