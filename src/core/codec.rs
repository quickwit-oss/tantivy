use std::io;
use std::io::{Read, Write};
use core::serial::{SegmentSerializer, SerializableSegment};
use rustc_serialize::json;
use core::directory::WritePtr;
use core::index::Segment;
use core::index::SegmentInfo;
use core::index::SegmentComponent;
use core::schema::Term;
use core::schema::DocId;
use core::fstmap::FstMapBuilder;
use core::store::StoreWriter;
use core::serialize::BinarySerializable;
use core::simdcompression;
use core::schema::FieldValue;


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
    postings_write: WritePtr,
    store_writer: StoreWriter,
    term_fst_builder: FstMapBuilder<WritePtr, TermInfo>, // TODO find an alternative to work around the "move"
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

    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()> {
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.term_fst_builder.insert(term.as_slice(), &term_info);
        // writing the size of the posting list
        Ok(())
    }

    fn write_docs(&mut self, doc_ids: &[DocId]) -> io::Result<()> {
        // TODO write_all transmuted [u8]
        let docs_data = self.encoder.encode_sorted(doc_ids);
        self.written_bytes_postings += try!((docs_data.len() as u32).serialize(&mut self.postings_write));
        for num in docs_data {
            self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
        }
        Ok(())
    }

    fn write_segment_info(&self, segment_info: &SegmentInfo) -> io::Result<()> {
        let mut write = try!(self.segment.open_write(SegmentComponent::INFO));
        let json_data = json::encode(segment_info).unwrap();
        write.write_all(json_data.as_bytes());
        write.flush();
        Ok(())
    }

    fn close(mut self,) -> io::Result<()> {
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
    pub fn serializer(segment: &Segment) -> io::Result<SimpleSegmentSerializer>  {
        let term_write = try!(segment.open_write(SegmentComponent::TERMS));
        let postings_write = try!(segment.open_write(SegmentComponent::POSTINGS));
        let store_write = try!(segment.open_write(SegmentComponent::STORE));
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


    pub fn write<I: SerializableSegment>(index: &I, segment: &Segment) -> io::Result<()> {
        let serializer = try!(SimpleCodec::serializer(segment));
        index.write(serializer)
    }
}
