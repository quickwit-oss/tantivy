use directory::WritePtr;
use DocId;
use schema::FieldValue;
use common::BinarySerializable;
use std::io::Write;
use std::io::Read;
use std::io;
use lz4;
use super::StoreReader;
use super::OffsetIndex;

const BLOCK_SIZE: usize = 16_384;

pub struct StoreWriter {
    doc: DocId,
    offsets: Vec<OffsetIndex>, // TODO have a better index.
    written: u64,
    writer: WritePtr,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
}

impl BinarySerializable for OffsetIndex {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let OffsetIndex(a, b) = *self;
        Ok(try!(a.serialize(writer)) + try!(b.serialize(writer)))
    }
    fn deserialize(reader: &mut Read) -> io::Result<OffsetIndex> {
        let a = try!(DocId::deserialize(reader));
        let b = try!(u64::deserialize(reader));
        Ok(OffsetIndex(a, b))
    }
}

impl StoreWriter {

    pub fn new(writer: WritePtr) -> StoreWriter {
        StoreWriter {
            doc: 0,
            written: 0,
            offsets: Vec::new(),
            writer: writer,
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
        }
    }

    pub fn stack_reader(&mut self, reader: &StoreReader) -> io::Result<()> {
        if self.current_block.len() > 0 {
            try!(self.write_and_compress_block());
        }
        match reader.offsets.last() {
            Some(&OffsetIndex(ref num_docs, ref body_size)) => {
                try!(self.writer.write_all(&reader.data.as_slice()[0..*body_size as usize]));
                for &OffsetIndex(doc, offset) in &reader.offsets {
                    self.offsets.push(OffsetIndex(self.doc + doc, self.written + offset));
                }
                self.written += *body_size;
                self.doc += *num_docs;
                Ok(())
            },
            None => {
                Err(io::Error::new(io::ErrorKind::Other, "No offset for reader"))
            }
        }
    }

    pub fn store<'a>(&mut self, field_values: &Vec<&'a FieldValue>) -> io::Result<()> {
        self.intermediary_buffer.clear();
        try!((field_values.len() as u32).serialize(&mut self.intermediary_buffer));
        for field_value in field_values {
            try!((*field_value).serialize(&mut self.intermediary_buffer));
        }
        try!((self.intermediary_buffer.len() as u32).serialize(&mut self.current_block));
        try!(self.current_block.write_all(&self.intermediary_buffer[..]));
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            try!(self.write_and_compress_block());
        }
        Ok(())
    }

    fn write_and_compress_block(&mut self,) -> io::Result<()> {
        self.intermediary_buffer.clear();
        {
            let mut encoder = lz4::EncoderBuilder::new()
                    .build(&mut self.intermediary_buffer)
                    .unwrap();
            try!(encoder.write_all(&self.current_block));
            let (_, encoder_result) = encoder.finish();
            try!(encoder_result);
        }
        let compressed_block_size = self.intermediary_buffer.len() as u64;
        self.written += try!((compressed_block_size as u32).serialize(&mut self.writer)) as u64;
        try!(self.writer.write_all(&self.intermediary_buffer));
        self.written += compressed_block_size;
        self.offsets.push(OffsetIndex(self.doc, self.written));
        self.current_block.clear();
        Ok(())
    }

    pub fn close(&mut self,) -> io::Result<()> {
        if self.current_block.len() > 0 {
            try!(self.write_and_compress_block());
        }
        let header_offset: u64 = self.written;
        try!(self.offsets.serialize(&mut self.writer));
        try!(header_offset.serialize(&mut self.writer));
        self.writer.flush()
    }

}
