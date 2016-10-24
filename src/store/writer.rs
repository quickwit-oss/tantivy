use directory::WritePtr;
use DocId;
use schema::FieldValue;
use common::BinarySerializable;
use std::io::Write;
use std::io;
use error::Result;
use lz4;
use datastruct::SkipListBuilder;
use super::StoreReader;

const BLOCK_SIZE: usize = 16_384;

pub struct StoreWriter {
    doc: DocId,
    written: u64,
    offset_index_writer: SkipListBuilder<u64>,
    writer: WritePtr,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
}


impl StoreWriter {
    pub fn new(writer: WritePtr) -> StoreWriter {
        StoreWriter {
            doc: 0,
            written: 0,
            offset_index_writer: SkipListBuilder::new(3),
            writer: writer,
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
        }
    }

    pub fn stack_reader(&mut self, reader: &StoreReader) -> Result<()> {
        for doc_id in 0..reader.max_doc {
            let doc = try!(reader.get(doc_id));
            let field_values: Vec<&FieldValue> = doc.field_values()
                .iter()
                .collect();
            try!(self.store(&field_values));
        }
        Ok(())
    }

    pub fn store<'a>(&mut self, field_values: &[&'a FieldValue]) -> io::Result<()> {
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

    fn write_and_compress_block(&mut self) -> io::Result<()> {
        self.intermediary_buffer.clear();
        {
            let mut encoder = try!(lz4::EncoderBuilder::new().build(&mut self.intermediary_buffer));
            try!(encoder.write_all(&self.current_block));
            let (_, encoder_result) = encoder.finish();
            try!(encoder_result);
        }
        let compressed_block_size = self.intermediary_buffer.len() as u64;
        self.written += try!((compressed_block_size as u32).serialize(&mut self.writer)) as u64;
        try!(self.writer.write_all(&self.intermediary_buffer));
        self.written += compressed_block_size;
        try!(self.offset_index_writer.insert(self.doc, &self.written));
        self.current_block.clear();
        Ok(())
    }

    pub fn close(mut self) -> io::Result<()> {
        if !self.current_block.is_empty() {
            try!(self.write_and_compress_block());
        }
        let header_offset: u64 = self.written;
        try!(self.offset_index_writer.write::<Box<Write>>(&mut self.writer));
        try!(header_offset.serialize(&mut self.writer));
        try!(self.doc.serialize(&mut self.writer));
        self.writer.flush()
    }
}
