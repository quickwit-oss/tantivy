use directory::WritePtr;
use DocId;
use schema::FieldValue;
use common::BinarySerializable;
use std::io::{self, Write};
use lz4;
use datastruct::SkipListBuilder;
use common::CountingWriter;

const BLOCK_SIZE: usize = 16_384;


/// Write tantivy's [`Store`](./index.html)
///
/// Contrary to the other components of `tantivy`,
/// the store is written to disc as document as being added,
/// as opposed to when the segment is getting finalized.
///
/// The skip list index on the other hand, is build in memory.
///
pub struct StoreWriter {
    doc: DocId,
    offset_index_writer: SkipListBuilder<u64>,
    writer: CountingWriter<WritePtr>,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
}


impl StoreWriter {
    /// Create a store writer.
    ///
    /// The store writer will writes blocks on disc as
    /// document are added.
    pub fn new(writer: WritePtr) -> StoreWriter {
        StoreWriter {
            doc: 0,
            offset_index_writer: SkipListBuilder::new(3),
            writer: CountingWriter::wrap(writer),
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
        }
    }

    /// Store a new document.
    ///
    /// The document id is implicitely the number of times
    /// this method has been called.
    ///
    pub fn store<'a>(&mut self, field_values: &[&'a FieldValue]) -> io::Result<()> {
        self.intermediary_buffer.clear();
        try!((field_values.len() as u32).serialize(
            &mut self.intermediary_buffer,
        ));
        for field_value in field_values {
            try!((*field_value).serialize(&mut self.intermediary_buffer));
        }
        (self.intermediary_buffer.len() as u32).serialize(
            &mut self.current_block,
        )?;
        self.current_block.write_all(&self.intermediary_buffer[..])?;
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            self.write_and_compress_block()?;
        }
        Ok(())
    }

    fn write_and_compress_block(&mut self) -> io::Result<()> {
        self.intermediary_buffer.clear();
        {
            let mut encoder = try!(lz4::EncoderBuilder::new().build(
                &mut self.intermediary_buffer,
            ));
            try!(encoder.write_all(&self.current_block));
            let (_, encoder_result) = encoder.finish();
            try!(encoder_result);
        }
        (self.intermediary_buffer.len() as u32).serialize(
            &mut self.writer,
        )?;
        self.writer.write_all(&self.intermediary_buffer)?;
        self.offset_index_writer.insert(
            self.doc,
            &(self.writer.written_bytes() as
                u64),
        )?;
        self.current_block.clear();
        Ok(())
    }


    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        if !self.current_block.is_empty() {
            try!(self.write_and_compress_block());
        }
        let header_offset: u64 = self.writer.written_bytes() as u64;
        try!(self.offset_index_writer.write(&mut self.writer));
        try!(header_offset.serialize(&mut self.writer));
        try!(self.doc.serialize(&mut self.writer));
        self.writer.flush()
    }
}
