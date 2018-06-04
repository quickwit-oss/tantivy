use super::StoreReader;
use common::CountingWriter;
use common::{BinarySerializable, VInt};
use super::skiplist::SkipListBuilder;
use directory::WritePtr;
use lz4;
use schema::Document;
use std::io::{self, Write};
use DocId;

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
            offset_index_writer: SkipListBuilder::new(4),
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
    pub fn store(&mut self, stored_document: &Document) -> io::Result<()> {
        self.intermediary_buffer.clear();
        stored_document.serialize(&mut self.intermediary_buffer)?;
        let doc_num_bytes = self.intermediary_buffer.len();
        VInt(doc_num_bytes as u64).serialize(&mut self.current_block)?;
        self.current_block.write_all(&self.intermediary_buffer[..])?;
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            self.write_and_compress_block()?;
        }
        Ok(())
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    pub fn stack(&mut self, store_reader: &StoreReader) -> io::Result<()> {
        if !self.current_block.is_empty() {
            self.write_and_compress_block()?;
            self.offset_index_writer
                .insert(u64::from(self.doc), &(self.writer.written_bytes() as u64))?;
        }
        let doc_offset = self.doc;
        let start_offset = self.writer.written_bytes() as u64;

        // just bulk write all of the block of the given reader.
        self.writer.write_all(store_reader.block_data())?;

        // concatenate the index of the `store_reader`, after translating
        // its start doc id and its start file offset.
        for (next_doc_id, block_addr) in store_reader.block_index() {
            self.doc = doc_offset + next_doc_id as u32;
            self.offset_index_writer
                .insert(u64::from(self.doc), &(start_offset + block_addr))?;
        }
        Ok(())
    }

    fn write_and_compress_block(&mut self) -> io::Result<()> {
        self.intermediary_buffer.clear();
        {
            let mut encoder = lz4::EncoderBuilder::new().build(&mut self.intermediary_buffer)?;
            encoder.write_all(&self.current_block)?;
            let (_, encoder_result) = encoder.finish();
            encoder_result?;
        }
        (self.intermediary_buffer.len() as u32).serialize(&mut self.writer)?;
        self.writer.write_all(&self.intermediary_buffer)?;
        self.offset_index_writer
            .insert(u64::from(self.doc), &(self.writer.written_bytes() as u64))?;
        self.current_block.clear();
        Ok(())
    }

    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        if !self.current_block.is_empty() {
            self.write_and_compress_block()?;
        }
        let header_offset: u64 = self.writer.written_bytes() as u64;
        self.offset_index_writer.write(&mut self.writer)?;
        header_offset.serialize(&mut self.writer)?;
        self.doc.serialize(&mut self.writer)?;
        self.writer.flush()
    }
}
