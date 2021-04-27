use super::compress;
use super::index::SkipIndexBuilder;
use super::StoreReader;
use crate::common::CountingWriter;
use crate::common::{BinarySerializable, VInt};
use crate::directory::TerminatingWrite;
use crate::directory::WritePtr;
use crate::schema::Document;
use crate::store::index::Checkpoint;
use crate::DocId;
use std::io::{self, Write};

const BLOCK_SIZE: usize = 16_384;

/// Write tantivy's [`Store`](./index.html)
///
/// Contrary to the other components of `tantivy`,
/// the store is written to disc as document as being added,
/// as opposed to when the segment is getting finalized.
///
/// The skip list index on the other hand, is built in memory.
///
pub struct StoreWriter {
    doc: DocId,
    first_doc_in_block: DocId,
    offset_index_writer: SkipIndexBuilder,
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
            first_doc_in_block: 0,
            offset_index_writer: SkipIndexBuilder::new(),
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
        self.current_block
            .write_all(&self.intermediary_buffer[..])?;
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
        }
        assert_eq!(self.first_doc_in_block, self.doc);
        let doc_shift = self.doc;
        let start_shift = self.writer.written_bytes() as usize;

        // just bulk write all of the block of the given reader.
        self.writer
            .write_all(store_reader.block_data()?.as_slice())?;

        // concatenate the index of the `store_reader`, after translating
        // its start doc id and its start file offset.
        for mut checkpoint in store_reader.block_checkpoints() {
            checkpoint.doc_range.start += doc_shift;
            checkpoint.doc_range.end += doc_shift;
            checkpoint.byte_range.start += start_shift;
            checkpoint.byte_range.end += start_shift;
            self.register_checkpoint(checkpoint);
        }
        Ok(())
    }

    fn register_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.offset_index_writer.insert(checkpoint.clone());
        self.first_doc_in_block = checkpoint.doc_range.end;
        self.doc = checkpoint.doc_range.end;
    }

    fn write_and_compress_block(&mut self) -> io::Result<()> {
        assert!(self.doc > 0);
        self.intermediary_buffer.clear();
        compress(&self.current_block[..], &mut self.intermediary_buffer)?;
        let start_offset = self.writer.written_bytes() as usize;
        self.writer.write_all(&self.intermediary_buffer)?;
        let end_offset = self.writer.written_bytes() as usize;
        let end_doc = self.doc;
        self.register_checkpoint(Checkpoint {
            doc_range: self.first_doc_in_block..end_doc,
            byte_range: start_offset..end_offset,
        });
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
        self.writer.terminate()
    }
}
