use std::io::{self, Write};

use common::{BinarySerializable, VInt};

use super::compressors::Compressor;
use super::StoreReader;
use crate::directory::WritePtr;
use crate::schema::Document;
use crate::store::store_compressor::BlockCompressor;
use crate::DocId;

/// Write tantivy's [`Store`](./index.html)
///
/// Contrary to the other components of `tantivy`,
/// the store is written to disc as document as being added,
/// as opposed to when the segment is getting finalized.
///
/// The skip list index on the other hand, is built in memory.
pub struct StoreWriter {
    compressor: Compressor,
    block_size: usize,
    num_docs_in_current_block: DocId,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
    block_compressor: BlockCompressor,
}

impl StoreWriter {
    /// Create a store writer.
    ///
    /// The store writer will writes blocks on disc as
    /// document are added.
    pub fn new(
        writer: WritePtr,
        compressor: Compressor,
        block_size: usize,
        dedicated_thread: bool,
    ) -> io::Result<StoreWriter> {
        let block_compressor = BlockCompressor::new(compressor, writer, dedicated_thread)?;
        Ok(StoreWriter {
            compressor,
            block_size,
            num_docs_in_current_block: 0,
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
            block_compressor,
        })
    }

    pub(crate) fn compressor(&self) -> Compressor {
        self.compressor
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.intermediary_buffer.capacity() + self.current_block.capacity()
    }

    /// Checks if the current block is full, and if so, compresses and flushes it.
    fn check_flush_block(&mut self) -> io::Result<()> {
        if self.current_block.len() > self.block_size {
            self.send_current_block_to_compressor()?;
        }
        Ok(())
    }

    /// Flushes current uncompressed block and sends to compressor.
    fn send_current_block_to_compressor(&mut self) -> io::Result<()> {
        // We don't do anything if the current block is empty to begin with.
        if self.current_block.is_empty() {
            return Ok(());
        }
        self.block_compressor
            .compress_block_and_write(&self.current_block, self.num_docs_in_current_block)?;
        self.current_block.clear();
        self.num_docs_in_current_block = 0;
        Ok(())
    }

    /// Store a new document.
    ///
    /// The document id is implicitly the current number
    /// of documents.
    pub fn store(&mut self, stored_document: &Document) -> io::Result<()> {
        self.intermediary_buffer.clear();
        stored_document.serialize(&mut self.intermediary_buffer)?;
        // calling store bytes would be preferable for code reuse, but then we can't use
        // intermediary_buffer due to the borrow checker
        // a new buffer costs ~1% indexing performance
        let doc_num_bytes = self.intermediary_buffer.len();
        VInt(doc_num_bytes as u64).serialize_into_vec(&mut self.current_block);
        self.current_block
            .write_all(&self.intermediary_buffer[..])?;
        self.num_docs_in_current_block += 1;
        self.check_flush_block()?;
        Ok(())
    }

    /// Store bytes of a serialized document.
    ///
    /// The document id is implicitly the current number
    /// of documents.
    pub fn store_bytes(&mut self, serialized_document: &[u8]) -> io::Result<()> {
        let doc_num_bytes = serialized_document.len();
        VInt(doc_num_bytes as u64).serialize_into_vec(&mut self.current_block);
        self.current_block.extend_from_slice(serialized_document);
        self.num_docs_in_current_block += 1;
        self.check_flush_block()?;
        Ok(())
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    pub fn stack(&mut self, store_reader: StoreReader) -> io::Result<()> {
        // We flush the current block first before stacking
        self.send_current_block_to_compressor()?;
        self.block_compressor.stack_reader(store_reader)?;
        Ok(())
    }

    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        self.send_current_block_to_compressor()?;
        self.block_compressor.close()?;
        Ok(())
    }
}
