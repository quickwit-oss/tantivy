use std::io::{self, Write};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

use common::{BinarySerializable, CountingWriter, VInt};

use super::compressors::Compressor;
use super::footer::DocStoreFooter;
use super::index::SkipIndexBuilder;
use super::{Decompressor, StoreReader};
use crate::directory::{TerminatingWrite, WritePtr};
use crate::schema::Document;
use crate::store::index::Checkpoint;
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

    // the channel to send data to the compressor thread.
    compressor_sender: SyncSender<BlockCompressorMessage>,
    // the handle to check for errors on the thread
    compressor_thread_handle: JoinHandle<io::Result<()>>,
}

enum BlockCompressorMessage {
    AddBlock(DocumentBlock),
    Stack(StoreReader),
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
    ) -> io::Result<StoreWriter> {
        let thread_builder = thread::Builder::new().name("docstore-compressor-thread".to_string());

        // Channel to send uncompressed data to compressor channel
        let (block_sender, block_receiver): (
            SyncSender<BlockCompressorMessage>,
            Receiver<BlockCompressorMessage>,
        ) = sync_channel(3);
        let thread_join_handle = thread_builder.spawn(move || {
            let mut block_compressor = BlockCompressor::new(compressor, writer);
            while let Ok(packet) = block_receiver.recv() {
                match packet {
                    BlockCompressorMessage::AddBlock(block) => {
                        block_compressor.compress_block_and_write(block)?;
                    }
                    BlockCompressorMessage::Stack(store_reader) => {
                        block_compressor.stack(store_reader)?;
                    }
                }
            }
            block_compressor.close()?;
            Ok(())
        })?;

        Ok(StoreWriter {
            compressor,
            block_size,
            num_docs_in_current_block: 0,
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
            compressor_sender: block_sender,
            compressor_thread_handle: thread_join_handle,
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
        let block = DocumentBlock {
            data: self.current_block.to_owned(),
            num_docs_in_block: self.num_docs_in_current_block,
        };
        self.current_block.clear();
        self.num_docs_in_current_block = 0;
        self.compressor_sender
            .send(BlockCompressorMessage::AddBlock(block))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        Ok(())
    }

    /// Store a new document.
    ///
    /// The document id is implicitely the current number
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
    /// The document id is implicitely the current number
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
        self.compressor_sender
            .send(BlockCompressorMessage::Stack(store_reader))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        Ok(())
    }

    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        self.send_current_block_to_compressor()?;
        drop(self.compressor_sender);

        self.compressor_thread_handle
            .join()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))??;

        Ok(())
    }
}

/// BlockCompressor is separated from StoreWriter, to be run in an own thread
pub struct BlockCompressor {
    compressor: Compressor,
    first_doc_in_block: DocId,
    offset_index_writer: SkipIndexBuilder,
    intermediary_buffer: Vec<u8>,
    writer: CountingWriter<WritePtr>,
}

struct DocumentBlock {
    data: Vec<u8>,
    num_docs_in_block: DocId,
}

impl BlockCompressor {
    fn new(compressor: Compressor, writer: WritePtr) -> Self {
        Self {
            compressor,
            first_doc_in_block: 0,
            offset_index_writer: SkipIndexBuilder::new(),
            intermediary_buffer: Vec::new(),
            writer: CountingWriter::wrap(writer),
        }
    }

    fn compress_block_and_write(&mut self, block: DocumentBlock) -> io::Result<()> {
        assert!(block.num_docs_in_block > 0);
        self.intermediary_buffer.clear();
        self.compressor
            .compress_into(&block.data[..], &mut self.intermediary_buffer)?;

        let start_offset = self.writer.written_bytes() as usize;
        self.writer.write_all(&self.intermediary_buffer)?;
        let end_offset = self.writer.written_bytes() as usize;

        self.register_checkpoint(Checkpoint {
            doc_range: self.first_doc_in_block..self.first_doc_in_block + block.num_docs_in_block,
            byte_range: start_offset..end_offset,
        });
        Ok(())
    }

    fn register_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.offset_index_writer.insert(checkpoint.clone());
        self.first_doc_in_block = checkpoint.doc_range.end;
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    fn stack(&mut self, store_reader: StoreReader) -> io::Result<()> {
        let doc_shift = self.first_doc_in_block;
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
    fn close(mut self) -> io::Result<()> {
        let header_offset: u64 = self.writer.written_bytes() as u64;
        let docstore_footer =
            DocStoreFooter::new(header_offset, Decompressor::from(self.compressor));

        self.offset_index_writer.serialize_into(&mut self.writer)?;
        docstore_footer.serialize(&mut self.writer)?;
        self.writer.terminate()
    }
}
