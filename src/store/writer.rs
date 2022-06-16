use std::io::{self, Write};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};

use common::{BinarySerializable, VInt};
use ownedbytes::OwnedBytes;

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

    writer: WritePtr,
    // the channel to communicate with the compressor thread.
    compressor_sender: SyncSender<BlockCompressorMessage>,
    // the channel to receive data to write from the compressor thread.
    data_receiver: Receiver<OwnedBytes>,

    // the handle to check for errors on the thread
    compressor_thread_handle: JoinHandle<io::Result<(DocStoreFooter, SkipIndexBuilder)>>,
}

enum BlockCompressorMessage {
    AddBlock(DocumentBlock),
    Stack((StoreReader, DocumentBlock)),
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
        let thread_builder = thread::Builder::new().name("docstore compressor thread".to_string());

        // Data channel to send fs writes, to write only from current thread
        let (data_sender, data_receiver) = sync_channel(3);
        // Channel to send uncompressed data to compressor channel
        let (block_sender, block_receiver) = sync_channel(3);
        let thread_join_handle = thread_builder.spawn(move || {
            let mut block_compressor = BlockCompressor::new(compressor, data_sender);
            while let Ok(packet) = block_receiver.recv() {
                match packet {
                    BlockCompressorMessage::AddBlock(block) => {
                        block_compressor.compress_block(block)?;
                    }
                    BlockCompressorMessage::Stack((store_reader, block)) => {
                        block_compressor.stack(block, store_reader)?;
                    }
                }
            }
            block_compressor.close()
        })?;

        Ok(StoreWriter {
            compressor,
            block_size,
            num_docs_in_current_block: 0,
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
            writer,
            compressor_sender: block_sender,
            compressor_thread_handle: thread_join_handle,
            data_receiver,
        })
    }

    pub(crate) fn compressor(&self) -> Compressor {
        self.compressor
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.intermediary_buffer.capacity() + self.current_block.capacity()
    }

    fn check_flush_block(&mut self) -> io::Result<()> {
        if self.current_block.len() > self.block_size {
            let block = self.get_current_block();
            self.compressor_sender
                .send(BlockCompressorMessage::AddBlock(block))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
            self.fetch_writes_from_channel()?;
        }
        Ok(())
    }

    /// Try to empty the queue to write into the file.
    ///
    /// This is done in order to avoid writing from multiple threads into the file.
    fn fetch_writes_from_channel(&mut self) -> io::Result<()> {
        loop {
            match self.data_receiver.try_recv() {
                Ok(data) => {
                    self.writer.write_all(data.as_slice())?;
                }
                Err(err) => match err {
                    TryRecvError::Empty => {
                        break;
                    }
                    TryRecvError::Disconnected => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "compressor data channel unexpected closed".to_string(),
                        ));
                    }
                },
            }
        }

        Ok(())
    }

    fn get_current_block(&mut self) -> DocumentBlock {
        let block = DocumentBlock {
            data: self.current_block.to_owned(),
            num_docs_in_block: self.num_docs_in_current_block,
        };
        self.current_block.clear();
        self.num_docs_in_current_block = 0;
        block
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
        VInt(doc_num_bytes as u64).serialize(&mut self.current_block)?;
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
        VInt(doc_num_bytes as u64).serialize(&mut self.current_block)?;
        self.current_block.write_all(serialized_document)?;
        self.num_docs_in_current_block += 1;
        self.check_flush_block()?;
        Ok(())
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    pub fn stack(&mut self, store_reader: StoreReader) -> io::Result<()> {
        self.check_flush_block()?;
        let block = self.get_current_block();
        self.compressor_sender
            .send(BlockCompressorMessage::Stack((store_reader, block)))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        Ok(())
    }

    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        let block = self.get_current_block();
        if !block.is_empty() {
            self.compressor_sender
                .send(BlockCompressorMessage::AddBlock(block))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        }
        drop(self.compressor_sender);

        // Wait for remaining data on the channel to write
        while let Ok(data) = self.data_receiver.recv() {
            self.writer.write_all(data.as_slice())?;
        }

        // The compressor thread should have finished already, since data_receiver stopped
        // receiving
        let (docstore_footer, offset_index_writer) = self
            .compressor_thread_handle
            .join()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))??;

        offset_index_writer.serialize_into(&mut self.writer)?;
        docstore_footer.serialize(&mut self.writer)?;
        self.writer.terminate()
    }
}

/// BlockCompressor is seperated from StoreWriter, to be run in an own thread
pub struct BlockCompressor {
    compressor: Compressor,
    doc: DocId,
    offset_index_writer: SkipIndexBuilder,
    intermediary_buffer: Vec<u8>,
    written_bytes: usize,
    data_sender: SyncSender<OwnedBytes>,
}

struct DocumentBlock {
    data: Vec<u8>,
    num_docs_in_block: DocId,
}

impl DocumentBlock {
    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl BlockCompressor {
    fn new(compressor: Compressor, data_sender: SyncSender<OwnedBytes>) -> Self {
        Self {
            compressor,
            doc: 0,
            offset_index_writer: SkipIndexBuilder::new(),
            intermediary_buffer: Vec::new(),
            written_bytes: 0,
            data_sender,
        }
    }

    fn compress_block(&mut self, block: DocumentBlock) -> io::Result<()> {
        assert!(block.num_docs_in_block > 0);
        self.intermediary_buffer.clear();
        self.compressor
            .compress_into(&block.data[..], &mut self.intermediary_buffer)?;

        let byte_range = self.written_bytes..self.written_bytes + self.intermediary_buffer.len();

        self.data_sender
            .send(OwnedBytes::new(self.intermediary_buffer.to_owned()))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        self.written_bytes += byte_range.len();

        self.register_checkpoint(Checkpoint {
            doc_range: self.doc..self.doc + block.num_docs_in_block,
            byte_range,
        });
        Ok(())
    }

    fn register_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.offset_index_writer.insert(checkpoint.clone());
        self.doc = checkpoint.doc_range.end;
    }

    /// Stacks a store reader on top of the documents written so far.
    /// This method is an optimization compared to iterating over the documents
    /// in the store and adding them one by one, as the store's data will
    /// not be decompressed and then recompressed.
    fn stack(&mut self, block: DocumentBlock, store_reader: StoreReader) -> io::Result<()> {
        if !block.is_empty() {
            self.compress_block(block)?;
        }
        let doc_shift = self.doc;
        let start_shift = self.written_bytes;

        // just bulk write all of the block of the given reader.
        self.data_sender
            .send(store_reader.block_data()?)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        self.written_bytes += store_reader.block_data()?.as_slice().len();

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
    fn close(self) -> io::Result<(DocStoreFooter, SkipIndexBuilder)> {
        drop(self.data_sender);

        let header_offset: u64 = self.written_bytes as u64;
        let footer = DocStoreFooter::new(header_offset, Decompressor::from(self.compressor));

        Ok((footer, self.offset_index_writer))
    }
}
