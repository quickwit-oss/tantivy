use super::decompress;
use super::skiplist::SkipList;
use crate::common::VInt;
use crate::common::{BinarySerializable, HasLen};
use crate::directory::{FileSlice, OwnedBytes};
use crate::schema::Document;
use crate::space_usage::StoreSpaceUsage;
use crate::DocId;
use std::cell::RefCell;
use std::io;
use std::mem::size_of;

/// Reads document off tantivy's [`Store`](./index.html)
#[derive(Clone)]
pub struct StoreReader {
    data: FileSlice,
    offset_index_file: OwnedBytes,
    current_block_offset: RefCell<usize>,
    current_block: RefCell<Vec<u8>>,
    max_doc: DocId,
}

impl StoreReader {
    /// Opens a store reader
    // TODO rename open
    pub fn open(store_file: FileSlice) -> io::Result<StoreReader> {
        let (data_file, offset_index_file, max_doc) = split_file(store_file)?;
        Ok(StoreReader {
            data: data_file,
            offset_index_file: offset_index_file.read_bytes()?,
            current_block_offset: RefCell::new(usize::max_value()),
            current_block: RefCell::new(Vec::new()),
            max_doc,
        })
    }

    pub(crate) fn block_index(&self) -> SkipList<'_, u64> {
        SkipList::from(self.offset_index_file.as_slice())
    }

    fn block_offset(&self, doc_id: DocId) -> (DocId, u64) {
        self.block_index()
            .seek(u64::from(doc_id) + 1)
            .map(|(doc, offset)| (doc as DocId, offset))
            .unwrap_or((0u32, 0u64))
    }

    pub(crate) fn block_data(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes()
    }

    fn compressed_block(&self, addr: usize) -> io::Result<OwnedBytes> {
        let (block_len_bytes, block_body) = self.data.slice_from(addr).split(4);
        let block_len = u32::deserialize(&mut block_len_bytes.read_bytes()?)?;
        block_body.slice_to(block_len as usize).read_bytes()
    }

    fn read_block(&self, block_offset: usize) -> io::Result<()> {
        if block_offset != *self.current_block_offset.borrow() {
            let mut current_block_mut = self.current_block.borrow_mut();
            current_block_mut.clear();
            let compressed_block = self.compressed_block(block_offset)?;
            decompress(compressed_block.as_slice(), &mut current_block_mut)?;
            *self.current_block_offset.borrow_mut() = block_offset;
        }
        Ok(())
    }

    /// Reads a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block.
    ///
    /// It should not be called to score documents
    /// for instance.
    pub fn get(&self, doc_id: DocId) -> crate::Result<Document> {
        let (first_doc_id, block_offset) = self.block_offset(doc_id);
        self.read_block(block_offset as usize)?;
        let current_block_mut = self.current_block.borrow_mut();
        let mut cursor = &current_block_mut[..];
        for _ in first_doc_id..doc_id {
            let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
            cursor = &cursor[doc_length..];
        }
        let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
        cursor = &cursor[..doc_length];
        Ok(Document::deserialize(&mut cursor)?)
    }

    /// Summarize total space usage of this store reader.
    pub fn space_usage(&self) -> StoreSpaceUsage {
        StoreSpaceUsage::new(self.data.len(), self.offset_index_file.len())
    }
}

fn split_file(data: FileSlice) -> io::Result<(FileSlice, FileSlice, DocId)> {
    let data_len = data.len();
    let footer_offset = data_len - size_of::<u64>() - size_of::<u32>();
    let serialized_offset: OwnedBytes = data.slice(footer_offset, data_len).read_bytes()?;
    let mut serialized_offset_buf = serialized_offset.as_slice();
    let offset = u64::deserialize(&mut serialized_offset_buf)?;
    let offset = offset as usize;
    let max_doc = u32::deserialize(&mut serialized_offset_buf)?;
    Ok((
        data.slice(0, offset),
        data.slice(offset, footer_offset),
        max_doc,
    ))
}
