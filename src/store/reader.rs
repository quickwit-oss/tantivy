use super::decompress;
use super::skiplist::SkipList;
use crate::common::VInt;
use crate::common::{BinarySerializable, HasLen};
use crate::directory::{FileSlice, OwnedBytes};
use crate::schema::Document;
use crate::space_usage::StoreSpaceUsage;
use crate::DocId;
use lru::LruCache;
use std::io;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const LRU_CACHE_CAPACITY: usize = 100;

type Block = Arc<Vec<u8>>;

type BlockCache = Arc<Mutex<LruCache<usize, Block>>>;

/// Reads document off tantivy's [`Store`](./index.html)
#[derive(Clone)]
pub struct StoreReader {
    data: FileSlice,
    offset_index_file: OwnedBytes,
    max_doc: DocId,
    cache: BlockCache,
    cache_hits: Arc<AtomicUsize>,
    cache_misses: Arc<AtomicUsize>,
}

impl StoreReader {
    /// Opens a store reader
    // TODO rename open
    pub fn open(store_file: FileSlice) -> io::Result<StoreReader> {
        let (data_file, offset_index_file, max_doc) = split_file(store_file)?;
        Ok(StoreReader {
            data: data_file,
            offset_index_file: offset_index_file.read_bytes()?,
            max_doc,
            cache: Arc::new(Mutex::new(LruCache::new(LRU_CACHE_CAPACITY))),
            cache_hits: Default::default(),
            cache_misses: Default::default(),
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

    fn read_block(&self, block_offset: usize) -> io::Result<Block> {
        if let Some(block) = self.cache.lock().unwrap().get(&block_offset) {
            self.cache_hits.fetch_add(1, Ordering::SeqCst);
            return Ok(block.clone());
        }

        self.cache_misses.fetch_add(1, Ordering::SeqCst);

        let compressed_block = self.compressed_block(block_offset)?;
        let mut decompressed_block = vec![];
        decompress(compressed_block.as_slice(), &mut decompressed_block)?;

        let block = Arc::new(decompressed_block);
        self.cache.lock().unwrap().put(block_offset, block.clone());

        Ok(block)
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
        let mut cursor = &self.read_block(block_offset as usize)?[..];

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Document;
    use crate::schema::Field;
    use crate::{directory::RAMDirectory, store::tests::write_lorem_ipsum_store, Directory};
    use std::path::Path;

    fn get_text_field<'a>(doc: &'a Document, field: &'a Field) -> Option<&'a str> {
        doc.get_first(*field).and_then(|f| f.text())
    }

    #[test]
    fn test_store_lru_cache() -> crate::Result<()> {
        let directory = RAMDirectory::create();
        let path = Path::new("store");
        let writer = directory.open_write(path)?;
        let schema = write_lorem_ipsum_store(writer, 500);
        let title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file)?;

        assert_eq!(store.cache.lock().unwrap().len(), 0);
        assert_eq!(store.cache_hits.load(Ordering::SeqCst), 0);
        assert_eq!(store.cache_misses.load(Ordering::SeqCst), 0);

        let doc = store.get(0)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 0"));

        assert_eq!(store.cache.lock().unwrap().len(), 1);
        assert_eq!(store.cache_hits.load(Ordering::SeqCst), 0);
        assert_eq!(store.cache_misses.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .cache
                .lock()
                .unwrap()
                .peek_lru()
                .map(|(&k, _)| k as usize),
            Some(0)
        );

        let doc = store.get(499)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 499"));

        assert_eq!(store.cache.lock().unwrap().len(), 2);
        assert_eq!(store.cache_hits.load(Ordering::SeqCst), 0);
        assert_eq!(store.cache_misses.load(Ordering::SeqCst), 2);

        assert_eq!(
            store
                .cache
                .lock()
                .unwrap()
                .peek_lru()
                .map(|(&k, _)| k as usize),
            Some(0)
        );

        let doc = store.get(0)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 0"));

        assert_eq!(store.cache.lock().unwrap().len(), 2);
        assert_eq!(store.cache_hits.load(Ordering::SeqCst), 1);
        assert_eq!(store.cache_misses.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .cache
                .lock()
                .unwrap()
                .peek_lru()
                .map(|(&k, _)| k as usize),
            Some(18862)
        );

        Ok(())
    }
}
