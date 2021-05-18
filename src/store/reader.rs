use super::decompress;
use super::index::SkipIndex;
use crate::common::{BinarySerializable, HasLen};
use crate::directory::{FileSlice, OwnedBytes};
use crate::schema::Document;
use crate::space_usage::StoreSpaceUsage;
use crate::store::index::Checkpoint;
use crate::DocId;
use crate::{common::VInt, fastfield::DeleteBitSet};
use lru::LruCache;
use std::io;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const LRU_CACHE_CAPACITY: usize = 100;

type Block = Arc<Vec<u8>>;

type BlockCache = Arc<Mutex<LruCache<usize, Block>>>;

/// Reads document off tantivy's [`Store`](./index.html)
pub struct StoreReader {
    data: FileSlice,
    cache: BlockCache,
    cache_hits: Arc<AtomicUsize>,
    cache_misses: Arc<AtomicUsize>,
    skip_index: Arc<SkipIndex>,
    space_usage: StoreSpaceUsage,
}

impl StoreReader {
    /// Opens a store reader
    pub fn open(store_file: FileSlice) -> io::Result<StoreReader> {
        let (data_file, offset_index_file) = split_file(store_file)?;
        let index_data = offset_index_file.read_bytes()?;
        let space_usage = StoreSpaceUsage::new(data_file.len(), offset_index_file.len());
        let skip_index = SkipIndex::open(index_data);
        Ok(StoreReader {
            data: data_file,
            cache: Arc::new(Mutex::new(LruCache::new(LRU_CACHE_CAPACITY))),
            cache_hits: Default::default(),
            cache_misses: Default::default(),
            skip_index: Arc::new(skip_index),
            space_usage,
        })
    }

    pub(crate) fn block_checkpoints(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.skip_index.checkpoints()
    }

    fn block_checkpoint(&self, doc_id: DocId) -> Option<Checkpoint> {
        self.skip_index.seek(doc_id)
    }

    pub(crate) fn block_data(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes()
    }

    fn compressed_block(&self, checkpoint: &Checkpoint) -> io::Result<OwnedBytes> {
        self.data.slice(checkpoint.byte_range.clone()).read_bytes()
    }

    fn read_block(&self, checkpoint: &Checkpoint) -> io::Result<Block> {
        if let Some(block) = self.cache.lock().unwrap().get(&checkpoint.byte_range.start) {
            self.cache_hits.fetch_add(1, Ordering::SeqCst);
            return Ok(block.clone());
        }

        self.cache_misses.fetch_add(1, Ordering::SeqCst);

        let compressed_block = self.compressed_block(checkpoint)?;
        let mut decompressed_block = vec![];
        decompress(compressed_block.as_slice(), &mut decompressed_block)?;

        let block = Arc::new(decompressed_block);
        self.cache
            .lock()
            .unwrap()
            .put(checkpoint.byte_range.start, block.clone());

        Ok(block)
    }

    /// Reads a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block. The store utilizes a LRU cache,
    /// so accessing docs from the same compressed block should be faster.
    /// For that reason a store reader should be kept and reused.
    ///
    /// It should not be called to score documents
    /// for instance.
    pub fn get(&self, doc_id: DocId) -> crate::Result<Document> {
        let raw_doc = self.get_raw(doc_id)?;
        let mut cursor = raw_doc.get_bytes();
        Ok(Document::deserialize(&mut cursor)?)
    }

    /// Reads raw bytes of a given document. Returns `RawDocument`, which contains the block of a document and its start and end
    /// position within the block.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block. The store utilizes a LRU cache,
    /// so accessing docs from the same compressed block should be faster.
    /// For that reason a store reader should be kept and reused.
    ///
    pub fn get_raw(&self, doc_id: DocId) -> crate::Result<RawDocument> {
        let checkpoint = self.block_checkpoint(doc_id).ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!("Failed to lookup Doc #{}.", doc_id))
        })?;
        let block = self.read_block(&checkpoint)?;
        let mut cursor = &block[..];
        let cursor_len_before = cursor.len();
        for _ in checkpoint.doc_range.start..doc_id {
            let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
            cursor = &cursor[doc_length..];
        }

        let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
        let start_pos = cursor_len_before - cursor.len();
        let end_pos = cursor_len_before - cursor.len() + doc_length;
        Ok(RawDocument {
            block,
            start_pos,
            end_pos,
        })
    }

    /// Iterator over all Documents in their order as they are stored in the doc store.
    /// Use this, if you want to extract all Documents from the doc store.
    /// The delete_bitset has to be forwarded from the `SegmentReader` or the results maybe wrong.
    pub fn iter<'a: 'b, 'b>(
        &'b self,
        delete_bitset: Option<&'a DeleteBitSet>,
    ) -> impl Iterator<Item = crate::Result<Document>> + 'b {
        self.iter_raw(delete_bitset).map(|raw_doc| {
            let raw_doc = raw_doc?;
            let mut cursor = raw_doc.get_bytes();
            Ok(Document::deserialize(&mut cursor)?)
        })
    }

    /// Iterator over all RawDocuments in their order as they are stored in the doc store.
    /// Use this, if you want to extract all Documents from the doc store.
    /// The delete_bitset has to be forwarded from the `SegmentReader` or the results maybe wrong.
    pub(crate) fn iter_raw<'a: 'b, 'b>(
        &'b self,
        delete_bitset: Option<&'a DeleteBitSet>,
    ) -> impl Iterator<Item = crate::Result<RawDocument>> + 'b {
        let last_docid = self
            .block_checkpoints()
            .last()
            .map(|checkpoint| checkpoint.doc_range.end)
            .unwrap_or(0);
        let mut checkpoint_block_iter = self.block_checkpoints();
        let mut curr_checkpoint = checkpoint_block_iter.next();
        let mut curr_block = curr_checkpoint
            .as_ref()
            .map(|checkpoint| self.read_block(&checkpoint));
        let mut block_start_pos = 0;
        let mut num_skipped = 0;
        (0..last_docid)
            .filter_map(move |doc_id| {
                // filter_map is only used to resolve lifetime issues between the two closures on
                // the outer variables
                let alive = delete_bitset.map_or(true, |bitset| bitset.is_alive(doc_id));
                if !alive {
                    // we keep the number of skipped documents to move forward in the map block
                    num_skipped += 1;
                }
                // check move to next checkpoint
                let mut reset_block_pos = false;
                if doc_id >= curr_checkpoint.as_ref().unwrap().doc_range.end {
                    curr_checkpoint = checkpoint_block_iter.next();
                    curr_block = curr_checkpoint
                        .as_ref()
                        .map(|checkpoint| self.read_block(&checkpoint));
                    reset_block_pos = true;
                    num_skipped = 0;
                }

                if alive {
                    let ret = Some((
                        curr_block.as_ref().unwrap().as_ref().unwrap().clone(), // todo forward errors
                        num_skipped,
                        reset_block_pos,
                    ));
                    // the map block will move over the num_skipped, so we reset to 0
                    num_skipped = 0;
                    ret
                } else {
                    None
                }
            })
            .map(move |(block, num_skipped, reset_block_pos)| {
                if reset_block_pos {
                    block_start_pos = 0;
                }
                let mut cursor = &block[block_start_pos..];
                let mut pos = 0;
                let doc_length = loop {
                    let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
                    let num_bytes_read = block[block_start_pos..].len() - cursor.len();
                    block_start_pos += num_bytes_read;

                    pos += 1;
                    if pos == num_skipped + 1 {
                        break doc_length;
                    } else {
                        block_start_pos += doc_length;
                        cursor = &block[block_start_pos..];
                    }
                };
                let end_pos = block_start_pos + doc_length;
                let raw_doc = RawDocument {
                    block,
                    start_pos: block_start_pos,
                    end_pos,
                };
                block_start_pos = end_pos;
                Ok(raw_doc)
            })
    }

    /// Summarize total space usage of this store reader.
    pub fn space_usage(&self) -> StoreSpaceUsage {
        self.space_usage.clone()
    }
}

/// Get the bytes of a serialized `Document` in a decompressed block.
pub struct RawDocument {
    /// the block of data containing multiple documents
    block: Arc<Vec<u8>>,
    /// start position of the document in the block
    start_pos: usize,
    /// end position of the document in the block
    end_pos: usize,
}

impl RawDocument {
    /// Get the bytes of a serialized `Document` in a decompressed block.
    pub fn get_bytes(&self) -> &[u8] {
        &self.block[self.start_pos..self.end_pos]
    }
}

fn split_file(data: FileSlice) -> io::Result<(FileSlice, FileSlice)> {
    let (data, footer_len_bytes) = data.split_from_end(size_of::<u64>());
    let serialized_offset: OwnedBytes = footer_len_bytes.read_bytes()?;
    let mut serialized_offset_buf = serialized_offset.as_slice();
    let offset = u64::deserialize(&mut serialized_offset_buf)? as usize;
    Ok(data.split(offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Document;
    use crate::schema::Field;
    use crate::{directory::RamDirectory, store::tests::write_lorem_ipsum_store, Directory};
    use std::path::Path;

    fn get_text_field<'a>(doc: &'a Document, field: &'a Field) -> Option<&'a str> {
        doc.get_first(*field).and_then(|f| f.text())
    }

    #[test]
    fn test_store_lru_cache() -> crate::Result<()> {
        let directory = RamDirectory::create();
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
            Some(9249)
        );

        Ok(())
    }
}
