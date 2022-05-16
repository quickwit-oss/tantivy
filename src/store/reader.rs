use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::{BinarySerializable, HasLen, VInt};
use lru::LruCache;
use ownedbytes::OwnedBytes;

use super::footer::DocStoreFooter;
use super::index::SkipIndex;
use super::Compressor;
use crate::directory::FileSlice;
use crate::error::DataCorruption;
use crate::fastfield::AliveBitSet;
use crate::schema::Document;
use crate::space_usage::StoreSpaceUsage;
use crate::store::index::Checkpoint;
use crate::DocId;

const LRU_CACHE_CAPACITY: usize = 100;

type Block = OwnedBytes;

/// Reads document off tantivy's [`Store`](./index.html)
pub struct StoreReader {
    compressor: Compressor,
    data: FileSlice,
    skip_index: Arc<SkipIndex>,
    space_usage: StoreSpaceUsage,
    cache: BlockCache,
}

struct BlockCache {
    cache: Mutex<LruCache<usize, Block>>,
    cache_hits: Arc<AtomicUsize>,
    cache_misses: Arc<AtomicUsize>,
}

pub struct CacheStats {
    pub num_entries: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

impl BlockCache {
    fn get_from_cache(&self, pos: &usize) -> Option<Block> {
        if let Some(block) = self.cache.lock().unwrap().get(pos) {
            self.cache_hits.fetch_add(1, Ordering::SeqCst);
            return Some(block.clone());
        }
        self.cache_misses.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn put_into_cache(&self, pos: usize, data: Block) {
        self.cache.lock().unwrap().put(pos, data);
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            num_entries: self.len(),
        }
    }
    fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

impl StoreReader {
    /// Opens a store reader
    pub fn open(store_file: FileSlice) -> io::Result<StoreReader> {
        let (footer, data_and_offset) = DocStoreFooter::extract_footer(store_file)?;

        let (data_file, offset_index_file) = data_and_offset.split(footer.offset as usize);
        let index_data = offset_index_file.read_bytes()?;
        let space_usage = StoreSpaceUsage::new(data_file.len(), offset_index_file.len());
        let skip_index = SkipIndex::open(index_data);
        Ok(StoreReader {
            compressor: footer.compressor,
            data: data_file,
            cache: BlockCache {
                cache: Mutex::new(LruCache::new(LRU_CACHE_CAPACITY)),
                cache_hits: Default::default(),
                cache_misses: Default::default(),
            },
            skip_index: Arc::new(skip_index),
            space_usage,
        })
    }

    pub(crate) fn block_checkpoints(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.skip_index.checkpoints()
    }

    pub(crate) fn compressor(&self) -> Compressor {
        self.compressor
    }

    /// Returns the cache hit and miss statistics of the store reader.
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Get checkpoint for DocId. The checkpoint can be used to load a block containing the
    /// document.
    ///
    /// Advanced API. In most cases use [get](Self::get).
    pub fn block_checkpoint(&self, doc_id: DocId) -> crate::Result<Checkpoint> {
        self.skip_index.seek(doc_id).ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!("Failed to lookup Doc #{}.", doc_id))
        })
    }

    pub(crate) fn block_data(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes()
    }

    fn compressed_block(&self, checkpoint: &Checkpoint) -> io::Result<OwnedBytes> {
        self.data.slice(checkpoint.byte_range.clone()).read_bytes()
    }

    /// Loads and decompresses a block.
    ///
    /// Advanced API. In most cases use [get](Self::get).
    pub fn read_block(&self, checkpoint: &Checkpoint) -> io::Result<Block> {
        if let Some(block) = self.cache.get_from_cache(&checkpoint.byte_range.start) {
            return Ok(block);
        }

        let compressed_block = self.compressed_block(checkpoint)?;

        self.decompress_block(compressed_block, checkpoint.byte_range.start)
    }

    fn decompress_block(
        &self,
        compressed_block: OwnedBytes,
        cache_key: usize,
    ) -> io::Result<Block> {
        let mut decompressed_block = vec![];
        self.compressor
            .decompress(compressed_block.as_slice(), &mut decompressed_block)?;

        let block = OwnedBytes::new(decompressed_block);
        self.cache.put_into_cache(cache_key, block.clone());

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
        let mut doc_bytes = self.get_document_bytes(doc_id)?;
        Ok(Document::deserialize(&mut doc_bytes)?)
    }

    /// Returns raw bytes of a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block. The store utilizes a LRU cache,
    /// so accessing docs from the same compressed block should be faster.
    /// For that reason a store reader should be kept and reused.
    pub fn get_document_bytes(&self, doc_id: DocId) -> crate::Result<OwnedBytes> {
        let checkpoint = self.block_checkpoint(doc_id)?;
        let block = self.read_block(&checkpoint)?;
        Self::get_document_bytes_from_block(block, doc_id, &checkpoint)
    }

    /// Advanced API.
    ///
    /// In most cases use [get_document_bytes](Self::get_document_bytes).
    pub fn get_document_bytes_from_block(
        block: OwnedBytes,
        doc_id: DocId,
        checkpoint: &Checkpoint,
    ) -> crate::Result<OwnedBytes> {
        let mut cursor = &block[..];
        let cursor_len_before = cursor.len();
        for _ in checkpoint.doc_range.start..doc_id {
            let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
            cursor = &cursor[doc_length..];
        }

        let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
        let start_pos = cursor_len_before - cursor.len();
        let end_pos = cursor_len_before - cursor.len() + doc_length;
        Ok(block.slice(start_pos..end_pos))
    }

    /// Iterator over all Documents in their order as they are stored in the doc store.
    /// Use this, if you want to extract all Documents from the doc store.
    /// The alive_bitset has to be forwarded from the `SegmentReader` or the results maybe wrong.
    pub fn iter<'a: 'b, 'b>(
        &'b self,
        alive_bitset: Option<&'a AliveBitSet>,
    ) -> impl Iterator<Item = crate::Result<Document>> + 'b {
        self.iter_raw(alive_bitset).map(|doc_bytes_res| {
            let mut doc_bytes = doc_bytes_res?;
            Ok(Document::deserialize(&mut doc_bytes)?)
        })
    }

    /// Iterator over all RawDocuments in their order as they are stored in the doc store.
    /// Use this, if you want to extract all Documents from the doc store.
    /// The alive_bitset has to be forwarded from the `SegmentReader` or the results maybe wrong.
    pub(crate) fn iter_raw<'a: 'b, 'b>(
        &'b self,
        alive_bitset: Option<&'a AliveBitSet>,
    ) -> impl Iterator<Item = crate::Result<OwnedBytes>> + 'b {
        let last_doc_id = self
            .block_checkpoints()
            .last()
            .map(|checkpoint| checkpoint.doc_range.end)
            .unwrap_or(0);
        let mut checkpoint_block_iter = self.block_checkpoints();
        let mut curr_checkpoint = checkpoint_block_iter.next();
        let mut curr_block = curr_checkpoint
            .as_ref()
            .map(|checkpoint| self.read_block(checkpoint).map_err(|e| e.kind())); // map error in order to enable cloning
        let mut block_start_pos = 0;
        let mut num_skipped = 0;
        let mut reset_block_pos = false;
        (0..last_doc_id)
            .filter_map(move |doc_id| {
                // filter_map is only used to resolve lifetime issues between the two closures on
                // the outer variables

                // check move to next checkpoint
                if doc_id >= curr_checkpoint.as_ref().unwrap().doc_range.end {
                    curr_checkpoint = checkpoint_block_iter.next();
                    curr_block = curr_checkpoint
                        .as_ref()
                        .map(|checkpoint| self.read_block(checkpoint).map_err(|e| e.kind()));
                    reset_block_pos = true;
                    num_skipped = 0;
                }

                let alive = alive_bitset.map_or(true, |bitset| bitset.is_alive(doc_id));
                if alive {
                    let ret = Some((curr_block.clone(), num_skipped, reset_block_pos));
                    // the map block will move over the num_skipped, so we reset to 0
                    num_skipped = 0;
                    reset_block_pos = false;
                    ret
                } else {
                    // we keep the number of skipped documents to move forward in the map block
                    num_skipped += 1;
                    None
                }
            })
            .map(move |(block, num_skipped, reset_block_pos)| {
                let block = block
                    .ok_or_else(|| {
                        DataCorruption::comment_only(
                            "the current checkpoint in the doc store iterator is none, this \
                             should never happen",
                        )
                    })?
                    .map_err(|error_kind| {
                        std::io::Error::new(error_kind, "error when reading block in doc store")
                    })?;
                // this flag is set, when filter_map moved to the next block
                if reset_block_pos {
                    block_start_pos = 0;
                }
                let mut cursor = &block[block_start_pos..];
                let mut pos = 0;
                // move forward 1 doc + num_skipped in block and return length of current doc
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
                let doc_bytes = block.slice(block_start_pos..end_pos);
                block_start_pos = end_pos;
                Ok(doc_bytes)
            })
    }

    /// Summarize total space usage of this store reader.
    pub fn space_usage(&self) -> StoreSpaceUsage {
        self.space_usage.clone()
    }
}

#[cfg(feature = "quickwit")]
impl StoreReader {
    /// Advanced API.
    ///
    /// In most cases use [get_async](Self::get_async)
    ///
    /// Loads and decompresses a block asynchronously.
    pub async fn read_block_async(&self, checkpoint: &Checkpoint) -> crate::AsyncIoResult<Block> {
        if let Some(block) = self.cache.get_from_cache(&checkpoint.byte_range.start) {
            return Ok(block.clone());
        }

        let compressed_block = self
            .data
            .slice(checkpoint.byte_range.clone())
            .read_bytes_async()
            .await?;
        let block = self.decompress_block(compressed_block, checkpoint.byte_range.start)?;
        Ok(block)
    }

    /// Fetches a document asynchronously.
    pub async fn get_document_bytes_async(&self, doc_id: DocId) -> crate::Result<OwnedBytes> {
        let checkpoint = self.block_checkpoint(doc_id)?;
        let block = self.read_block_async(&checkpoint).await?;
        Self::get_document_bytes_from_block(block, doc_id, &checkpoint)
    }

    /// Reads raw bytes of a given document. Async version of [get](Self::get).
    pub async fn get_async(&self, doc_id: DocId) -> crate::Result<Document> {
        let mut doc_bytes = self.get_document_bytes_async(doc_id).await?;
        Ok(Document::deserialize(&mut doc_bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::directory::RamDirectory;
    use crate::schema::{Document, Field};
    use crate::store::tests::write_lorem_ipsum_store;
    use crate::Directory;

    const BLOCK_SIZE: usize = 16_384;

    fn get_text_field<'a>(doc: &'a Document, field: &'a Field) -> Option<&'a str> {
        doc.get_first(*field).and_then(|f| f.as_text())
    }

    #[test]
    fn test_store_lru_cache() -> crate::Result<()> {
        let directory = RamDirectory::create();
        let path = Path::new("store");
        let writer = directory.open_write(path)?;
        let schema = write_lorem_ipsum_store(writer, 500, Compressor::default(), BLOCK_SIZE);
        let title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file)?;

        assert_eq!(store.cache.len(), 0);
        assert_eq!(store.cache_stats().cache_hits, 0);
        assert_eq!(store.cache_stats().cache_misses, 0);

        let doc = store.get(0)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 0"));

        assert_eq!(store.cache.len(), 1);
        assert_eq!(store.cache_stats().cache_hits, 0);
        assert_eq!(store.cache_stats().cache_misses, 1);

        let doc = store.get(499)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 499"));

        assert_eq!(store.cache.len(), 2);
        assert_eq!(store.cache_stats().cache_hits, 0);
        assert_eq!(store.cache_stats().cache_misses, 2);

        let doc = store.get(0)?;
        assert_eq!(get_text_field(&doc, &title), Some("Doc 0"));

        assert_eq!(store.cache.len(), 2);
        assert_eq!(store.cache_stats().cache_hits, 1);
        assert_eq!(store.cache_stats().cache_misses, 2);

        Ok(())
    }
}
