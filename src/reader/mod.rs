mod pool;

use self::pool::{LeasedItem, Pool};
use core::Segment;
use directory::Directory;
use directory::META_LOCK;
use std::sync::Arc;
use Index;
use Result;
use Searcher;
use SegmentReader;

#[derive(Clone, Copy)]
pub enum ReloadPolicy {
    MANUAL,
    // NEAR_REAL_TIME(target_ms),
    ON_COMMIT,
}

#[derive(Clone)]
pub struct IndexReaderBuilder {
    num_searchers: usize,
    reload_policy: ReloadPolicy,
    index: Index,
}

impl IndexReaderBuilder {
    pub(crate) fn new(index: Index) -> IndexReaderBuilder {
        IndexReaderBuilder {
            num_searchers: num_cpus::get(),
            reload_policy: ReloadPolicy::MANUAL,
            index,
        }
    }
}

impl Into<IndexReader> for IndexReaderBuilder {
    fn into(self) -> IndexReader {
        IndexReader::new(self.index, self.num_searchers, self.reload_policy)
    }
}

struct InnerIndexReader {
    num_searchers: usize,
    searcher_pool: Pool<Searcher>,
    reload_policy: ReloadPolicy,
    index: Index,
}

impl InnerIndexReader {
    fn load_searchers(&self) -> Result<()> {
        let _meta_lock = self.index.directory().acquire_lock(&META_LOCK)?;
        let searchable_segments = self.searchable_segments()?;
        let segment_readers: Vec<SegmentReader> = searchable_segments
            .iter()
            .map(SegmentReader::open)
            .collect::<Result<_>>()?;
        let schema = self.index.schema();
        let searchers = (0..self.num_searchers)
            .map(|_| Searcher::new(schema.clone(), self.index.clone(), segment_readers.clone()))
            .collect();
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

    /// Returns the list of segments that are searchable
    fn searchable_segments(&self) -> Result<Vec<Segment>> {
        self.index.searchable_segments()
    }

    fn searcher(&self) -> LeasedItem<Searcher> {
        self.searcher_pool.acquire()
    }
}

pub struct IndexReader {
    inner: Arc<InnerIndexReader>,
}

impl IndexReader {
    /// Update searchers so that they reflect the state of the last
    /// `.commit()`.
    ///
    /// If indexing happens in the same process as searching,
    /// you most likely want to call `.load_searchers()` right after each
    /// successful call to `.commit()`.
    ///
    /// If indexing and searching happen in different processes, the way to
    /// get the freshest `index` at all time, is to watch `meta.json` and
    /// call `load_searchers` whenever a changes happen.
    pub fn load_searchers(&self) -> Result<()> {
        self.inner.load_searchers()
    }

    /// Returns a searcher
    ///
    /// This method should be called every single time a search
    /// query is performed.
    /// The searchers are taken from a pool of `num_searchers` searchers.
    /// If no searcher is available
    /// this may block.
    ///
    /// The same searcher must be used for a given query, as it ensures
    /// the use of a consistent segment set.
    pub fn searcher(&self) -> LeasedItem<Searcher> {
        self.inner.searcher()
    }

    pub(crate) fn new(
        index: Index,
        num_searchers: usize,
        reload_policy: ReloadPolicy,
    ) -> IndexReader {
        IndexReader {
            inner: Arc::new(InnerIndexReader {
                index,
                num_searchers,
                searcher_pool: Pool::new(),
                reload_policy,
            }),
        }
    }
}
