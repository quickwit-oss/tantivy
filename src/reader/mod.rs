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

/// Defines when a new version of the index should be reloaded.
///
/// Regardless of whether you search and index in the same process, tantivy does not necessarily
/// reflects the change that are commited to your index. `ReloadPolicy` precisely helps you define
/// when you want your index to be reloaded.
#[derive(Clone, Copy)]
pub enum ReloadPolicy {
    /// The index is entirely reloaded manually.
    /// All updates of the index should be manual.
    ///
    /// No change is reflected automatically. You are required to call `.load_seacher()` manually.
    Manual,
    /// The index is reloaded within milliseconds after a new commit is available.
    /// This is made possible by watching changes in the `meta.json` file.
    OnCommit
    // TODO add NEAR_REAL_TIME(target_ms),
}

/// `IndexReader` builder
///
/// It makes it possible to set the following values.
///
/// - `num_searchers` (by default, the number of detected CPU threads):
///
///   When `num_searchers` queries are requested at the same time, the `num_searchers` will block
///   until the one of the searcher in-use gets released.
/// - `reload_policy` (by default `ReloadPolicy::OnCommit`):
///
///   See [`ReloadPolicy`](./enum.ReloadPolicy.html) for more details.
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
            reload_policy: ReloadPolicy::Manual,
            index,
        }
    }

    /// Builds the reader.
    ///
    /// Building the reader is a non-trivial operation that requires
    /// to open different segment readers. It may take hundreds of milliseconds
    /// of time and it may return an error.
    /// TODO(pmasurel) Use the `TryInto` trait once it is available in stable.
    pub fn try_into(self) -> Result<IndexReader> {
        let reader = IndexReader::new(self.index, self.num_searchers, self.reload_policy);
        reader.load_searchers()?;
        Ok(reader)
    }

    /// Sets the reload_policy.
    ///
    /// See [`ReloadPolicy`](./enum.ReloadPolicy.html) for more details.
    pub fn reload_policy(mut self, reload_policy: ReloadPolicy) -> IndexReaderBuilder {
        self.reload_policy = reload_policy;
        self
    }

    /// Sets the number of `Searcher` in the searcher pool.
    pub fn num_searchers(mut self, num_searchers: usize) -> IndexReaderBuilder {
        self.num_searchers = num_searchers;
        self
    }
}

struct InnerIndexReader {
    num_searchers: usize,
    searcher_pool: Pool<Searcher>,
    _reload_policy: ReloadPolicy,
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

/// `IndexReader` is your entry point to read and search the index.
///
/// It controls when a new version of the index should be loaded and lends
/// you instances of `Searcher` for the last loaded version.
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
                _reload_policy: reload_policy,
            }),
        }
    }
}
