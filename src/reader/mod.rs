mod pool;
mod warming;

use std::convert::TryInto;
use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc, Weak};

pub use warming::Warmer;

pub use self::pool::LeasedItem;
use self::pool::Pool;
use self::warming::WarmingState;
use crate::core::searcher::SearcherGeneration;
use crate::directory::{Directory, WatchCallback, WatchHandle, META_LOCK};
use crate::{Index, Inventory, Searcher, SegmentReader, TrackedObject};

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
    /// No change is reflected automatically. You are required to call `IndexReader::reload()`
    /// manually.
    Manual,
    /// The index is reloaded within milliseconds after a new commit is available.
    /// This is made possible by watching changes in the `meta.json` file.
    OnCommit, // TODO add NEAR_REAL_TIME(target_ms)
}

/// [IndexReader] builder
///
/// It makes it possible to configure:
/// - [Searcher] pool size
/// - [ReloadPolicy] defining when new index versions are detected
/// - [Warmer] implementations
/// - number of warming threads, for parallelizing warming work
#[derive(Clone)]
pub struct IndexReaderBuilder {
    num_searchers: usize,
    reload_policy: ReloadPolicy,
    index: Index,
    warmers: Vec<Weak<dyn Warmer>>,
    num_warming_threads: usize,
}

impl IndexReaderBuilder {
    #[must_use]
    pub(crate) fn new(index: Index) -> IndexReaderBuilder {
        IndexReaderBuilder {
            num_searchers: num_cpus::get(),
            reload_policy: ReloadPolicy::OnCommit,
            index,
            warmers: Vec::new(),
            num_warming_threads: 1,
        }
    }

    /// Builds the reader.
    ///
    /// Building the reader is a non-trivial operation that requires
    /// to open different segment readers. It may take hundreds of milliseconds
    /// of time and it may return an error.
    pub fn try_into(self) -> crate::Result<IndexReader> {
        let searcher_generation_inventory = Inventory::default();
        let warming_state = WarmingState::new(
            self.num_warming_threads,
            self.warmers,
            searcher_generation_inventory.clone(),
        )?;
        let inner_reader = InnerIndexReader {
            index: self.index,
            num_searchers: self.num_searchers,
            searcher_pool: Pool::new(),
            warming_state,
            searcher_generation_counter: Default::default(),
            searcher_generation_inventory,
        };
        inner_reader.reload()?;
        let inner_reader_arc = Arc::new(inner_reader);
        let watch_handle_opt: Option<WatchHandle> = match self.reload_policy {
            ReloadPolicy::Manual => {
                // No need to set anything...
                None
            }
            ReloadPolicy::OnCommit => {
                let inner_reader_arc_clone = inner_reader_arc.clone();
                let callback = move || {
                    if let Err(err) = inner_reader_arc_clone.reload() {
                        error!(
                            "Error while loading searcher after commit was detected. {:?}",
                            err
                        );
                    }
                };
                let watch_handle = inner_reader_arc
                    .index
                    .directory()
                    .watch(WatchCallback::new(callback))?;
                Some(watch_handle)
            }
        };
        Ok(IndexReader {
            inner: inner_reader_arc,
            _watch_handle_opt: watch_handle_opt,
        })
    }

    /// Sets the reload_policy.
    ///
    /// See [`ReloadPolicy`](./enum.ReloadPolicy.html) for more details.
    #[must_use]
    pub fn reload_policy(mut self, reload_policy: ReloadPolicy) -> IndexReaderBuilder {
        self.reload_policy = reload_policy;
        self
    }

    /// Sets the number of [Searcher] to pool.
    ///
    /// See [IndexReader::searcher()].
    #[must_use]
    pub fn num_searchers(mut self, num_searchers: usize) -> IndexReaderBuilder {
        self.num_searchers = num_searchers;
        self
    }

    /// Set the [Warmer]s that are invoked when reloading searchable segments.
    #[must_use]
    pub fn warmers(mut self, warmers: Vec<Weak<dyn Warmer>>) -> IndexReaderBuilder {
        self.warmers = warmers;
        self
    }

    /// Sets the number of warming threads.
    ///
    /// This allows parallelizing warming work when there are multiple [Warmer] registered with the
    /// [IndexReader].
    #[must_use]
    pub fn num_warming_threads(mut self, num_warming_threads: usize) -> IndexReaderBuilder {
        self.num_warming_threads = num_warming_threads;
        self
    }
}

impl TryInto<IndexReader> for IndexReaderBuilder {
    type Error = crate::TantivyError;

    fn try_into(self) -> crate::Result<IndexReader> {
        IndexReaderBuilder::try_into(self)
    }
}

struct InnerIndexReader {
    num_searchers: usize,
    index: Index,
    warming_state: WarmingState,
    searcher_pool: Pool<Searcher>,
    searcher_generation_counter: Arc<AtomicU64>,
    searcher_generation_inventory: Inventory<SearcherGeneration>,
}

impl InnerIndexReader {
    /// Opens the freshest segments `SegmentReader`.
    ///
    /// This function acquires a lot to prevent GC from removing files
    /// as we are opening our index.
    fn open_segment_readers(&self) -> crate::Result<Vec<SegmentReader>> {
        // Prevents segment files from getting deleted while we are in the process of opening them
        let _meta_lock = self.index.directory().acquire_lock(&META_LOCK)?;
        let searchable_segments = self.index.searchable_segments()?;
        let segment_readers = searchable_segments
            .iter()
            .map(SegmentReader::open)
            .collect::<crate::Result<_>>()?;
        Ok(segment_readers)
    }

    fn create_new_searcher_generation(
        &self,
        segment_readers: &[SegmentReader],
    ) -> TrackedObject<SearcherGeneration> {
        let generation_id = self
            .searcher_generation_counter
            .fetch_add(1, atomic::Ordering::Relaxed);
        let searcher_generation =
            SearcherGeneration::from_segment_readers(segment_readers, generation_id);
        self.searcher_generation_inventory
            .track(searcher_generation)
    }

    fn reload(&self) -> crate::Result<()> {
        let segment_readers = self.open_segment_readers()?;
        let searcher_generation = self.create_new_searcher_generation(&segment_readers);
        let schema = self.index.schema();
        let searchers: Vec<Searcher> = std::iter::repeat_with(|| {
            Searcher::new(
                schema.clone(),
                self.index.clone(),
                segment_readers.clone(),
                searcher_generation.clone(),
            )
        })
        .take(self.num_searchers)
        .collect::<io::Result<_>>()?;
        self.warming_state
            .warm_new_searcher_generation(&searchers[0])?;
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

    fn searcher(&self) -> LeasedItem<Searcher> {
        self.searcher_pool.acquire()
    }
}

/// `IndexReader` is your entry point to read and search the index.
///
/// It controls when a new version of the index should be loaded and lends
/// you instances of `Searcher` for the last loaded version.
///
/// `Clone` does not clone the different pool of searcher. `IndexReader`
/// just wraps and `Arc`.
#[derive(Clone)]
pub struct IndexReader {
    inner: Arc<InnerIndexReader>,
    _watch_handle_opt: Option<WatchHandle>,
}

impl IndexReader {
    #[cfg(test)]
    pub(crate) fn index(&self) -> Index {
        self.inner.index.clone()
    }

    /// Update searchers so that they reflect the state of the last
    /// `.commit()`.
    ///
    /// If you set up the `OnCommit` `ReloadPolicy` (which is the default)
    /// every commit should be rapidly reflected on your `IndexReader` and you should
    /// not need to call `reload()` at all.
    ///
    /// This automatic reload can take 10s of milliseconds to kick in however, and in unit tests
    /// it can be nice to deterministically force the reload of searchers.
    pub fn reload(&self) -> crate::Result<()> {
        self.inner.reload()
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
}
