mod pool;

pub use self::pool::LeasedItem;
use self::pool::Pool;
use crate::core::Segment;
use crate::directory::WatchHandle;
use crate::directory::META_LOCK;
use crate::directory::{Directory, WatchCallback};
use crate::Index;
use crate::Searcher;
use crate::SegmentReader;
use std::sync::Arc;
use std::{convert::TryInto, io};

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
    /// No change is reflected automatically. You are required to call `IndexReader::reload()` manually.
    Manual,
    /// The index is reloaded within milliseconds after a new commit is available.
    /// This is made possible by watching changes in the `meta.json` file.
    OnCommit, // TODO add NEAR_REAL_TIME(target_ms)
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
            reload_policy: ReloadPolicy::OnCommit,
            index,
        }
    }

    /// Builds the reader.
    ///
    /// Building the reader is a non-trivial operation that requires
    /// to open different segment readers. It may take hundreds of milliseconds
    /// of time and it may return an error.
    pub fn try_into(self) -> crate::Result<IndexReader> {
        let inner_reader = InnerIndexReader {
            index: self.index,
            num_searchers: self.num_searchers,
            searcher_pool: Pool::new(),
        };
        inner_reader.reload()?;
        let inner_reader_arc = Arc::new(inner_reader);
        let watch_handle_opt: Option<WatchHandle>;
        match self.reload_policy {
            ReloadPolicy::Manual => {
                // No need to set anything...
                watch_handle_opt = None;
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
                watch_handle_opt = Some(watch_handle);
            }
        }
        Ok(IndexReader {
            inner: inner_reader_arc,
            watch_handle_opt,
        })
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

impl TryInto<IndexReader> for IndexReaderBuilder {
    type Error = crate::TantivyError;

    fn try_into(self) -> crate::Result<IndexReader> {
        IndexReaderBuilder::try_into(self)
    }
}

struct InnerIndexReader {
    num_searchers: usize,
    searcher_pool: Pool<Searcher>,
    index: Index,
}

impl InnerIndexReader {
    fn reload(&self) -> crate::Result<()> {
        let segment_readers: Vec<SegmentReader> = {
            let _meta_lock = self.index.directory().acquire_lock(&META_LOCK)?;
            let searchable_segments = self.searchable_segments()?;
            searchable_segments
                .iter()
                .map(SegmentReader::open)
                .collect::<crate::Result<_>>()?
        };
        let schema = self.index.schema();
        let searchers: Vec<Searcher> = std::iter::repeat_with(|| {
            Searcher::new(schema.clone(), self.index.clone(), segment_readers.clone())
        })
        .take(self.num_searchers)
        .collect::<io::Result<_>>()?;
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

    /// Returns the list of segments that are searchable
    fn searchable_segments(&self) -> crate::Result<Vec<Segment>> {
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
///
/// `Clone` does not clone the different pool of searcher. `IndexReader`
/// just wraps and `Arc`.
#[derive(Clone)]
pub struct IndexReader {
    inner: Arc<InnerIndexReader>,
    watch_handle_opt: Option<WatchHandle>,
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
