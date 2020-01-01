use super::pool::Pool;
use crate::core::Segment;
use crate::directory::Directory;
use crate::directory::WatchHandle;
use crate::directory::META_LOCK;
use crate::Searcher;
use crate::SegmentReader;
use crate::{Index, LeasedItem};
use crate::{IndexReader, Result};
use std::iter::repeat_with;
use std::sync::Arc;

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
    /// TODO(pmasurel) Use the `TryInto` trait once it is available in stable.
    pub fn try_into(self) -> Result<IndexReader> {
        let inner_reader = MetaFileIndexReaderInner {
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
                    .watch(Box::new(callback))?;
                watch_handle_opt = Some(watch_handle);
            }
        }
        Ok(IndexReader::from(MetaFileIndexReader {
            inner: inner_reader_arc,
            watch_handle_opt,
        }))
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

struct MetaFileIndexReaderInner {
    num_searchers: usize,
    searcher_pool: Pool<Searcher>,
    index: Index,
}

impl MetaFileIndexReaderInner {
    fn load_segment_readers(&self) -> crate::Result<Vec<SegmentReader>> {
        // We keep the lock until we have effectively finished opening the
        // the `SegmentReader` because it prevents a diffferent process
        // to garbage collect these file while we open them.
        //
        // Once opened, on linux & mac, the mmap will remain valid after
        // the file has been deleted
        // On windows, the file deletion will fail.
        let _meta_lock = self.index.directory().acquire_lock(&META_LOCK)?;
        let searchable_segments = self.searchable_segments()?;
        searchable_segments
            .iter()
            .map(SegmentReader::open)
            .collect::<Result<_>>()
    }

    fn reload(&self) -> crate::Result<()> {
        let segment_readers: Vec<SegmentReader> = self.load_segment_readers()?;
        let schema = self.index.schema();
        let searchers = repeat_with(|| {
            Searcher::new(schema.clone(), self.index.clone(), segment_readers.clone())
        })
        .take(self.num_searchers)
        .collect();
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
pub struct MetaFileIndexReader {
    inner: Arc<MetaFileIndexReaderInner>,
    watch_handle_opt: Option<WatchHandle>,
}

impl MetaFileIndexReader {
    pub fn reload(&self) -> crate::Result<()> {
        self.inner.reload()
    }
    pub fn searcher(&self) -> LeasedItem<Searcher> {
        self.inner.searcher()
    }
}
