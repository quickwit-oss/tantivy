mod pool;

pub use self::pool::LeasedItem;
use self::pool::Pool;
use crate::core::Segment;
use crate::directory::WatchHandle;
use crate::directory::META_LOCK;
use crate::directory::{Directory, WatchCallback};
use crate::Executor;
use crate::Index;
use crate::Searcher;
use crate::SegmentId;
use crate::SegmentReader;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;
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
    warmers: Vec<Weak<dyn Warmer>>,
    num_warming_threads: usize,
}

impl IndexReaderBuilder {
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
        let warming_executor = if self.num_warming_threads <= 1 {
            Executor::single_thread()
        } else {
            Executor::multi_thread(self.num_warming_threads, "tantivy-warm-")?
        };
        let inner_reader = InnerIndexReader {
            index: self.index,
            num_searchers: self.num_searchers,
            searcher_pool: Pool::new(),
            warmers: self.warmers,
            warming_executor,
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
            _watch_handle_opt: watch_handle_opt,
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

    /// Sets the number of warming threads.
    pub fn num_warming_threads(mut self, num_warming_threads: usize) -> IndexReaderBuilder {
        self.num_warming_threads = num_warming_threads;
        self
    }

    /// Register a [Warmer] for active [SegmentReader]s before they can be used by [Searcher]s.
    pub fn register_warmer(mut self, warmer: Weak<dyn Warmer>) -> IndexReaderBuilder {
        self.warmers.push(warmer);
        self
    }
}

impl TryInto<IndexReader> for IndexReaderBuilder {
    type Error = crate::TantivyError;

    fn try_into(self) -> crate::Result<IndexReader> {
        IndexReaderBuilder::try_into(self)
    }
}

/// `Warmer` can be used to maintain any segment-level caches.
pub trait Warmer: Sync + Send {
    /// Perform any warming work using the provided [Searcher].
    fn warm(&self, searcher: &Searcher) -> crate::Result<()>;

    /// Discard internal state for any [SegmentId] not provided.
    ///
    /// This is a garbage collection mechanism.
    fn retain(&self, segment_ids: &HashSet<SegmentId>);
}

struct InnerIndexReader {
    num_searchers: usize,
    searcher_pool: Pool<Searcher>,
    index: Index,
    warmers: Vec<Weak<dyn Warmer>>,
    warming_executor: Executor,
}

impl InnerIndexReader {
    fn list_warmers(&self) -> Vec<Arc<dyn Warmer>> {
        self.warmers
            .iter()
            .flat_map(|weak_warmer| weak_warmer.upgrade())
            .collect()
    }

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
        let warmers = self.list_warmers();
        self.warming_executor
            .map(|warmer| warmer.warm(&searchers[0]), warmers.iter().cloned())?;
        self.searcher_pool.publish_new_generation(searchers);
        // No searcher with previous set of segments will be returned by the pool at this point,
        // so we can inform the warmers to garbgage collect their caches.
        let segment_ids = segment_readers
            .iter()
            .map(|segment_reader| segment_reader.segment_id())
            .collect();
        for warmer in &warmers {
            warmer.retain(&segment_ids);
        }
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            atomic::{self, AtomicUsize},
            Arc, RwLock, Weak,
        },
    };

    use crate::{
        directory::RamDirectory,
        schema::{Schema, INDEXED},
        Index, IndexSettings, ReloadPolicy, Searcher, SegmentId,
    };

    use super::Warmer;

    #[derive(Default)]
    struct TestWarmer {
        active_segment_ids: RwLock<HashSet<SegmentId>>,
        warm_calls: AtomicUsize,
        retain_calls: AtomicUsize,
    }

    impl TestWarmer {
        fn active_segment_ids(&self) -> HashSet<SegmentId> {
            self.active_segment_ids.read().unwrap().clone()
        }
        fn warm_calls(&self) -> usize {
            self.warm_calls.load(atomic::Ordering::Acquire)
        }
        fn retain_calls(&self) -> usize {
            self.retain_calls.load(atomic::Ordering::Acquire)
        }

        fn verify(
            &self,
            expected_segment_ids: HashSet<SegmentId>,
            expected_warm_calls: usize,
            expected_retain_calls: usize,
        ) {
            assert_eq!(self.active_segment_ids(), expected_segment_ids);
            assert_eq!(self.warm_calls(), expected_warm_calls);
            assert_eq!(self.retain_calls(), expected_retain_calls);
        }
    }

    impl Warmer for TestWarmer {
        fn warm(&self, searcher: &crate::Searcher) -> crate::Result<()> {
            self.warm_calls.fetch_add(1, atomic::Ordering::Release);
            for reader in searcher.segment_readers() {
                self.active_segment_ids
                    .write()
                    .unwrap()
                    .insert(reader.segment_id());
            }
            Ok(())
        }
        fn retain(&self, segment_ids: &HashSet<SegmentId>) {
            self.retain_calls
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            self.active_segment_ids
                .write()
                .unwrap()
                .retain(|id| segment_ids.contains(id));
        }
    }

    fn segment_ids(searcher: &Searcher) -> HashSet<SegmentId> {
        searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect()
    }

    #[test]
    fn warming() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field("pk", INDEXED);
        let schema = schema_builder.build();

        let directory = RamDirectory::create();
        let index = Index::create(directory.clone(), schema, IndexSettings::default())?;

        let mut writer = index.writer_with_num_threads(4, 25_000_000).unwrap();

        for i in 0u64..100u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;

        let warmer1 = Arc::new(TestWarmer::default());
        let warmer2 = Arc::new(TestWarmer::default());
        warmer1.verify(HashSet::new(), 0, 0);
        warmer2.verify(HashSet::new(), 0, 0);

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .register_warmer(Arc::downgrade(&warmer1) as Weak<dyn Warmer>)
            .register_warmer(Arc::downgrade(&warmer2) as Weak<dyn Warmer>)
            .try_into()?;

        warmer1.verify(segment_ids(&reader.searcher()), 1, 1);
        warmer2.verify(segment_ids(&reader.searcher()), 1, 1);
        assert_eq!(reader.searcher().num_docs(), 100);

        reader.reload()?;

        warmer1.verify(segment_ids(&reader.searcher()), 2, 2);
        warmer2.verify(segment_ids(&reader.searcher()), 2, 2);
        assert_eq!(reader.searcher().num_docs(), 100);

        for i in 100u64..200u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;

        drop(warmer1);

        reader.reload()?;

        assert_eq!(reader.searcher().num_docs(), 200);
        warmer2.verify(segment_ids(&reader.searcher()), 3, 3);

        Ok(())
    }
}
