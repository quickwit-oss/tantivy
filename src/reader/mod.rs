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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
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

/// `Warmer` implementations can be used to maintain segment-level caches,
/// by registering them with the [IndexReaderBuilder].
pub trait Warmer: Sync + Send {
    /// Perform any warming work using the provided [Searcher].
    fn warm(&self, searcher: &Searcher) -> crate::Result<()>;

    /// Discard internal state for any [SegmentId] not provided.
    ///
    /// This should be implemented as cheaply as possible,
    /// it can be invoked inline when requesting [IndexReader::searcher()].
    fn garbage_collect(&self, live_segment_ids: &HashSet<SegmentId>);
}

/// [IndexReader] builder
///
/// It makes it possible to configure:
/// - [Searcher] pool size
/// - [ReloadPolicy] defining when new index versions are detected
/// - [Warmer] implementations to be called when reloading
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
        let inner_reader = InnerIndexReader {
            index: self.index,
            num_searchers: self.num_searchers,
            searcher_pool: Pool::new(),
            warming_state: WarmingState {
                warmers: RwLock::new(self.warmers),
                executor: if self.num_warming_threads <= 1 {
                    Executor::single_thread()
                } else {
                    Executor::multi_thread(self.num_warming_threads, "tantivy-warm-")?
                },
                searcher_generations: Default::default(),
                pending_gc: AtomicBool::new(false),
            },
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

    /// Sets the number of [Searcher] to pool.
    ///
    /// When more than `num_searchers` are requested, the caller will block.
    pub fn num_searchers(mut self, num_searchers: usize) -> IndexReaderBuilder {
        self.num_searchers = num_searchers;
        self
    }

    /// Set the [Warmer]s that are invoked when reloading searchable segments.
    pub fn warmers(mut self, warmers: Vec<Weak<dyn Warmer>>) -> IndexReaderBuilder {
        self.warmers = warmers;
        self
    }

    /// Sets the number of warming threads.
    ///
    /// This allows parallelizing warming work when there are multiple [Warmer] registered with the [IndexReader].
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

/// Warming-related state with interior mutability.
struct WarmingState {
    warmers: RwLock<Vec<Weak<dyn Warmer>>>,
    executor: Executor,
    searcher_generations: RwLock<Vec<(Weak<()>, Vec<SegmentId>)>>,
    pending_gc: AtomicBool,
}

impl WarmingState {
    fn pruned_warmers(&self) -> Vec<Arc<dyn Warmer>> {
        let mut weak_warmers = self.warmers.write().unwrap();
        let strong_warmers = weak_warmers
            .iter()
            .flat_map(|weak_warmer| weak_warmer.upgrade())
            .collect::<Vec<_>>();
        *weak_warmers = strong_warmers.iter().map(|w| Arc::downgrade(w)).collect();
        strong_warmers
    }

    fn live_searcher_generations(&self) -> RwLockWriteGuard<Vec<(Weak<()>, Vec<SegmentId>)>> {
        let mut searcher_generations = self.searcher_generations.write().unwrap();
        *searcher_generations = searcher_generations
            .iter()
            .filter(|(token, _segment_ids)| token.strong_count() > 0)
            .cloned()
            .collect();
        searcher_generations
    }

    fn warm(&self, searcher: &Searcher, token: Weak<()>) -> crate::Result<()> {
        self.executor.map(
            |warmer| warmer.warm(searcher),
            self.pruned_warmers().into_iter(),
        )?;
        let segment_ids = searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect();
        self.live_searcher_generations().push((token, segment_ids));
        self.pending_gc.store(true, Ordering::Release);
        Ok(())
    }

    fn live_segment_ids(&self) -> HashSet<SegmentId> {
        self.live_searcher_generations()
            .iter()
            .flat_map(|(_token, segment_ids)| segment_ids.iter().copied())
            .collect()
    }

    fn eligible_gc(&self) -> bool {
        let searcher_generations = self.searcher_generations.read().unwrap();
        // unique live generation
        (searcher_generations.len() == 1)
            // some generation got dropped
            || (searcher_generations
                .iter()
                .any(|(token, _segment_ids)| token.strong_count() == 0))
    }

    /// Cheap when there is no pending GC.
    ///
    /// Otherwise, we may acquire a read lock to scan searcher generations for GC worthiness,
    /// and trigger [Warmer::garbage_collect()] inline.
    fn maybe_gc(&self) {
        if self.pending_gc.swap(false, Ordering::AcqRel) {
            return;
        }
        if !self.eligible_gc() {
            // try again later
            self.pending_gc.store(true, Ordering::Release);
            return;
        }
        let live_segment_ids = self.live_segment_ids();
        for warmer in self.pruned_warmers() {
            warmer.garbage_collect(&live_segment_ids);
        }
    }
}

struct InnerIndexReader {
    num_searchers: usize,
    searcher_pool: Pool<Searcher>,
    index: Index,
    warming_state: WarmingState,
}

impl InnerIndexReader {
    fn reload(&self) -> crate::Result<()> {
        let generation_token = Arc::new(());
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
            Searcher::new(
                schema.clone(),
                self.index.clone(),
                segment_readers.clone(),
                generation_token.clone(),
            )
        })
        .take(self.num_searchers)
        .collect::<io::Result<_>>()?;
        self.warming_state
            .warm(&searchers[0], Arc::downgrade(&generation_token))?;
        self.searcher_pool.publish_new_generation(searchers);
        self.warming_state.maybe_gc();
        Ok(())
    }

    /// Returns the list of segments that are searchable
    fn searchable_segments(&self) -> crate::Result<Vec<Segment>> {
        self.index.searchable_segments()
    }

    fn searcher(&self) -> LeasedItem<Searcher> {
        let searcher = self.searcher_pool.acquire();
        self.warming_state.maybe_gc();
        searcher
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
        gc_calls: AtomicUsize,
    }

    impl TestWarmer {
        fn active_segment_ids(&self) -> HashSet<SegmentId> {
            self.active_segment_ids.read().unwrap().clone()
        }

        fn warm_calls(&self) -> usize {
            self.warm_calls.load(atomic::Ordering::Acquire)
        }

        fn gc_calls(&self) -> usize {
            self.gc_calls.load(atomic::Ordering::Acquire)
        }

        fn verify(
            &self,
            expected_segment_ids: HashSet<SegmentId>,
            expected_warm_calls: usize,
            expected_gc_calls: usize,
        ) {
            assert_eq!(self.warm_calls(), expected_warm_calls);
            assert_eq!(self.gc_calls(), expected_gc_calls);
            assert_eq!(self.active_segment_ids(), expected_segment_ids);
        }
    }

    impl Warmer for TestWarmer {
        fn warm(&self, searcher: &crate::Searcher) -> crate::Result<()> {
            self.warm_calls.fetch_add(1, atomic::Ordering::SeqCst);
            for reader in searcher.segment_readers() {
                self.active_segment_ids
                    .write()
                    .unwrap()
                    .insert(reader.segment_id());
            }
            Ok(())
        }

        fn garbage_collect(&self, segment_ids: &HashSet<SegmentId>) {
            self.gc_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

    fn test_warming(num_warming_threads: usize) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field("pk", INDEXED);
        let schema = schema_builder.build();

        let directory = RamDirectory::create();
        let index = Index::create(directory.clone(), schema, IndexSettings::default())?;

        let num_writer_threads = 4;
        let mut writer = index
            .writer_with_num_threads(num_writer_threads, 25_000_000)
            .unwrap();

        for i in 0u64..1000u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;

        let warmer1 = Arc::new(TestWarmer::default());
        let warmer2 = Arc::new(TestWarmer::default());
        warmer1.verify(HashSet::new(), 0, 0);
        warmer2.verify(HashSet::new(), 0, 0);

        let num_searchers = 4;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .num_warming_threads(num_warming_threads)
            .num_searchers(num_searchers)
            .warmers(vec![
                Arc::downgrade(&warmer1) as Weak<dyn Warmer>,
                Arc::downgrade(&warmer2) as Weak<dyn Warmer>,
            ])
            .try_into()?;

        {
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), num_writer_threads);
            warmer1.verify(segment_ids(&searcher), 1, 1);
            warmer2.verify(segment_ids(&searcher), 1, 1);
            assert_eq!(searcher.num_docs(), 1000);
        }

        reader.reload()?;

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1000);
        warmer1.verify(segment_ids(&searcher), 2, 2);
        warmer2.verify(segment_ids(&searcher), 2, 2);

        for i in 1000u64..2000u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;
        writer.wait_merging_threads()?;

        drop(warmer1);

        let old_searcher = searcher;

        reader.reload()?;

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2000);

        warmer2.verify(
            segment_ids(&old_searcher)
                .union(&segment_ids(&searcher))
                .copied()
                .collect(),
            3,
            2,
        );

        drop(old_searcher);

        for _ in 0..num_searchers {
            // GC triggered when all old searchers are dropped by the pool
            let _ = reader.searcher();
        }
        warmer2.verify(segment_ids(&searcher), 3, 3);

        Ok(())
    }

    #[test]
    fn warming_single_thread() -> crate::Result<()> {
        test_warming(1)
    }

    #[test]
    fn warming_four_threads() -> crate::Result<()> {
        test_warming(4)
    }
}
