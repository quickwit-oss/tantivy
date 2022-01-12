use std::{
    collections::HashSet,
    iter::FromIterator,
    sync::{Arc, Mutex, Weak},
    thread::JoinHandle,
    time::Duration,
};

use crate::{Executor, Searcher, SegmentId, TantivyError};

pub const GC_INTERVAL: Duration = Duration::from_secs(1);

/// `Warmer` can be used to maintain segment-level state e.g. caches.
///
/// They must be registered with the [IndexReaderBuilder].
pub trait Warmer: Sync + Send {
    /// Perform any warming work using the provided [Searcher].
    fn warm(&self, searcher: &Searcher) -> crate::Result<()>;

    /// Discard internal state for any [SegmentId] not provided.
    fn garbage_collect(&self, live_segment_ids: &HashSet<SegmentId>);
}

/// Warming-related state with interior mutability.
#[derive(Clone)]
pub(crate) struct WarmingState(Arc<Mutex<WarmingStateInner>>);

impl WarmingState {
    pub fn new(num_warming_threads: usize, warmers: Vec<Weak<dyn Warmer>>) -> crate::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(WarmingStateInner {
            num_warming_threads,
            warmers,
            searcher_generations: Vec::new(),
            gc_thread: None,
        }))))
    }

    /// Start tracking a new generation of searchers, and [Warmer::warm] it if there are active warmers.
    ///
    /// A background GC thread for [Warmer::garbage_collect] calls is uniquely created if there are active warmers.
    pub fn new_searcher_generation(
        &self,
        liveness_token: Weak<()>,
        searcher: &Searcher,
    ) -> crate::Result<()> {
        self.0
            .lock()
            .unwrap()
            .new_searcher_generation(liveness_token, searcher, &self.0)
    }

    #[cfg(test)]
    fn gc_maybe(&self) -> bool {
        self.0.lock().unwrap().gc_maybe()
    }
}

#[derive(Debug, Clone)]
struct SearcherGeneration {
    liveness_token: Weak<()>,
    segment_ids: Vec<SegmentId>,
}

impl SearcherGeneration {
    fn is_live(&self) -> bool {
        self.liveness_token.strong_count() > 0
    }
}

#[derive(Default)]
struct WarmingStateInner {
    num_warming_threads: usize,
    warmers: Vec<Weak<dyn Warmer>>,
    searcher_generations: Vec<SearcherGeneration>,
    gc_thread: Option<JoinHandle<()>>,
}

impl WarmingStateInner {
    /// Start tracking provided searcher's segment IDs with its liveness token.
    /// If there are active warmers, warm them with the provided searcher, and kick background GC thread if it has not yet been kicked.
    /// Otherwise, prune state for dropped searcher generations inline.
    fn new_searcher_generation(
        &mut self,
        liveness_token: Weak<()>,
        searcher: &Searcher,
        this: &Arc<Mutex<Self>>,
    ) -> crate::Result<()> {
        self.searcher_generations.push(SearcherGeneration {
            liveness_token,
            segment_ids: segment_ids(searcher),
        });
        let warmers = self.pruned_warmers();
        // Avoid threads (warming as well as background GC) if there are no warmers
        if warmers.is_empty() {
            self.prune_searcher_generations();
        } else {
            self.start_gc_thread_maybe(this)?;
            warming_executor(self.num_warming_threads.min(warmers.len()))?
                .map(|warmer| warmer.warm(searcher), warmers.into_iter())?;
        }
        Ok(())
    }

    /// Attempt to upgrade the weak Warmer references, pruning those which cannot be upgraded.
    /// Return the strong references.
    fn pruned_warmers(&mut self) -> Vec<Arc<dyn Warmer>> {
        let strong_warmers = self
            .warmers
            .iter()
            .flat_map(|weak_warmer| weak_warmer.upgrade())
            .collect::<Vec<_>>();
        self.warmers = strong_warmers.iter().map(Arc::downgrade).collect();
        strong_warmers
    }

    /// Prune dropped searcher generations from our state.
    /// Return count of removed generations.
    fn prune_searcher_generations(&mut self) -> usize {
        // This can be implemented neatly using Vec::drain_filter() when that is stable.
        let mut pruned = 0;
        let mut i = 0;
        while i < self.searcher_generations.len() {
            if !self.searcher_generations[i].is_live() {
                self.searcher_generations.remove(i);
                pruned += 1;
            } else {
                i += 1;
            }
        }
        pruned
    }

    /// Segment ID set of live searcher generations.
    fn live_segment_ids(&self) -> HashSet<SegmentId> {
        self.searcher_generations
            .iter()
            .filter(|gen| gen.is_live())
            .flat_map(|gen| gen.segment_ids.iter().copied())
            .collect()
    }

    /// [Warmer::garbage_collect] active warmers if some searcher generation is observed to have been dropped.
    fn gc_maybe(&mut self) -> bool {
        if self.prune_searcher_generations() > 0 {
            let live_segment_ids = self.live_segment_ids();
            for warmer in self.pruned_warmers() {
                warmer.garbage_collect(&live_segment_ids);
            }
            true
        } else {
            false
        }
    }

    /// Start GC thread if one has not already been started.
    fn start_gc_thread_maybe(&mut self, this: &Arc<Mutex<Self>>) -> crate::Result<bool> {
        if self.gc_thread.is_some() {
            return Ok(false);
        }
        let weak_inner = Arc::downgrade(this);
        let handle = std::thread::Builder::new()
            .name("tantivy-warm-gc".to_owned())
            .spawn(|| Self::gc_loop(weak_inner))
            .map_err(|_| {
               TantivyError::SystemError("Failed to spawn warmer GC thread".to_owned())
             })?;
        self.gc_thread = Some(handle);
        Ok(true)
    }

    /// Every [GC_INTERVAL] attempt to GC, with panics caught and logged using [std::panic::catch_unwind].
    fn gc_loop(inner: Weak<Mutex<WarmingStateInner>>) {
        for _ in crossbeam::channel::tick(GC_INTERVAL) {
            if let Some(inner) = inner.upgrade() {
                // rely on deterministic gc in tests
                #[cfg(not(test))]
                if let Err(err) = std::panic::catch_unwind(|| inner.lock().unwrap().gc_maybe()) {
                    error!("Panic in Warmer GC {:?}", err);
                }
                // avoid unused var warning in tests
                #[cfg(test)]
                drop(inner);
            }
        }
    }
}

fn warming_executor(num_threads: usize) -> crate::Result<Executor> {
    if num_threads <= 1 {
        Ok(Executor::single_thread())
    } else {
        Executor::multi_thread(num_threads, "tantivy-warm-")
    }
}

fn segment_ids<T>(searcher: &Searcher) -> T
where
    T: FromIterator<SegmentId>,
{
    searcher
        .segment_readers()
        .iter()
        .map(|reader| reader.segment_id())
        .collect::<T>()
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
        reader::warming::segment_ids,
        schema::{Schema, INDEXED},
        Index, IndexSettings, ReloadPolicy, SegmentId,
    };

    use super::Warmer;

    #[derive(Default)]
    struct TestWarmer {
        active_segment_ids: RwLock<HashSet<SegmentId>>,
        warm_calls: AtomicUsize,
        gc_calls: AtomicUsize,
    }

    impl TestWarmer {
        fn live_segment_ids(&self) -> HashSet<SegmentId> {
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
            expected_warm_calls: usize,
            expected_gc_calls: usize,
            expected_segment_ids: HashSet<SegmentId>,
        ) {
            assert_eq!(self.warm_calls(), expected_warm_calls);
            assert_eq!(self.gc_calls(), expected_gc_calls);
            assert_eq!(self.live_segment_ids(), expected_segment_ids);
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
        warmer1.verify(0, 0, HashSet::new());
        warmer2.verify(0, 0, HashSet::new());

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

        let warming_state = &reader.inner.warming_state;

        {
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), num_writer_threads);
            assert!(
                !warming_state.gc_maybe(),
                "no GC after first searcher generation"
            );
            warmer1.verify(1, 0, segment_ids(&searcher));
            warmer2.verify(1, 0, segment_ids(&searcher));
            assert_eq!(searcher.num_docs(), 1000);
        }

        reader.reload()?;
        warming_state.gc_maybe();

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1000);
        warmer1.verify(2, 1, segment_ids(&searcher));
        warmer2.verify(2, 1, segment_ids(&searcher));

        for i in 1000u64..2000u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;
        writer.wait_merging_threads()?;

        drop(warmer1);

        let old_searcher = searcher;

        reader.reload()?;
        assert!(!warming_state.gc_maybe(), "old searcher still around");

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2000);

        warmer2.verify(
            3,
            1,
            segment_ids::<HashSet<_>>(&old_searcher)
                .union(&segment_ids(&searcher))
                .copied()
                .collect(),
        );

        drop(old_searcher);
        for _ in 0..num_searchers {
            // make sure the old searcher is dropped by the pool too
            let _ = reader.searcher();
        }
        assert!(warming_state.gc_maybe(), "old searcher dropped");

        warmer2.verify(3, 2, segment_ids(&searcher));

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
