use std::collections::BTreeMap;
#[cfg(feature = "quickwit")]
use std::future::Future;
use std::sync::Arc;
use std::{fmt, io};

use crate::collector::Collector;
use crate::core::{Executor, SegmentReader};
use crate::query::{Bm25StatisticsProvider, EnableScoring, Query};
use crate::schema::document::DocumentDeserialize;
use crate::schema::{Schema, Term};
use crate::space_usage::SearcherSpaceUsage;
use crate::store::{CacheStats, StoreReader};
use crate::{DocAddress, Index, Opstamp, SegmentId, TrackedObject};

/// Identifies the searcher generation accessed by a [`Searcher`].
///
/// While this might seem redundant, a [`SearcherGeneration`] contains
/// both a `generation_id` AND a list of `(SegmentId, DeleteOpstamp)`.
///
/// This is on purpose. This object is used by the [`Warmer`](crate::reader::Warmer) API.
/// Having both information makes it possible to identify which
/// artifact should be refreshed or garbage collected.
///
/// Depending on the use case, `Warmer`'s implementers can decide to
/// produce artifacts per:
/// - `generation_id` (e.g. some searcher level aggregates)
/// - `(segment_id, delete_opstamp)` (e.g. segment level aggregates)
/// - `segment_id` (e.g. for immutable document level information)
/// - `(generation_id, segment_id)` (e.g. for consistent dynamic column)
/// - ...
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearcherGeneration {
    segments: BTreeMap<SegmentId, Option<Opstamp>>,
    generation_id: u64,
}

impl SearcherGeneration {
    pub(crate) fn from_segment_readers(
        segment_readers: &[SegmentReader],
        generation_id: u64,
    ) -> Self {
        let mut segment_id_to_del_opstamp = BTreeMap::new();
        for segment_reader in segment_readers {
            segment_id_to_del_opstamp
                .insert(segment_reader.segment_id(), segment_reader.delete_opstamp());
        }
        Self {
            segments: segment_id_to_del_opstamp,
            generation_id,
        }
    }

    /// Returns the searcher generation id.
    pub fn generation_id(&self) -> u64 {
        self.generation_id
    }

    /// Return a `(SegmentId -> DeleteOpstamp)` mapping.
    pub fn segments(&self) -> &BTreeMap<SegmentId, Option<Opstamp>> {
        &self.segments
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
#[derive(Clone)]
pub struct Searcher {
    inner: Arc<SearcherInner>,
}

impl Searcher {
    /// Returns the `Index` associated with the `Searcher`
    pub fn index(&self) -> &Index {
        &self.inner.index
    }

    /// [`SearcherGeneration`] which identifies the version of the snapshot held by this `Searcher`.
    pub fn generation(&self) -> &SearcherGeneration {
        self.inner.generation.as_ref()
    }

    /// Fetches a document from tantivy's store given a [`DocAddress`].
    ///
    /// The searcher uses the segment ordinal to route the
    /// request to the right `Segment`.
    pub fn doc<D: DocumentDeserialize>(&self, doc_address: DocAddress) -> crate::Result<D> {
        let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
        store_reader.get(doc_address.doc_id)
    }

    /// The cache stats for the underlying store reader.
    ///
    /// Aggregates the sum for each segment store reader.
    pub fn doc_store_cache_stats(&self) -> CacheStats {
        let cache_stats: CacheStats = self
            .inner
            .store_readers
            .iter()
            .map(|reader| reader.cache_stats())
            .sum();
        cache_stats
    }

    /// Fetches a document in an asynchronous manner.
    #[cfg(feature = "quickwit")]
    pub async fn doc_async<D: DocumentDeserialize>(
        &self,
        doc_address: DocAddress,
    ) -> crate::Result<D> {
        let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
        store_reader.get_async(doc_address.doc_id).await
    }

    /// Fetches multiple documents in an asynchronous manner.
    ///
    /// This method is more efficient than calling [`doc_async`](Self::doc_async) multiple times,
    /// as it groups overlapping requests to segments and blocks and avoids concurrent requests
    /// trashing the caches of each other. However, it does so using intermediate data structures
    /// and independent block caches so it will be slower if documents from very few blocks are
    /// fetched which would have fit into the global block cache.
    ///
    /// The caller is expected to poll these futures concurrently (e.g. using `FuturesUnordered`)
    /// or in parallel (e.g. using `JoinSet`) as fits best with the given use case, i.e. whether
    /// it is predominately I/O-bound or rather CPU-bound.
    ///
    /// Note that any blocks brought into any of the per-segment-and-block groups will not be pulled
    /// into the global block cache and hence not be available for subsequent calls.
    ///
    /// Note that there is no synchronous variant of this method as the same degree of efficiency
    /// can be had by accessing documents in address order.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use futures::executor::block_on;
    /// # use futures::stream::{FuturesUnordered, StreamExt};
    /// #
    /// # use tantivy::schema::Schema;
    /// # use tantivy::{DocAddress, Index, TantivyDocument, TantivyError};
    /// #
    /// # let index = Index::create_in_ram(Schema::builder().build());
    /// # let searcher = index.reader()?.searcher();
    /// #
    /// # let doc_addresses = (0..10).map(|_| DocAddress::new(0, 0));
    /// #
    /// let mut groups: FuturesUnordered<_> = searcher
    ///     .docs_async::<TantivyDocument>(doc_addresses)?
    ///     .collect();
    ///
    /// let mut docs = Vec::new();
    ///
    /// block_on(async {
    ///     while let Some(group) = groups.next().await {
    ///         docs.extend(group?);
    ///     }
    ///
    ///     Ok::<_, TantivyError>(())
    /// })?;
    /// #
    /// # Ok::<_, TantivyError>(())
    /// ```
    #[cfg(feature = "quickwit")]
    pub fn docs_async<D: DocumentDeserialize>(
        &self,
        doc_addresses: impl IntoIterator<Item = DocAddress>,
    ) -> crate::Result<
        impl Iterator<Item = impl Future<Output = crate::Result<Vec<(DocAddress, D)>>>> + '_,
    > {
        use rustc_hash::FxHashMap;

        use crate::store::CacheKey;
        use crate::{DocId, SegmentOrdinal};

        let mut groups: FxHashMap<(SegmentOrdinal, CacheKey), Vec<DocId>> = Default::default();

        for doc_address in doc_addresses {
            let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
            let cache_key = store_reader.cache_key(doc_address.doc_id)?;

            groups
                .entry((doc_address.segment_ord, cache_key))
                .or_default()
                .push(doc_address.doc_id);
        }

        let futures = groups
            .into_iter()
            .map(|((segment_ord, cache_key), doc_ids)| {
                // Each group fetches documents from exactly one block and
                // therefore gets an independent block cache of size one.
                let store_reader =
                    self.inner.store_readers[segment_ord as usize].fork_cache(1, &[cache_key]);

                async move {
                    let mut docs = Vec::new();

                    for doc_id in doc_ids {
                        let doc = store_reader.get_async(doc_id).await?;

                        docs.push((
                            DocAddress {
                                segment_ord,
                                doc_id,
                            },
                            doc,
                        ));
                    }

                    Ok(docs)
                }
            });

        Ok(futures)
    }

    /// Access the schema associated with the index of this searcher.
    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }

    /// Returns the overall number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.inner
            .segment_readers
            .iter()
            .map(|segment_reader| u64::from(segment_reader.num_docs()))
            .sum::<u64>()
    }

    /// Return the overall number of documents containing
    /// the given term.
    pub fn doc_freq(&self, term: &Term) -> crate::Result<u64> {
        let mut total_doc_freq = 0;
        for segment_reader in &self.inner.segment_readers {
            let inverted_index = segment_reader.inverted_index(term.field())?;
            let doc_freq = inverted_index.doc_freq(term)?;
            total_doc_freq += u64::from(doc_freq);
        }
        Ok(total_doc_freq)
    }

    /// Return the overall number of documents containing
    /// the given term in an asynchronous manner.
    #[cfg(feature = "quickwit")]
    pub async fn doc_freq_async(&self, term: &Term) -> crate::Result<u64> {
        let mut total_doc_freq = 0;
        for segment_reader in &self.inner.segment_readers {
            let inverted_index = segment_reader.inverted_index(term.field())?;
            let doc_freq = inverted_index.doc_freq_async(term).await?;
            total_doc_freq += u64::from(doc_freq);
        }
        Ok(total_doc_freq)
    }

    /// Return the list of segment readers
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.inner.segment_readers
    }

    /// Returns the segment_reader associated with the given segment_ord
    pub fn segment_reader(&self, segment_ord: u32) -> &SegmentReader {
        &self.inner.segment_readers[segment_ord as usize]
    }

    /// Runs a query on the segment readers wrapped by the searcher.
    ///
    /// Search works as follows :
    ///
    ///  First the weight object associated with the query is created.
    ///
    ///  Then, the query loops over the segments and for each segment :
    ///  - setup the collector and informs it that the segment being processed has changed.
    ///  - creates a SegmentCollector for collecting documents associated with the segment
    ///  - creates a `Scorer` object associated for this segment
    ///  - iterate through the matched documents and push them to the segment collector.
    ///
    ///  Finally, the Collector merges each of the child collectors into itself for result usability
    ///  by the caller.
    pub fn search<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
    ) -> crate::Result<C::Fruit> {
        self.search_with_statistics_provider(query, collector, self)
    }

    /// Same as [`search(...)`](Searcher::search) but allows specifying
    /// a [Bm25StatisticsProvider].
    ///
    /// This can be used to adjust the statistics used in computing BM25
    /// scores.
    pub fn search_with_statistics_provider<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
        statistics_provider: &dyn Bm25StatisticsProvider,
    ) -> crate::Result<C::Fruit> {
        let enabled_scoring = if collector.requires_scoring() {
            EnableScoring::enabled_from_statistics_provider(statistics_provider, self)
        } else {
            EnableScoring::disabled_from_searcher(self)
        };
        let executor = self.inner.index.search_executor();
        self.search_with_executor(query, collector, executor, enabled_scoring)
    }

    /// Same as [`search(...)`](Searcher::search) but multithreaded.
    ///
    /// The current implementation is rather naive :
    /// multithreading is by splitting search into as many task
    /// as there are segments.
    ///
    /// It is powerless at making search faster if your index consists in
    /// one large segment.
    ///
    /// Also, keep in my multithreading a single query on several
    /// threads will not improve your throughput. It can actually
    /// hurt it. It will however, decrease the average response time.
    pub fn search_with_executor<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
        executor: &Executor,
        enabled_scoring: EnableScoring,
    ) -> crate::Result<C::Fruit> {
        let weight = query.weight(enabled_scoring)?;
        let segment_readers = self.segment_readers();
        let fruits = executor.map(
            |(segment_ord, segment_reader)| {
                collector.collect_segment(weight.as_ref(), segment_ord as u32, segment_reader)
            },
            segment_readers.iter().enumerate(),
        )?;
        collector.merge_fruits(fruits)
    }

    /// Summarize total space usage of this searcher.
    pub fn space_usage(&self) -> io::Result<SearcherSpaceUsage> {
        let mut space_usage = SearcherSpaceUsage::new();
        for segment_reader in self.segment_readers() {
            space_usage.add_segment(segment_reader.space_usage()?);
        }
        Ok(space_usage)
    }
}

impl From<Arc<SearcherInner>> for Searcher {
    fn from(inner: Arc<SearcherInner>) -> Self {
        Searcher { inner }
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
pub(crate) struct SearcherInner {
    schema: Schema,
    index: Index,
    segment_readers: Vec<SegmentReader>,
    store_readers: Vec<StoreReader>,
    generation: TrackedObject<SearcherGeneration>,
}

impl SearcherInner {
    /// Creates a new `Searcher`
    pub(crate) fn new(
        schema: Schema,
        index: Index,
        segment_readers: Vec<SegmentReader>,
        generation: TrackedObject<SearcherGeneration>,
        doc_store_cache_num_blocks: usize,
    ) -> io::Result<SearcherInner> {
        assert_eq!(
            &segment_readers
                .iter()
                .map(|reader| (reader.segment_id(), reader.delete_opstamp()))
                .collect::<BTreeMap<_, _>>(),
            generation.segments(),
            "Set of segments referenced by this Searcher and its SearcherGeneration must match"
        );
        let store_readers: Vec<StoreReader> = segment_readers
            .iter()
            .map(|segment_reader| segment_reader.get_store_reader(doc_store_cache_num_blocks))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(SearcherInner {
            schema,
            index,
            segment_readers,
            store_readers,
            generation,
        })
    }
}

impl fmt::Debug for Searcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segment_ids = self
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect::<Vec<_>>();
        write!(f, "Searcher({segment_ids:?})")
    }
}
