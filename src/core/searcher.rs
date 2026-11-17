use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::{fmt, io};

use crate::collector::Collector;
use crate::core::Executor;
use crate::index::{SegmentId, SegmentReader};
use crate::query::{Bm25StatisticsProvider, EnableScoring, Query};
use crate::schema::document::DocumentDeserialize;
use crate::schema::{Schema, Term};
use crate::space_usage::SearcherSpaceUsage;
use crate::store::{CacheStats, StoreReader};
use crate::{DocAddress, Index, Opstamp, TrackedObject};

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
        let executor = self.inner.index.search_executor();
        let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
        store_reader.get_async(doc_address.doc_id, executor).await
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
    /// Also, keep in mind multithreading a single query on several
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
        collector.check_schema(self.schema())?;
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
    store_readers: Vec<Arc<StoreReader>>,
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
        previous: Option<&SearcherInner>,
    ) -> io::Result<SearcherInner> {
        assert_eq!(
            &segment_readers
                .iter()
                .map(|reader| (reader.segment_id(), reader.delete_opstamp()))
                .collect::<BTreeMap<_, _>>(),
            generation.segments(),
            "Set of segments referenced by this Searcher and its SearcherGeneration must match"
        );
        // Store readers of segments that were already part of the previous searcher are
        // reused, so that their block caches survive reloads. Only segments still part of
        // the new searcher are carried over: readers of segments that went away are dropped
        // with the previous searcher, as before.
        let previous_store_readers: HashMap<SegmentId, &Arc<StoreReader>> = previous
            .map(|previous| {
                previous
                    .segment_readers
                    .iter()
                    .map(SegmentReader::segment_id)
                    .zip(previous.store_readers.iter())
                    .collect()
            })
            .unwrap_or_default();
        let store_readers: Vec<Arc<StoreReader>> = segment_readers
            .iter()
            .map(|segment_reader| {
                if let Some(store_reader) = previous_store_readers.get(&segment_reader.segment_id())
                {
                    return Ok(Arc::clone(store_reader));
                }
                segment_reader
                    .get_store_reader(doc_store_cache_num_blocks)
                    .map(Arc::new)
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(SearcherInner {
            schema,
            index,
            segment_readers,
            store_readers,
            generation,
        })
    }

    /// The segment readers of this searcher.
    pub(crate) fn segment_readers(&self) -> &[SegmentReader] {
        &self.segment_readers
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::collector::Count;
    use crate::query::TermQuery;
    use crate::schema::{IndexRecordOption, Schema, STORED, STRING};
    use crate::{Index, IndexWriter, ReloadPolicy, Term};

    #[test]
    fn test_reload_reuses_unchanged_segment_readers() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", STRING | STORED);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.add_document(doc!(text => "a"))?;
        writer.commit()?;
        reader.reload()?;
        let searcher1 = reader.searcher();
        assert_eq!(searcher1.segment_readers().len(), 1);
        let segment_a = searcher1.segment_readers()[0].segment_id();

        writer.add_document(doc!(text => "b"))?;
        writer.commit()?;
        reader.reload()?;
        let searcher2 = reader.searcher();
        assert_eq!(searcher2.segment_readers().len(), 2);
        let ord_a = searcher2
            .segment_readers()
            .iter()
            .position(|segment_reader| segment_reader.segment_id() == segment_a)
            .unwrap();
        // The unchanged segment's store reader (and its block cache) is shared.
        assert!(Arc::ptr_eq(
            &searcher1.inner.store_readers[0],
            &searcher2.inner.store_readers[ord_a]
        ));
        assert_eq!(searcher2.num_docs(), 2);

        // Deleting a document changes the delete opstamp: the segment reader must be
        // reopened so that the new alive bitset is taken into account.
        writer.delete_term(Term::from_field_text(text, "a"));
        writer.commit()?;
        reader.reload()?;
        let searcher3 = reader.searcher();
        assert_eq!(searcher3.num_docs(), 1);
        let query = TermQuery::new(Term::from_field_text(text, "a"), IndexRecordOption::Basic);
        assert_eq!(searcher3.search(&query, &Count)?, 0);
        // The older searchers are unaffected.
        assert_eq!(searcher2.search(&query, &Count)?, 1);
        assert_eq!(searcher2.num_docs(), 2);

        // Segments that go away are not carried over.
        let segment_ids = index.searchable_segment_ids()?;
        writer.merge(&segment_ids).wait()?;
        writer.wait_merging_threads()?;
        reader.reload()?;
        let searcher4 = reader.searcher();
        assert_eq!(searcher4.segment_readers().len(), 1);
        assert_eq!(searcher4.inner.store_readers.len(), 1);
        assert_eq!(searcher4.num_docs(), 1);
        let doc: crate::TantivyDocument = searcher4.doc(crate::DocAddress::new(0, 0))?;
        assert_eq!(
            crate::schema::Value::as_str(&doc.get_first(text).unwrap()),
            Some("b")
        );
        Ok(())
    }
}
