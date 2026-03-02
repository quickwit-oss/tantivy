use std::collections::BTreeMap;
use std::sync::Arc;
use std::{fmt, io};

use crate::collector::Collector;
use crate::core::Executor;
use crate::index::{Index, SegmentId, SegmentReader};
use crate::query::{Bm25StatisticsProvider, EnableScoring, Query};
use crate::schema::{Field, FieldType, Schema, TantivyDocument, Term};
use crate::space_usage::SearcherSpaceUsage;
use crate::store::{CacheStats, StoreReader, DOCSTORE_CACHE_CAPACITY};
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::{DocAddress, Inventory, Opstamp, TantivyError, TrackedObject};

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
        segment_readers: &[Arc<dyn SegmentReader>],
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

/// Search-time context required by a [`Searcher`].
#[derive(Clone)]
pub struct SearcherContext {
    schema: Schema,
    executor: Executor,
    tokenizers: TokenizerManager,
    fast_field_tokenizers: TokenizerManager,
}

impl SearcherContext {
    /// Creates a context from explicit search-time components.
    pub fn new(
        schema: Schema,
        executor: Executor,
        tokenizers: TokenizerManager,
        fast_field_tokenizers: TokenizerManager,
    ) -> SearcherContext {
        SearcherContext {
            schema,
            executor,
            tokenizers,
            fast_field_tokenizers,
        }
    }

    /// Creates a context from an index.
    pub fn from_index<C: crate::codec::Codec>(index: &Index<C>) -> SearcherContext {
        SearcherContext::new(
            index.schema(),
            index.search_executor().clone(),
            index.tokenizers().clone(),
            index.fast_field_tokenizer().clone(),
        )
    }

    /// Access the schema associated with this context.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Access the executor associated with this context.
    pub fn search_executor(&self) -> &Executor {
        &self.executor
    }

    /// Access the tokenizer manager associated with this context.
    pub fn tokenizers(&self) -> &TokenizerManager {
        &self.tokenizers
    }

    /// Access the fast field tokenizer manager associated with this context.
    pub fn fast_field_tokenizer(&self) -> &TokenizerManager {
        &self.fast_field_tokenizers
    }

    /// Get the tokenizer associated with a specific field.
    pub fn tokenizer_for_field(&self, field: Field) -> crate::Result<TextAnalyzer> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let indexing_options_opt = match field_type {
            FieldType::JsonObject(options) => options.get_text_indexing_options(),
            FieldType::Str(options) => options.get_indexing_options(),
            _ => {
                return Err(TantivyError::SchemaError(format!(
                    "{:?} is not a text field.",
                    field_entry.name()
                )))
            }
        };
        let indexing_options = indexing_options_opt.ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "No indexing options set for field {field_entry:?}"
            ))
        })?;

        self.tokenizers
            .get(indexing_options.tokenizer())
            .ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "No Tokenizer found for field {field_entry:?}"
                ))
            })
    }
}

impl<C: crate::codec::Codec> From<&Index<C>> for SearcherContext {
    fn from(index: &Index<C>) -> Self {
        SearcherContext::from_index(index)
    }
}

impl<C: crate::codec::Codec> From<Index<C>> for SearcherContext {
    fn from(index: Index<C>) -> Self {
        SearcherContext::from(&index)
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
    /// Creates a `Searcher` from an arbitrary list of segment readers.
    ///
    /// This is useful when segment readers are not opened from
    /// `IndexReader` / `meta.json` (e.g. external segment sources).
    /// The generated [`SearcherGeneration`] uses `generation_id = 0`.
    pub fn from_segment_readers<Ctx: Into<SearcherContext>>(
        context: Ctx,
        segment_readers: Vec<Arc<dyn SegmentReader>>,
    ) -> crate::Result<Searcher> {
        Self::from_segment_readers_with_generation_id(context, segment_readers, 0)
    }

    /// Same as [`Searcher::from_segment_readers`] but allows setting
    /// a custom generation id.
    pub fn from_segment_readers_with_generation_id<Ctx: Into<SearcherContext>>(
        context: Ctx,
        segment_readers: Vec<Arc<dyn SegmentReader>>,
        generation_id: u64,
    ) -> crate::Result<Searcher> {
        let context = context.into();
        let generation = SearcherGeneration::from_segment_readers(&segment_readers, generation_id);
        let tracked_generation = Inventory::default().track(generation);
        let inner = SearcherInner::new(
            context,
            segment_readers,
            tracked_generation,
            DOCSTORE_CACHE_CAPACITY,
        )?;
        Ok(Arc::new(inner).into())
    }

    /// Returns the search context associated with the `Searcher`.
    pub fn context(&self) -> &SearcherContext {
        &self.inner.context
    }

    /// Deprecated alias for [`Searcher::context`].
    #[deprecated(note = "use Searcher::context()")]
    pub fn index(&self) -> &SearcherContext {
        self.context()
    }

    /// Access the search executor associated with this searcher.
    pub fn search_executor(&self) -> &Executor {
        self.context().search_executor()
    }

    /// Access the tokenizer manager associated with this searcher.
    pub fn tokenizers(&self) -> &TokenizerManager {
        self.context().tokenizers()
    }

    /// Access the fast field tokenizer manager associated with this searcher.
    pub fn fast_field_tokenizer(&self) -> &TokenizerManager {
        self.context().fast_field_tokenizer()
    }

    /// Get the tokenizer associated with a specific field.
    pub fn tokenizer_for_field(&self, field: Field) -> crate::Result<TextAnalyzer> {
        self.context().tokenizer_for_field(field)
    }

    /// [`SearcherGeneration`] which identifies the version of the snapshot held by this `Searcher`.
    pub fn generation(&self) -> &SearcherGeneration {
        self.inner.generation.as_ref()
    }

    /// Fetches a document from tantivy's store given a [`DocAddress`].
    ///
    /// The searcher uses the segment ordinal to route the
    /// request to the right `Segment`.
    pub fn doc(&self, doc_address: DocAddress) -> crate::Result<TantivyDocument> {
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
    pub async fn doc_async(&self, doc_address: DocAddress) -> crate::Result<TantivyDocument> {
        let executor = self.search_executor();
        let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
        store_reader.get_async(doc_address.doc_id, executor).await
    }

    /// Access the schema associated with the index of this searcher.
    pub fn schema(&self) -> &Schema {
        self.context().schema()
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
    pub fn segment_readers(&self) -> &[Arc<dyn SegmentReader>] {
        &self.inner.segment_readers
    }

    /// Returns the segment_reader associated with the given segment_ord
    pub fn segment_reader(&self, segment_ord: u32) -> &dyn SegmentReader {
        self.inner.segment_readers[segment_ord as usize].as_ref()
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
        let executor = self.search_executor();
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
                collector.collect_segment(
                    weight.as_ref(),
                    segment_ord as u32,
                    segment_reader.as_ref(),
                )
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
    context: SearcherContext,
    segment_readers: Vec<Arc<dyn SegmentReader>>,
    store_readers: Vec<Box<dyn StoreReader>>,
    generation: TrackedObject<SearcherGeneration>,
}

impl SearcherInner {
    /// Creates a new `Searcher`
    pub(crate) fn new(
        context: SearcherContext,
        segment_readers: Vec<Arc<dyn SegmentReader>>,
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
        let store_readers: Vec<Box<dyn StoreReader>> = segment_readers
            .iter()
            .map(|segment_reader| segment_reader.get_store_reader(doc_store_cache_num_blocks))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(SearcherInner {
            context,
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
            .map(|segment_reader| segment_reader.segment_id())
            .collect::<Vec<_>>();
        write!(f, "Searcher({segment_ids:?})")
    }
}
