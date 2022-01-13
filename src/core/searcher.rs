use crate::collector::Collector;
use crate::core::Executor;
use crate::SegmentId;

use crate::core::SegmentReader;
use crate::query::Query;
use crate::schema::Document;
use crate::schema::Schema;
use crate::schema::Term;
use crate::space_usage::SearcherSpaceUsage;
use crate::store::StoreReader;
use crate::DocAddress;
use crate::Index;

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Weak;
use std::{fmt, io};

/// Identifies the index generation accessed by a [Searcher].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearcherIndexGeneration(BTreeMap<SegmentId, u32>);

impl SearcherIndexGeneration {
    pub(crate) fn from_segment_readers(readers: &[SegmentReader]) -> Self {
        let mut segment_id_to_del_docs = BTreeMap::new();
        for reader in readers {
            segment_id_to_del_docs.insert(reader.segment_id(), reader.num_deleted_docs());
        }
        Self(segment_id_to_del_docs)
    }

    /// Segment IDs represented in this generation.
    pub fn segment_ids(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.0.keys().copied()
    }

    /// Number of deleted documents for the specified segment ID if it is present in this generation.
    pub fn num_deleted_docs(&self, segment_id: &SegmentId) -> Option<u32> {
        self.0.get(segment_id).copied()
    }
}

/// A token that is coupled with the lifetime of a generation of [Searcher].
#[derive(Debug, Clone)]
pub struct SearcherGenerationToken(Weak<SearcherIndexGeneration>);

impl SearcherGenerationToken {
    /// Whether this generation of [Searcher] is still alive.
    pub fn is_live(&self) -> bool {
        self.0.strong_count() > 0
    }

    /// Access the [SearcherIndexGeneration] if this generation of [Searcher] is still alive.
    pub fn index_generation(&self) -> Option<SearcherIndexGeneration> {
        Weak::upgrade(&self.0).map(|gen| gen.deref().clone())
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
///
pub struct Searcher {
    schema: Schema,
    index: Index,
    segment_readers: Vec<SegmentReader>,
    store_readers: Vec<StoreReader>,
    index_generation: Arc<SearcherIndexGeneration>,
}

impl Searcher {
    /// Creates a new `Searcher`
    pub(crate) fn new(
        schema: Schema,
        index: Index,
        segment_readers: Vec<SegmentReader>,
        index_generation: Arc<SearcherIndexGeneration>,
    ) -> io::Result<Searcher> {
        let store_readers: Vec<StoreReader> = segment_readers
            .iter()
            .map(SegmentReader::get_store_reader)
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Searcher {
            schema,
            index,
            segment_readers,
            store_readers,
            index_generation,
        })
    }

    /// Returns the `Index` associated to the `Searcher`
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// [SearcherIndexGeneration] which identifies the index data accessed by this generation of `Searcher`.
    pub fn index_generation(&self) -> &SearcherIndexGeneration {
        &self.index_generation
    }

    /// [SearcherGenerationToken] which is coupled with the lifetime of this generation of `Searcher`.
    pub fn generation_token(&self) -> SearcherGenerationToken {
        SearcherGenerationToken(Arc::downgrade(&self.index_generation))
    }

    /// Fetches a document from tantivy's store given a `DocAddress`.
    ///
    /// The searcher uses the segment ordinal to route the
    /// the request to the right `Segment`.
    pub fn doc(&self, doc_address: DocAddress) -> crate::Result<Document> {
        let store_reader = &self.store_readers[doc_address.segment_ord as usize];
        store_reader.get(doc_address.doc_id)
    }

    /// Access the schema associated to the index of this searcher.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the overall number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.segment_readers
            .iter()
            .map(|segment_reader| u64::from(segment_reader.num_docs()))
            .sum::<u64>()
    }

    /// Return the overall number of documents containing
    /// the given term.
    pub fn doc_freq(&self, term: &Term) -> crate::Result<u64> {
        let mut total_doc_freq = 0;
        for segment_reader in &self.segment_readers {
            let inverted_index = segment_reader.inverted_index(term.field())?;
            let doc_freq = inverted_index.doc_freq(term)?;
            total_doc_freq += u64::from(doc_freq);
        }
        Ok(total_doc_freq)
    }

    /// Return the list of segment readers
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.segment_readers
    }

    /// Returns the segment_reader associated with the given segment_ord
    pub fn segment_reader(&self, segment_ord: u32) -> &SegmentReader {
        &self.segment_readers[segment_ord as usize]
    }

    /// Runs a query on the segment readers wrapped by the searcher.
    ///
    /// Search works as follows :
    ///
    ///  First the weight object associated to the query is created.
    ///
    ///  Then, the query loops over the segments and for each segment :
    ///  - setup the collector and informs it that the segment being processed has changed.
    ///  - creates a SegmentCollector for collecting documents associated to the segment
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
        let executor = self.index.search_executor();
        self.search_with_executor(query, collector, executor)
    }

    /// Same as [`search(...)`](#method.search) but multithreaded.
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
    ) -> crate::Result<C::Fruit> {
        let scoring_enabled = collector.requires_scoring();
        let weight = query.weight(self, scoring_enabled)?;
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
        for segment_reader in &self.segment_readers {
            space_usage.add_segment(segment_reader.space_usage()?);
        }
        Ok(space_usage)
    }
}

impl fmt::Debug for Searcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segment_ids = self
            .segment_readers
            .iter()
            .map(SegmentReader::segment_id)
            .collect::<Vec<_>>();
        write!(f, "Searcher({:?})", segment_ids)
    }
}
