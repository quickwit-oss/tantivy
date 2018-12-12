use collector::Collector;
use collector::SegmentCollector;
use core::Executor;
use core::InvertedIndexReader;
use core::SegmentReader;
use query::Query;
use query::Scorer;
use query::Weight;
use schema::Document;
use schema::Schema;
use schema::{Field, Term};
use space_usage::SearcherSpaceUsage;
use std::fmt;
use std::sync::Arc;
use store::StoreReader;
use termdict::TermMerger;
use DocAddress;
use Index;
use Result;

fn collect_segment<C: Collector>(
    collector: &C,
    weight: &Weight,
    segment_ord: u32,
    segment_reader: &SegmentReader,
) -> Result<C::SegmentFruit> {
    let mut scorer = weight.scorer(segment_reader)?;
    let mut segment_collector = collector.for_segment(segment_ord as u32, segment_reader)?;
    if let Some(delete_bitset) = segment_reader.delete_bitset() {
        scorer.for_each(&mut |doc, score| {
            if !delete_bitset.is_deleted(doc) {
                segment_collector.collect(doc, score);
            }
        });
    } else {
        scorer.for_each(&mut |doc, score| segment_collector.collect(doc, score));
    }
    Ok(segment_collector.harvest())
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
}

impl Searcher {
    /// Creates a new `Searcher`
    pub(crate) fn new(
        schema: Schema,
        index: Index,
        segment_readers: Vec<SegmentReader>,
    ) -> Searcher {
        let store_readers = segment_readers
            .iter()
            .map(|segment_reader| segment_reader.get_store_reader())
            .collect();
        Searcher {
            schema,
            index,
            segment_readers,
            store_readers,
        }
    }

    /// Returns the `Index` associated to the `Searcher`
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// Fetches a document from tantivy's store given a `DocAddress`.
    ///
    /// The searcher uses the segment ordinal to route the
    /// the request to the right `Segment`.
    pub fn doc(&self, doc_address: DocAddress) -> Result<Document> {
        let DocAddress(segment_local_id, doc_id) = doc_address;
        let store_reader = &self.store_readers[segment_local_id as usize];
        store_reader.get(doc_id)
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
    pub fn doc_freq(&self, term: &Term) -> u64 {
        self.segment_readers
            .iter()
            .map(|segment_reader| {
                u64::from(segment_reader.inverted_index(term.field()).doc_freq(term))
            })
            .sum::<u64>()
    }

    /// Return the list of segment readers
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.segment_readers
    }

    /// Returns the segment_reader associated with the given segment_ordinal
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
    pub fn search<C: Collector>(&self, query: &Query, collector: &C) -> Result<C::Fruit> {
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
        query: &Query,
        collector: &C,
        executor: &Executor,
    ) -> Result<C::Fruit> {
        let scoring_enabled = collector.requires_scoring();
        let weight = query.weight(self, scoring_enabled)?;
        let segment_readers = self.segment_readers();
        let fruits = executor.map(
            |(segment_ord, segment_reader)| {
                collect_segment(
                    collector,
                    weight.as_ref(),
                    segment_ord as u32,
                    segment_reader,
                )
            },
            segment_readers.iter().enumerate(),
        )?;
        collector.merge_fruits(fruits)
    }

    /// Return the field searcher associated to a `Field`.
    pub fn field(&self, field: Field) -> FieldSearcher {
        let inv_index_readers = self
            .segment_readers
            .iter()
            .map(|segment_reader| segment_reader.inverted_index(field))
            .collect::<Vec<_>>();
        FieldSearcher::new(inv_index_readers)
    }

    /// Summarize total space usage of this searcher.
    pub fn space_usage(&self) -> SearcherSpaceUsage {
        let mut space_usage = SearcherSpaceUsage::new();
        for segment_reader in self.segment_readers.iter() {
            space_usage.add_segment(segment_reader.space_usage());
        }
        space_usage
    }
}

pub struct FieldSearcher {
    inv_index_readers: Vec<Arc<InvertedIndexReader>>,
}

impl FieldSearcher {
    fn new(inv_index_readers: Vec<Arc<InvertedIndexReader>>) -> FieldSearcher {
        FieldSearcher { inv_index_readers }
    }

    /// Returns a Stream over all of the sorted unique terms of
    /// for the given field.
    pub fn terms(&self) -> TermMerger {
        let term_streamers: Vec<_> = self
            .inv_index_readers
            .iter()
            .map(|inverted_index| inverted_index.terms().stream())
            .collect();
        TermMerger::new(term_streamers)
    }
}

impl fmt::Debug for Searcher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let segment_ids = self
            .segment_readers
            .iter()
            .map(|segment_reader| segment_reader.segment_id())
            .collect::<Vec<_>>();
        write!(f, "Searcher({:?})", segment_ids)
    }
}
