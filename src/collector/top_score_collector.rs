use super::Collector;
use collector::top_collector::TopSegmentCollector;
use DocAddress;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use collector::SegmentCollector;
use collector::CollectDocScore;
use collector::top_collector::ComparableDoc;
use std::collections::BinaryHeap;
use collector::top_collector::TopDocs;
use collector::top_collector::TopCollector;

/// The Top Score Collector keeps track of the K documents
/// sorted by their score.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{SchemaBuilder, TEXT};
/// use tantivy::{Index, Result, DocId, Score};
/// use tantivy::collector::TopScoreCollector;
/// use tantivy::query::QueryParser;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer_with_num_threads(1, 3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ));
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ));
///         index_writer.commit().unwrap();
///     }
///
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     {
///	        let mut top_collector = TopScoreCollector::with_limit(2);
///         let query_parser = QueryParser::for_index(&index, vec![title]);
///         let query = query_parser.parse_query("diary")?;
///         searcher.search(&*query, &mut top_collector).unwrap();
///
///         let score_docs: Vec<(Score, DocId)> = top_collector
///           .top_docs()
///           .into_iter()
///           .map(|(score, doc_address)| (score, doc_address.doc()))
///           .collect();
///
///         assert_eq!(score_docs, vec![(0.7261542, 1), (0.6099695, 3)]);
///     }
///
///     Ok(())
/// }
/// ```
pub struct TopScoreCollector(TopCollector<Score>);


impl TopScoreCollector {
    /// Creates a top score collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopScoreCollector {
        TopScoreCollector(TopCollector::with_limit(limit))
    }

// / Return true iff at least K documents have gone through
// / the collector.
//    #[inline]
//    pub fn at_capacity(&self) -> bool {
//        self.collector.at_capacity()
//    }
}


pub struct TopScoreDocs(TopDocs<Score>);

impl TopScoreDocs {

    /// Returns K best scored documents sorted in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn docs(&self) -> Vec<DocAddress> {
        self.0.docs()
    }

    /// Returns K best ScoredDocuments sorted in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn top_docs(&self) -> Vec<(Score, DocAddress)> {
        self.0.top_docs()
    }

}

pub struct TopScoreSegmentCollector(TopSegmentCollector<Score>);

impl SegmentCollector for TopScoreSegmentCollector {
    type Fruit = TopScoreDocs;

    fn harvest(self) -> TopScoreDocs {
        TopScoreDocs(self.0.harvest())
    }
}

impl CollectDocScore for TopScoreSegmentCollector {
    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score)
    }
}



impl Collector for TopScoreCollector {

    type Fruit = TopScoreDocs;
    type Child = TopScoreSegmentCollector;

    fn for_segment(&self, segment_local_id: SegmentLocalId, reader: &SegmentReader) -> Result<Self::Child> {
        let collector = self.0.for_segment(segment_local_id, reader)?;
        Ok(TopScoreSegmentCollector(collector))
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<TopScoreDocs>) -> Self::Fruit {
        let children = children.into_iter()
            .map(|top_score_docs| top_score_docs.0)
            .collect();
        TopScoreDocs(self.0.merge_fruits(children))
    }
}


#[cfg(test)]
mod tests {
    // TODO fix tests

    use super::{TopScoreCollector, TopScoreSegmentCollector};
    use collector::SegmentCollector;
    use DocId;
    use Score;

    /*

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopScoreSegmentCollector::with_limit(4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        let score_docs: Vec<(Score, DocId)> = top_collector
            .harvest()
            .top_docs()
            .into_iter()
            .map(|(score, doc_address)| (score, doc_address.doc()))
            .collect();
        assert_eq!(score_docs, vec![(0.8, 1), (0.3, 5), (0.2, 3)]);
    }


    #[test]
    fn test_top_collector_at_capacity() {
        let mut top_collector = TopScoreSegmentCollector::with_limit(4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        top_collector.collect(7, 0.9);
        top_collector.collect(9, -0.2);
        let top_docs = top_collector.harvest();
        {
            let score_docs: Vec<(Score, DocId)> = top_docs
                .top_docs()
                .into_iter()
                .map(|(score, doc_address)| (score, doc_address.doc()))
                .collect();
            assert_eq!(score_docs, vec![(0.9, 7), (0.8, 1), (0.3, 5), (0.2, 3)]);
        }
        {
            let docs: Vec<DocId> = top_docs
                .docs()
                .into_iter()
                .map(|doc_address| doc_address.doc())
                .collect();
            assert_eq!(docs, vec![7, 1, 5, 3]);
        }
    }
    */

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopScoreCollector::with_limit(0);
    }

}

