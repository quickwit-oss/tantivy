use super::Collector;
use collector::top_collector::TopCollector;
use collector::top_collector::TopSegmentCollector;
use collector::SegmentCollector;
use collector::TopDocsByField;
use fastfield::FastValue;
use schema::Field;
use DocAddress;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;

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
/// use tantivy::DocAddress;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{Index, Result};
/// use tantivy::collector::TopDocs;
/// use tantivy::query::QueryParser;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = Schema::builder();
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
///     let query_parser = QueryParser::for_index(&index, vec![title]);
///     let query = query_parser.parse_query("diary")?;
///     let top_docs = searcher.search(&query, &TopDocs::with_limit(2))?;
///
///     assert_eq!(&top_docs[0], &(0.7261542, DocAddress(0, 1)));
///     assert_eq!(&top_docs[1], &(0.6099695, DocAddress(0, 3)));
///
///     Ok(())
/// }
/// ```
pub struct TopDocs(TopCollector<Score>);

impl TopDocs {
    /// Creates a top score collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopDocs {
        TopDocs(TopCollector::with_limit(limit))
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// (By default, `TopDocs` collects the top-K documents sorted by
    /// the similarity score.)
    pub fn order_by_field<T: PartialOrd + FastValue + Clone>(
        self,
        field: Field,
    ) -> TopDocsByField<T> {
        TopDocsByField::new(field, self.0.limit())
    }
}

impl Collector for TopDocs {
    type Fruit = Vec<(Score, DocAddress)>;

    type Child = TopScoreSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let collector = self.0.for_segment(segment_local_id, reader)?;
        Ok(TopScoreSegmentCollector(collector))
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, child_fruits: Vec<Vec<(Score, DocAddress)>>) -> Result<Self::Fruit> {
        self.0.merge_fruits(child_fruits)
    }
}

/// Segment Collector associated to `TopDocs`.
pub struct TopScoreSegmentCollector(TopSegmentCollector<Score>);

impl SegmentCollector for TopScoreSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score)
    }

    fn harvest(self) -> Vec<(Score, DocAddress)> {
        self.0.harvest()
    }
}

#[cfg(test)]
mod tests {
    use super::TopDocs;
    use query::QueryParser;
    use schema::Schema;
    use schema::TEXT;
    use DocAddress;
    use Index;
    use Score;

    fn make_index() -> Index {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer.add_document(doc!(text_field=>"Hello happy tax payer."));
            index_writer.add_document(doc!(text_field=>"Droopy says hello happy tax payer"));
            index_writer.add_document(doc!(text_field=>"I like Droopy"));
            assert!(index_writer.commit().is_ok());
        }
        index.load_searchers().unwrap();
        index
    }

    #[test]
    fn test_top_collector_not_at_capacity() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4))
            .unwrap();
        assert_eq!(
            score_docs,
            vec![
                (0.81221175, DocAddress(0u32, 1)),
                (0.5376842, DocAddress(0u32, 2)),
                (0.48527452, DocAddress(0, 0))
            ]
        );
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .searcher()
            .search(&text_query, &TopDocs::with_limit(2))
            .unwrap();
        assert_eq!(
            score_docs,
            vec![
                (0.81221175, DocAddress(0u32, 1)),
                (0.5376842, DocAddress(0u32, 2)),
            ]
        );
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopDocs::with_limit(0);
    }

}
