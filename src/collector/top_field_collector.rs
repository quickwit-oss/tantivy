use super::Collector;
use collector::top_collector::TopCollector;
use collector::{SegmentCollector, CollectDocScore};
use fastfield::FastFieldReader;
use fastfield::FastValue;
use schema::Field;
use Result;
use SegmentReader;
use SegmentLocalId;
use collector::top_collector::TopSegmentCollector;
use collector::top_collector::TopDocs;

/// The Top Field Collector keeps track of the K documents
/// sorted by a fast field in the index
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{SchemaBuilder, TEXT, FAST};
/// use tantivy::{Index, Result, DocId};
/// use tantivy::collector::TopFieldCollector;
/// use tantivy::query::QueryParser;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let rating = schema_builder.add_u64_field("rating", FAST);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer_with_num_threads(1, 3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///             rating => 92u64,
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///             rating => 97u64,
///         ));
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///             rating => 63u64,
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///             rating => 80u64,
///         ));
///         index_writer.commit().unwrap();
///     }
///
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     {
///	        let top_collector = TopFieldCollector::with_limit(rating, 2);
///         let query_parser = QueryParser::for_index(&index, vec![title]);
///         let query = query_parser.parse_query("diary")?;
///         let top_docs = searcher.search(&*query, top_collector).unwrap();
///
///         let score_docs: Vec<(u64, DocId)> = top_docs
///           .top_docs()
///           .into_iter()
///           .map(|(field, doc_address)| (field, doc_address.doc()))
///           .collect();
///
///         assert_eq!(score_docs, vec![(97u64, 1), (80, 3)]);
///     }
///
///     Ok(())
/// }
/// ```
pub struct TopFieldCollector<T> {
    collector: TopCollector<T>,
    field: Field
}

impl<T: FastValue + PartialOrd + Clone> TopFieldCollector<T> {
    /// Creates a top field collector, with a number of documents equal to "limit".
    ///
    /// The given field name must be a fast field, otherwise the collector have an error while
    /// collecting results.
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(field: Field, limit: usize) -> TopFieldCollector<T> {
        TopFieldCollector {
            collector: TopCollector::with_limit(limit),
            field
        }
    }
}


impl<T: FastValue + PartialOrd + Send + 'static> Collector for TopFieldCollector<T> {

    type Fruit = TopDocs<T>;

    type Child = TopFieldSegmentCollector<T>;

    fn for_segment(&self, segment_local_id: SegmentLocalId, reader: &SegmentReader) -> Result<TopFieldSegmentCollector<T>> {
        let collector = self.collector.for_segment(segment_local_id, reader)?;
        let reader = reader.fast_field_reader(self.field)?;
        Ok(TopFieldSegmentCollector { collector, reader })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<TopDocs<T>>) -> TopDocs<T> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct TopFieldSegmentCollector<T: FastValue + PartialOrd> {
    collector: TopSegmentCollector<T>,
    reader: FastFieldReader<T>,
}

impl<T: FastValue + PartialOrd + Send + 'static> SegmentCollector for TopFieldSegmentCollector<T> {

    type Fruit = TopDocs<T>;

    fn harvest(self) -> TopDocs<T> {
        self.collector.harvest()
    }
}

impl<T: FastValue + PartialOrd + Send + 'static> CollectDocScore for TopFieldSegmentCollector<T> {
    fn collect(&mut self, doc: u32, _score: f32) {
        let field_value = self.reader.get(doc);
        self.collector.collect(doc, field_value);
    }
}

#[cfg(test)]
mod tests {
    use super::TopFieldCollector;
    use query::Query;
    use query::QueryParser;
    use schema::Field;
    use schema::IntOptions;
    use schema::Schema;
    use schema::{SchemaBuilder, FAST, TEXT};
    use Index;
    use IndexWriter;
    use TantivyError;
    use DocId;
    use collector::Collector;

    const TITLE: &str = "title";
    const SIZE: &str = "size";

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut schema_builder = SchemaBuilder::new();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, query) = index("beer", title, schema, |index_writer| {
            index_writer.add_document(doc!(
                title => "bottle of beer",
                size => 12u64,
            ));
            index_writer.add_document(doc!(
                title => "growler of beer",
                size => 64u64,
            ));
            index_writer.add_document(doc!(
                title => "pint of beer",
                size => 16u64,
            ));
        });
        let searcher = index.searcher();

        let top_collector = TopFieldCollector::with_limit(size, 4);
        let top_docs = searcher.search(&*query, top_collector).unwrap();
        assert_eq!(top_docs.len(), 3);

        let score_docs: Vec<(u64, DocId)> = top_docs
            .top_docs()
            .into_iter()
            .map(|(field, doc_address)| (field, doc_address.doc()))
            .collect();
        assert_eq!(score_docs, vec![(64, 1), (16, 2), (12, 0)]);
    }

    #[test]
    #[should_panic]
    fn test_field_does_not_exist() {
        let mut schema_builder = SchemaBuilder::new();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, _) = index("beer", title, schema, |index_writer| {
            index_writer.add_document(doc!(
                title => "bottle of beer",
                size => 12u64,
            ));
        });
        let searcher = index.searcher();
        let top_collector: TopFieldCollector<u64> =
            TopFieldCollector::with_limit(Field(2), 4);
        let segment_reader = searcher.segment_reader(0u32);
        top_collector.for_segment(0, segment_reader)
                .expect("should panic");
    }

    #[test]
    fn test_field_not_fast_field() {
        let mut schema_builder = SchemaBuilder::new();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, IntOptions::default());
        let schema = schema_builder.build();
        let (index, _) = index("beer", title, schema, |index_writer| {
            index_writer.add_document(doc!(
                title => "bottle of beer",
                size => 12u64,
            ));
        });
        let searcher = index.searcher();
        let segment = searcher.segment_reader(0);
        let top_collector: TopFieldCollector<u64> = TopFieldCollector::with_limit(size, 4);
        assert_matches!(
            top_collector.for_segment(0, segment).map(|_| ()).unwrap_err(),
            TantivyError::FastFieldError(_)
        );
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        let _: TopFieldCollector<u64> = TopFieldCollector::with_limit(Field(0), 0);
    }

    fn index(
        query: &str,
        query_field: Field,
        schema: Schema,
        mut doc_adder: impl FnMut(&mut IndexWriter) -> (),
    ) -> (Index, Box<Query>) {
        let index = Index::create_in_ram(schema);

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        doc_adder(&mut index_writer);
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();

        let query_parser = QueryParser::for_index(&index, vec![query_field]);
        let query = query_parser.parse_query(query).unwrap();
        (index, query)
    }
}
