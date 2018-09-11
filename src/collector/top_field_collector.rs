use collector::TopCollector;
use DocAddress;
use DocId;
use fastfield::FastFieldReader;
use fastfield::FastValue;
use Result;
use Score;
use SegmentReader;
use super::Collector;
use TantivyError;

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
///	        let mut top_collector = TopFieldCollector::with_limit("rating", 2);
///         let query_parser = QueryParser::for_index(&index, vec![title]);
///         let query = query_parser.parse_query("diary")?;
///         searcher.search(&*query, &mut top_collector).unwrap();
///
///         let score_docs: Vec<(u64, DocId)> = top_collector
///           .field_docs()
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
pub struct TopFieldCollector<T: FastValue> {
    field_name: String,
    collector: TopCollector<T>,
    fast_field: Option<FastFieldReader<T>>,
}

impl<T: FastValue + PartialOrd + Clone> TopFieldCollector<T> {
    /// Creates a top field collector, with a number of documents equal to "limit".
    ///
    /// The given field name must be a fast field, otherwise the collector have an error while
    /// collecting results.
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(field_name: impl Into<String>, limit: usize) -> Self {
        TopFieldCollector {
            field_name: field_name.into(),
            collector: TopCollector::with_limit(limit),
            fast_field: None,
        }
    }

    /// Returns K best documents sorted the given field name in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn docs(&self) -> Vec<DocAddress> {
        self.collector.docs()
    }

    /// Returns K best FieldDocuments sorted in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn field_docs(&self) -> Vec<(T, DocAddress)> {
        self.collector.feature_docs()
    }

    /// Return true iff at least K documents have gone through
    /// the collector.
    #[inline]
    pub fn at_capacity(&self) -> bool {
        self.collector.at_capacity()
    }
}

impl<T: FastValue + PartialOrd + Clone> Collector for TopFieldCollector<T> {
    fn set_segment(&mut self, segment_id: u32, segment: &SegmentReader) -> Result<()> {
        self.collector.set_segment_id(segment_id);
        let field = segment.schema().get_field(&self.field_name).ok_or(
            TantivyError::InvalidArgument("Could not find field.".to_owned()),
        )?;
        self.fast_field = Some(segment.fast_field_reader(field)?);
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        let field_value = self
            .fast_field
            .as_ref()
            .expect("collect() was called before set_segment. This should never happen.")
            .get(doc);
        self.collector.collect_feature(doc, field_value);
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use Index;
    use IndexWriter;
    use query::Query;
    use query::QueryParser;
    use schema::{FAST, SchemaBuilder, TEXT};
    use schema::Field;
    use schema::IntOptions;
    use schema::Schema;
    use super::*;

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

        let mut top_collector = TopFieldCollector::with_limit(SIZE, 4);
        searcher.search(&*query, &mut top_collector).unwrap();
        assert!(!top_collector.at_capacity());

        let score_docs: Vec<(u64, DocId)> = top_collector
            .field_docs()
            .into_iter()
            .map(|(field, doc_address)| (field, doc_address.doc()))
            .collect();
        assert_eq!(score_docs, vec![(64, 1), (16, 2), (12, 0)]);
    }

    #[test]
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
        let segment = searcher.segment_reader(0);
        let mut top_collector: TopFieldCollector<u64> = TopFieldCollector::with_limit("unknown", 4);
        assert_matches!(
            top_collector.set_segment(0, segment),
            Err(TantivyError::InvalidArgument(_))
        );
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
        let mut top_collector: TopFieldCollector<u64> = TopFieldCollector::with_limit(SIZE, 4);
        assert_matches!(
            top_collector.set_segment(0, segment),
            Err(TantivyError::FastFieldError(_))
        );
    }

    #[test]
    #[should_panic]
    fn test_collect_before_set_segment() {
        let mut top_collector: TopFieldCollector<u64> = TopFieldCollector::with_limit(SIZE, 4);
        top_collector.collect(0, 0f32);
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        let _: TopFieldCollector<u64> = TopFieldCollector::with_limit(SIZE, 0);
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
