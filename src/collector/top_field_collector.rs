use super::Collector;
use collector::top_collector::TopCollector;
use collector::SegmentCollector;
use fastfield::FastFieldReader;
use fastfield::FastValue;
use schema::Field;
use DocAddress;
use std::fmt;
use Result;
use SegmentReader;
use SegmentLocalId;

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
///	        let mut top_collector = TopFieldCollector::with_limit(rating, 2);
///         let query_parser = QueryParser::for_index(&index, vec![title]);
///         let query = query_parser.parse_query("diary")?;
///         searcher.search(&*query, &mut top_collector).unwrap();
///
///         let score_docs: Vec<(u64, DocId)> = top_collector
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
pub struct TopFieldCollector<T: FastValue> {
    field: Field,
    collector: TopCollector<T>,
    fast_field: Option<FastFieldReader<T>>,
}

impl<T: FastValue> fmt::Debug for TopFieldCollector<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result     {
        write!(f, "TopFieldCollector({:?})", self.field)
    }
}


impl<T: FastValue + PartialOrd + Clone> TopFieldCollector<T> {
    /// Creates a top field collector, with a number of documents equal to "limit".
    ///
    /// The given field name must be a fast field, otherwise the collector have an error while
    /// collecting results.
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(field: Field, limit: usize) -> Self {
        TopFieldCollector {
            field,
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
    pub fn top_docs(&self) -> Vec<(T, DocAddress)> {
        self.collector.top_docs()
    }

    /// Return true iff at least K documents have gone through
    /// the collector.
    #[inline]
    pub fn at_capacity(&self) -> bool {
        self.collector.at_capacity()
    }
}

impl<T: FastValue + PartialOrd + Clone + 'static> Collector for TopFieldCollector<T> {

    type Child = Self;

    fn for_segment(&self, segment_local_id: SegmentLocalId, reader: &SegmentReader) -> Result<Self> {
        let collector = self.collector.for_segment(segment_local_id, reader)?;
        let fast_field = Some(reader.fast_field_reader(self.field)?);
        Ok(TopFieldCollector {
            field: self.field,
            collector,
            fast_field
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_children(&mut self, children: Vec<Self>) {
        let children = children.into_iter()
            .map(|child| child.collector)
            .collect::<Vec<_>>();
        self.collector.merge_children(children);
    }
}

impl<T: FastValue + PartialOrd + Clone + 'static> SegmentCollector for TopFieldCollector<T> {
    fn collect(&mut self, doc: u32, _score: f32) {
        let field_value = self
            .fast_field
            .as_ref()
            .expect("collect() was called before set_segment. This should never happen.")
            .get(doc);
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
    use collector::{SegmentCollector, Collector};

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

        let mut top_collector = TopFieldCollector::with_limit(size, 4);
        searcher.search(&*query, &mut top_collector).unwrap();
        assert!(!top_collector.at_capacity());

        let score_docs: Vec<(u64, DocId)> = top_collector
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
            top_collector.for_segment(0, segment),
            Err(TantivyError::FastFieldError(_))
        );
    }

    #[test]
    #[should_panic]
    fn test_collect_before_set_segment() {
        let mut top_collector: TopFieldCollector<u64> = TopFieldCollector::with_limit(Field(0), 4);
        top_collector.collect(0, 0f32);
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
