// # Custom collector example
//
// This example shows how you can implement your own
// collector. As an example, we will compute a collector
// that computes the standard deviation of a given fast field.
//
// Of course, you can have a look at the tantivy's built-in collectors
// such as the `CountCollector` for more examples.

// ---
// Importing tantivy...
use crate::collector::{Collector, SegmentCollector};
use crate::fastfield::FastFieldReader;
use crate::schema::Field;
use crate::{Score, SegmentReader, TantivyError};

/// The `FilterCollector` collector filters docs using a u64 fast field value and a predicate.
/// Only the documents for which the predicate returned "true" will be passed on to the next collector.
///
/// ```rust
/// use tantivy::collector::{TopDocs, FilterCollector};
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT, INDEXED, FAST};
/// use tantivy::{doc, DocAddress, Index};
///
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let price = schema_builder.add_u64_field("price", INDEXED | FAST);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
/// index_writer.add_document(doc!(title => "The Name of the Wind", price => 30_200u64));
/// index_writer.add_document(doc!(title => "The Diary of Muadib", price => 29_240u64));
/// index_writer.add_document(doc!(title => "A Dairy Cow", price => 21_240u64));
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl", price => 20_120u64));
/// assert!(index_writer.commit().is_ok());
///
/// let reader = index.reader().unwrap();
/// let searcher = reader.searcher();
///
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary").unwrap();
/// let no_filter_collector = FilterCollector::new(price, &|value| value > 20_120u64, TopDocs::with_limit(1));
/// let top_docs = searcher.search(&query, &no_filter_collector).unwrap();
///
/// assert_eq!(top_docs.len(), 1);
/// assert_eq!(top_docs[0].1, DocAddress(0, 1));
///
/// let filter_all_collector = FilterCollector::new(price, &|value| value < 5u64, TopDocs::with_limit(2));
/// let filtered_top_docs = searcher.search(&query, &filter_all_collector).unwrap();
///
/// assert_eq!(filtered_top_docs.len(), 0);
/// ```
pub struct FilterCollector<TCollector, TPredicate>
where
    TPredicate: 'static,
{
    field: Field,
    collector: TCollector,
    predicate: &'static TPredicate,
}

impl<TCollector, TPredicate> FilterCollector<TCollector, TPredicate>
where
    TCollector: Collector + Send + Sync,
    TPredicate: Fn(u64) -> bool + Send + Sync,
{
    /// Create a new FilterCollector.
    pub fn new(
        field: Field,
        predicate: &'static TPredicate,
        collector: TCollector,
    ) -> FilterCollector<TCollector, TPredicate> {
        FilterCollector {
            field,
            predicate,
            collector,
        }
    }
}

impl<TCollector, TPredicate> Collector for FilterCollector<TCollector, TPredicate>
where
    TCollector: Collector + Send + Sync,
    TPredicate: 'static + Fn(u64) -> bool + Send + Sync,
{
    // That's the type of our result.
    // Our standard deviation will be a float.
    type Fruit = TCollector::Fruit;

    type Child = FilterSegmentCollector<TCollector::Child, TPredicate>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<FilterSegmentCollector<TCollector::Child, TPredicate>> {
        let fast_field_reader = segment_reader
            .fast_fields()
            .u64(self.field)
            .ok_or_else(|| {
                let field_name = segment_reader.schema().get_field_name(self.field);
                TantivyError::SchemaError(format!(
                    "Field {:?} is not a u64 fast field.",
                    field_name
                ))
            })?;
        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;
        Ok(FilterSegmentCollector {
            fast_field_reader,
            segment_collector: segment_collector,
            predicate: self.predicate,
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<TCollector::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<TCollector::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct FilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TPredicate: 'static,
{
    fast_field_reader: FastFieldReader<u64>,
    segment_collector: TSegmentCollector,
    predicate: &'static TPredicate,
}

impl<TSegmentCollector, TPredicate> SegmentCollector
    for FilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TSegmentCollector: SegmentCollector,
    TPredicate: 'static + Fn(u64) -> bool + Send + Sync,
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        let value = self.fast_field_reader.get(doc);
        if (self.predicate)(value) {
            self.segment_collector.collect(doc, score)
        }
    }

    fn harvest(self) -> <TSegmentCollector as SegmentCollector>::Fruit {
        self.segment_collector.harvest()
    }
}
