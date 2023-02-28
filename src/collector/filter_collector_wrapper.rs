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
use std::marker::PhantomData;
use std::sync::Arc;

use columnar::{ColumnValues, DynamicColumn, HasAssociatedColumnType};

use crate::collector::{Collector, SegmentCollector};
use crate::schema::Field;
use crate::{Score, SegmentReader, TantivyError};

/// The `FilterCollector` filters docs using a fast field value and a predicate.
/// Only the documents for which the predicate returned "true" will be passed on to the next
/// collector.
///
/// ```rust
/// use tantivy::collector::{TopDocs, FilterCollector};
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT, INDEXED, FAST};
/// use tantivy::{doc, DocAddress, Index};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let price = schema_builder.add_u64_field("price", INDEXED | FAST);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
/// index_writer.add_document(doc!(title => "The Name of the Wind", price => 30_200u64))?;
/// index_writer.add_document(doc!(title => "The Diary of Muadib", price => 29_240u64))?;
/// index_writer.add_document(doc!(title => "A Dairy Cow", price => 21_240u64))?;
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl", price => 20_120u64))?;
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary")?;
/// let no_filter_collector = FilterCollector::new(price, &|value: u64| value > 20_120u64, TopDocs::with_limit(2));
/// let top_docs = searcher.search(&query, &no_filter_collector)?;
///
/// assert_eq!(top_docs.len(), 1);
/// assert_eq!(top_docs[0].1, DocAddress::new(0, 1));
///
/// let filter_all_collector: FilterCollector<_, _, u64> = FilterCollector::new(price, &|value| value < 5u64, TopDocs::with_limit(2));
/// let filtered_top_docs = searcher.search(&query, &filter_all_collector)?;
///
/// assert_eq!(filtered_top_docs.len(), 0);
/// # Ok(())
/// # }
/// ```
pub struct FilterCollector<TCollector, TPredicate, TPredicateValue: Default>
where TPredicate: 'static + Clone
{
    field: Field,
    collector: TCollector,
    predicate: TPredicate,
    t_predicate_value: PhantomData<TPredicateValue>,
}

impl<TCollector, TPredicate, TPredicateValue: Default>
    FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TCollector: Collector + Send + Sync,
    TPredicate: Fn(TPredicateValue) -> bool + Send + Sync + Clone,
{
    /// Create a new FilterCollector.
    pub fn new(
        field: Field,
        predicate: TPredicate,
        collector: TCollector,
    ) -> FilterCollector<TCollector, TPredicate, TPredicateValue> {
        FilterCollector {
            field,
            predicate,
            collector,
            t_predicate_value: PhantomData,
        }
    }
}

impl<TCollector, TPredicate, TPredicateValue: Default> Collector
    for FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TCollector: Collector + Send + Sync,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync + Clone,
    TPredicateValue: HasAssociatedColumnType,
    DynamicColumn: Into<Option<columnar::Column<TPredicateValue>>>,
{
    // That's the type of our result.
    // Our standard deviation will be a float.
    type Fruit = TCollector::Fruit;

    type Child = FilterSegmentCollector<TCollector::Child, TPredicate, TPredicateValue>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<FilterSegmentCollector<TCollector::Child, TPredicate, TPredicateValue>> {
        let schema = segment_reader.schema();
        let field_entry = schema.get_field_entry(self.field);
        if !field_entry.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not a fast field.",
                field_entry.name()
            )));
        }

        let fast_field_reader = segment_reader
            .fast_fields()
            .column_first_or_default(schema.get_field_name(self.field))?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(FilterSegmentCollector {
            fast_field_reader,
            segment_collector,
            predicate: self.predicate.clone(),
            t_predicate_value: PhantomData,
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

pub struct FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue>
where
    TPredicate: 'static,
    DynamicColumn: Into<Option<columnar::Column<TPredicateValue>>>,
{
    fast_field_reader: Arc<dyn ColumnValues<TPredicateValue>>,
    segment_collector: TSegmentCollector,
    predicate: TPredicate,
    t_predicate_value: PhantomData<TPredicateValue>,
}

impl<TSegmentCollector, TPredicate, TPredicateValue> SegmentCollector
    for FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue>
where
    TSegmentCollector: SegmentCollector,
    TPredicateValue: HasAssociatedColumnType,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync,
    DynamicColumn: Into<Option<columnar::Column<TPredicateValue>>>,
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        let value = self.fast_field_reader.get_val(doc);
        if (self.predicate)(value) {
            self.segment_collector.collect(doc, score)
        }
    }

    fn harvest(self) -> <TSegmentCollector as SegmentCollector>::Fruit {
        self.segment_collector.harvest()
    }
}
