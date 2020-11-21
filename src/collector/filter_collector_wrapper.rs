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

use crate::collector::{Collector, SegmentCollector};
use crate::fastfield::FastFieldReader;
use crate::schema::Field;
use crate::{Score, SegmentReader, TantivyError};

/// The `TopDocs` collector keeps track of the top `K` documents
/// sorted by their score.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
///
/// This collector guarantees a stable sorting in case of a tie on the
/// document score. As such, it is suitable to implement pagination.
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
/// let no_filter_collector = FilterCollector::new(price, &|value| true, TopDocs::with_limit(2));
/// let top_docs = searcher.search(&query, &no_filter_collector).unwrap();
///
/// assert_eq!(top_docs.len(), 2);
/// assert_eq!(top_docs[0].1, DocAddress(0, 1));
/// assert_eq!(top_docs[1].1, DocAddress(0, 3));
/// 
/// let filter_all_collector = FilterCollector::new(price, &|value| false, TopDocs::with_limit(2));
/// let filtered_top_docs = searcher.search(&query, &filter_all_collector).unwrap();
/// 
/// assert_eq!(filtered_top_docs.len(), 0);
/// ```
pub struct FilterCollector<TCollector, TSegmentCollector> {
    field: Field,
    collector: TCollector,
    predicate: &'static (dyn Fn(u64) -> bool + Send + Sync),
    phantom: PhantomData<TSegmentCollector>
}

impl<TCollector, TSegmentCollector> FilterCollector<TCollector, TSegmentCollector> 
where 
    TCollector: Collector<Child = TSegmentCollector> + Send + Sync,
    TSegmentCollector: 'static + SegmentCollector + Send + Sync {
    pub fn new(field: Field, predicate: &'static (dyn Fn(u64) -> bool + Send + Sync), collector: TCollector) -> FilterCollector<TCollector, TSegmentCollector> {
        FilterCollector { field, predicate, collector, phantom: PhantomData }
    }
}

impl<TCollector, TSegmentCollector> Collector for FilterCollector<TCollector, TSegmentCollector> 
where 
    TSegmentCollector: 'static + SegmentCollector + Send + Sync,
    TCollector: Collector<Child = TSegmentCollector> + Send + Sync {
    // That's the type of our result.
    // Our standard deviation will be a float.
    type Fruit = TCollector::Fruit;

    type Child = FilterSegmentCollector<TSegmentCollector>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<FilterSegmentCollector<TSegmentCollector>> {
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
        let child_segment_collector = self.collector.for_segment(segment_local_id, segment_reader);
        match child_segment_collector {
            Ok(segment_collector) => { 
                Ok(FilterSegmentCollector::<TSegmentCollector> {
                    fast_field_reader,
                    segment_collector: segment_collector,
                    predicate: self.predicate
                })        
            },
            Err(_) => {
                Err(TantivyError::SystemError("Could not open segment: ".to_owned()))
            },
        }
        
        
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(&self, segment_fruits: Vec<<TCollector::Child as SegmentCollector>::Fruit>) -> crate::Result<TCollector::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct FilterSegmentCollector<TSegmentCollector> {
    fast_field_reader: FastFieldReader<u64>,
    segment_collector: TSegmentCollector,
    predicate: &'static (dyn Fn(u64) -> bool + Send + Sync)
}

impl<TSegmentCollector> SegmentCollector for FilterSegmentCollector<TSegmentCollector> 
where 
    TSegmentCollector: 'static + SegmentCollector + Send + Sync {
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
