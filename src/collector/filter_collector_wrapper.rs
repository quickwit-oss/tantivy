// # Custom collector example
//
// This example shows how you can implement your own
// collector. As an example, we will compute a collector
// that computes the standard deviation of a given fast field.
//
// Of course, you can have a look at the tantivy's built-in collectors
// such as the `CountCollector` for more examples.
use std::fmt::Debug;
use std::marker::PhantomData;

use columnar::{BytesColumn, Column, DynamicColumn, HasAssociatedColumnType};

use crate::collector::{Collector, SegmentCollector};
use crate::schema::Field;
use crate::{DocId, Score, SegmentReader, TantivyError};

/// The `FilterCollector` filters docs using a fast field value and a predicate.
///
/// Only the documents containing at least one value for which the predicate returns `true`
/// will be passed on to the next collector.
///
/// In other words,
/// - documents with no values are filtered out.
/// - documents with several values are accepted if at least one value matches the predicate.
///
///
/// ```rust
/// use tantivy::collector::{TopDocs, FilterCollector};
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT, FAST};
/// use tantivy::{doc, DocAddress, Index};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let price = schema_builder.add_u64_field("price", FAST);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
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
/// let no_filter_collector = FilterCollector::new(price, |value: u64| value > 20_120u64, TopDocs::with_limit(2));
/// let top_docs = searcher.search(&query, &no_filter_collector)?;
///
/// assert_eq!(top_docs.len(), 1);
/// assert_eq!(top_docs[0].1, DocAddress::new(0, 1));
///
/// let filter_all_collector: FilterCollector<_, _, u64> = FilterCollector::new(price, |value| value < 5u64, TopDocs::with_limit(2));
/// let filtered_top_docs = searcher.search(&query, &filter_all_collector)?;
///
/// assert_eq!(filtered_top_docs.len(), 0);
/// # Ok(())
/// # }
/// ```
///
/// Note that this is limited to fast fields which implement the
/// [`FastValue`][crate::fastfield::FastValue] trait, e.g. `u64` but not `&[u8]`.
/// To filter based on a bytes fast field, use a [`BytesFilterCollector`] instead.
pub struct FilterCollector<TCollector, TPredicate, TPredicateValue>
where TPredicate: 'static + Clone
{
    field: Field,
    collector: TCollector,
    predicate: TPredicate,
    t_predicate_value: PhantomData<TPredicateValue>,
}

impl<TCollector, TPredicate, TPredicateValue>
    FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TCollector: Collector + Send + Sync,
    TPredicate: Fn(TPredicateValue) -> bool + Send + Sync + Clone,
{
    /// Create a new `FilterCollector`.
    pub fn new(field: Field, predicate: TPredicate, collector: TCollector) -> Self {
        Self {
            field,
            predicate,
            collector,
            t_predicate_value: PhantomData,
        }
    }
}

impl<TCollector, TPredicate, TPredicateValue> Collector
    for FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TCollector: Collector + Send + Sync,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync + Clone,
    TPredicateValue: HasAssociatedColumnType,
    DynamicColumn: Into<Option<columnar::Column<TPredicateValue>>>,
{
    type Fruit = TCollector::Fruit;

    type Child = FilterSegmentCollector<TCollector::Child, TPredicate, TPredicateValue>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let schema = segment_reader.schema();
        let field_entry = schema.get_field_entry(self.field);
        if !field_entry.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not a fast field.",
                field_entry.name()
            )));
        }

        let column_opt = segment_reader
            .fast_fields()
            .column_opt(field_entry.name())?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(FilterSegmentCollector {
            column_opt,
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

pub struct FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue> {
    column_opt: Option<Column<TPredicateValue>>,
    segment_collector: TSegmentCollector,
    predicate: TPredicate,
    t_predicate_value: PhantomData<TPredicateValue>,
}

impl<TSegmentCollector, TPredicate, TPredicateValue>
    FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue>
where
    TPredicateValue: PartialOrd + Copy + Debug + Send + Sync + 'static,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync,
{
    #[inline]
    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(column) = &self.column_opt {
            for val in column.values_for_doc(doc_id) {
                if (self.predicate)(val) {
                    return true;
                }
            }
        }
        false
    }
}

impl<TSegmentCollector, TPredicate, TPredicateValue> SegmentCollector
    for FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue>
where
    TSegmentCollector: SegmentCollector,
    TPredicateValue: HasAssociatedColumnType,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync, /* DynamicColumn: Into<Option<columnar::Column<TPredicateValue>>> */
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        if self.accept_document(doc) {
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> TSegmentCollector::Fruit {
        self.segment_collector.harvest()
    }
}

/// A variant of the [`FilterCollector`] specialized for bytes fast fields, i.e.
/// it transparently wraps an inner [`Collector`] but filters documents
/// based on the result of applying the predicate to the bytes fast field.
///
/// A document is accepted if and only if the predicate returns `true` for at least one value.
///
/// In other words,
/// - documents with no values are filtered out.
/// - documents with several values are accepted if at least one value matches the predicate.
///
/// ```rust
/// use tantivy::collector::{TopDocs, BytesFilterCollector};
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT, FAST};
/// use tantivy::{doc, DocAddress, Index};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let barcode = schema_builder.add_bytes_field("barcode", FAST);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
/// index_writer.add_document(doc!(title => "The Name of the Wind", barcode => &b"010101"[..]))?;
/// index_writer.add_document(doc!(title => "The Diary of Muadib", barcode => &b"110011"[..]))?;
/// index_writer.add_document(doc!(title => "A Dairy Cow", barcode => &b"110111"[..]))?;
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl", barcode => &b"011101"[..]))?;
/// index_writer.add_document(doc!(title => "Bridget Jones's Diary"))?;
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary")?;
/// let filter_collector = BytesFilterCollector::new(barcode, |bytes: &[u8]| bytes.starts_with(b"01"), TopDocs::with_limit(2));
/// let top_docs = searcher.search(&query, &filter_collector)?;
///
/// assert_eq!(top_docs.len(), 1);
/// assert_eq!(top_docs[0].1, DocAddress::new(0, 3));
/// # Ok(())
/// # }
/// ```
pub struct BytesFilterCollector<TCollector, TPredicate>
where TPredicate: 'static + Clone
{
    field: Field,
    collector: TCollector,
    predicate: TPredicate,
}

impl<TCollector, TPredicate> BytesFilterCollector<TCollector, TPredicate>
where
    TCollector: Collector + Send + Sync,
    TPredicate: Fn(&[u8]) -> bool + Send + Sync + Clone,
{
    /// Create a new `BytesFilterCollector`.
    pub fn new(field: Field, predicate: TPredicate, collector: TCollector) -> Self {
        Self {
            field,
            predicate,
            collector,
        }
    }
}

impl<TCollector, TPredicate> Collector for BytesFilterCollector<TCollector, TPredicate>
where
    TCollector: Collector + Send + Sync,
    TPredicate: 'static + Fn(&[u8]) -> bool + Send + Sync + Clone,
{
    type Fruit = TCollector::Fruit;

    type Child = BytesFilterSegmentCollector<TCollector::Child, TPredicate>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let schema = segment_reader.schema();
        let field_name = schema.get_field_name(self.field);

        let column_opt = segment_reader.fast_fields().bytes(field_name)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(BytesFilterSegmentCollector {
            column_opt,
            segment_collector,
            predicate: self.predicate.clone(),
            buffer: Vec::new(),
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

pub struct BytesFilterSegmentCollector<TSegmentCollector, TPredicate>
where TPredicate: 'static
{
    column_opt: Option<BytesColumn>,
    segment_collector: TSegmentCollector,
    predicate: TPredicate,
    buffer: Vec<u8>,
}

impl<TSegmentCollector, TPredicate> BytesFilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TSegmentCollector: SegmentCollector,
    TPredicate: 'static + Fn(&[u8]) -> bool + Send + Sync,
{
    #[inline]
    fn accept_document(&mut self, doc_id: DocId) -> bool {
        if let Some(column) = &self.column_opt {
            for ord in column.term_ords(doc_id) {
                self.buffer.clear();

                let found = column.ord_to_bytes(ord, &mut self.buffer).unwrap_or(false);

                if found && (self.predicate)(&self.buffer) {
                    return true;
                }
            }
        }
        false
    }
}

impl<TSegmentCollector, TPredicate> SegmentCollector
    for BytesFilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TSegmentCollector: SegmentCollector,
    TPredicate: 'static + Fn(&[u8]) -> bool + Send + Sync,
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        if self.accept_document(doc) {
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> TSegmentCollector::Fruit {
        self.segment_collector.harvest()
    }
}
