use std::marker::PhantomData;

use columnar::{Column, ValueRange};

use crate::collector::sort_key::sort_key_computer::convert_optional_u64_range_to_u64_range;
use crate::collector::sort_key::NaturalComparator;
use crate::collector::{ComparableDoc, SegmentSortKeyComputer, SortKeyComputer};
use crate::fastfield::{FastFieldNotAvailableError, FastValue};
use crate::{DocId, Score, SegmentReader};

/// Sorts by a fast value (u64, i64, f64, bool).
///
/// The field must appear explicitly in the schema, with the right type, and declared as
/// a fast field..
///
/// If the field is multivalued, only the first value is considered.
///
/// Documents that do not have this value are still considered.
/// Their sort key will simply be `None`.
#[derive(Debug, Clone)]
pub struct SortByStaticFastValue<T: FastValue> {
    field: String,
    typ: PhantomData<T>,
}

impl<T: FastValue> SortByStaticFastValue<T> {
    /// Creates a new `SortByStaticFastValue` instance for the given field.
    pub fn for_field(column_name: impl ToString) -> SortByStaticFastValue<T> {
        Self {
            field: column_name.to_string(),
            typ: PhantomData,
        }
    }
}

impl<T: FastValue> SortKeyComputer for SortByStaticFastValue<T> {
    type Child = SortByFastValueSegmentSortKeyComputer<T>;
    type SortKey = Option<T>;
    type Comparator = NaturalComparator;

    fn check_schema(&self, schema: &crate::schema::Schema) -> crate::Result<()> {
        // At the segment sort key computer level, we rely on the u64 representation.
        // The mapping is monotonic, so it is sufficient to compute our top-K docs.
        let field = schema.get_field(&self.field)?;
        let field_entry = schema.get_field_entry(field);
        if !field_entry.is_fast() {
            return Err(crate::TantivyError::SchemaError(format!(
                "Field `{}` is not a fast field.",
                self.field,
            )));
        }
        let schema_type = field_entry.field_type().value_type();
        if schema_type != T::to_type() {
            return Err(crate::TantivyError::SchemaError(format!(
                "Field `{}` is of type {schema_type:?}, not of the type {:?}.",
                &self.field,
                T::to_type()
            )));
        }
        Ok(())
    }

    fn segment_sort_key_computer(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let sort_column_opt = segment_reader.fast_fields().u64_lenient(&self.field)?;
        let (sort_column, _sort_column_type) =
            sort_column_opt.ok_or_else(|| FastFieldNotAvailableError {
                field_name: self.field.clone(),
            })?;
        Ok(SortByFastValueSegmentSortKeyComputer {
            sort_column,
            typ: PhantomData,
        })
    }
}

pub struct SortByFastValueSegmentSortKeyComputer<T> {
    sort_column: Column<u64>,
    typ: PhantomData<T>,
}

pub struct FastValueBuffer {
    docs: Vec<DocId>,
    vals: Vec<Option<u64>>,
}

impl Default for FastValueBuffer {
    fn default() -> Self {
        FastValueBuffer {
            docs: Vec::new(),
            vals: Vec::new(),
        }
    }
}

impl<T: FastValue> SegmentSortKeyComputer for SortByFastValueSegmentSortKeyComputer<T> {
    type SortKey = Option<T>;
    type SegmentSortKey = Option<u64>;
    type SegmentComparator = NaturalComparator;
    type Buffer = FastValueBuffer;

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> Self::SegmentSortKey {
        self.sort_column.first(doc)
    }

    fn segment_sort_keys(
        &mut self,
        input_docs: &[DocId],
        output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
        buffer: &mut Self::Buffer,
        filter: ValueRange<Self::SegmentSortKey>,
    ) {
        let u64_filter = convert_optional_u64_range_to_u64_range(filter);
        buffer.docs.clear();
        buffer.vals.clear();
        self.sort_column.first_vals_in_value_range(
            input_docs,
            &mut buffer.docs,
            &mut buffer.vals,
            u64_filter,
        );
        for (&doc, &sort_key) in buffer.docs.iter().zip(buffer.vals.iter()) {
            output.push(ComparableDoc { doc, sort_key });
        }
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key.map(T::from_u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, FAST};
    use crate::Index;

    #[test]
    fn test_sort_by_fast_value_batch() {
        let mut schema_builder = Schema::builder();
        let field_col = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();

        index_writer
            .add_document(crate::doc!(field_col => 10u64))
            .unwrap();
        index_writer
            .add_document(crate::doc!(field_col => 20u64))
            .unwrap();
        index_writer.add_document(crate::doc!()).unwrap();
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);

        let sorter = SortByStaticFastValue::<u64>::for_field("field");
        let mut computer = sorter.segment_sort_key_computer(segment_reader).unwrap();

        let mut docs = vec![0, 1, 2];
        let mut output = Vec::new();
        let mut buffer = FastValueBuffer::default();
        computer.segment_sort_keys(&mut docs, &mut output, &mut buffer, ValueRange::All);

        assert_eq!(
            output.iter().map(|c| c.sort_key).collect::<Vec<_>>(),
            &[Some(10), Some(20), None]
        );
        assert_eq!(
            output.iter().map(|c| c.doc).collect::<Vec<_>>(),
            &[0, 1, 2]
        );
    }

    #[test]
    fn test_sort_by_fast_value_batch_with_filter() {
        let mut schema_builder = Schema::builder();
        let field_col = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();

        index_writer
            .add_document(crate::doc!(field_col => 10u64))
            .unwrap();
        index_writer
            .add_document(crate::doc!(field_col => 20u64))
            .unwrap();
        index_writer.add_document(crate::doc!()).unwrap();
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);

        let sorter = SortByStaticFastValue::<u64>::for_field("field");
        let mut computer = sorter.segment_sort_key_computer(segment_reader).unwrap();

        let mut docs = vec![0, 1, 2];
        let mut output = Vec::new();
        let mut buffer = FastValueBuffer::default();
        computer.segment_sort_keys(
            &mut docs,
            &mut output,
            &mut buffer,
            ValueRange::GreaterThan(Some(15u64), false /* inclusive */),
        );

        assert_eq!(
            output.iter().map(|c| c.sort_key).collect::<Vec<_>>(),
            &[Some(20)]
        );
        assert_eq!(
            output.iter().map(|c| c.doc).collect::<Vec<_>>(),
            &[1]
        );
    }
}
