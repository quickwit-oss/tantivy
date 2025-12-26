use std::marker::PhantomData;

use columnar::{Column, ValueRange};

use crate::collector::sort_key::sort_key_computer::convert_optional_u64_range_to_u64_range;
use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
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
            buffer: Vec::new(),
            fetch_buffer: Vec::new(),
        })
    }
}

pub struct SortByFastValueSegmentSortKeyComputer<T> {
    sort_column: Column<u64>,
    typ: PhantomData<T>,
    buffer: Vec<(DocId, Option<u64>)>,
    fetch_buffer: Vec<Option<Option<u64>>>,
}

impl<T: FastValue> SegmentSortKeyComputer for SortByFastValueSegmentSortKeyComputer<T> {
    type SortKey = Option<T>;
    type SegmentSortKey = Option<u64>;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> Self::SegmentSortKey {
        self.sort_column.first(doc)
    }

    fn segment_sort_keys(
        &mut self,
        docs: &[DocId],
        filter: ValueRange<Self::SegmentSortKey>,
    ) -> &mut Vec<(DocId, Self::SegmentSortKey)> {
        self.fetch_buffer.resize(docs.len(), None);
        let u64_filter = convert_optional_u64_range_to_u64_range(filter);
        self.sort_column
            .first_vals_in_value_range(docs, &mut self.fetch_buffer, u64_filter);

        self.buffer.clear();
        for (&doc, val) in docs.iter().zip(self.fetch_buffer.iter()) {
            if let Some(val) = val {
                self.buffer.push((doc, *val));
            }
        }
        &mut self.buffer
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

        let docs = vec![0, 1, 2];
        let output = computer.segment_sort_keys(&docs, ValueRange::All);

        assert_eq!(output, &[(0, Some(10)), (1, Some(20)), (2, None)]);
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

        let docs = vec![0, 1, 2];
        let output = computer.segment_sort_keys(
            &docs,
            ValueRange::GreaterThan(Some(15u64), false /* inclusive */),
        );

        // Should contain only the document with value 20.
        // Doc 0 (10) < 15
        // Doc 2 (None) < 15
        assert_eq!(output, &[(1, Some(20))]);
    }
}
