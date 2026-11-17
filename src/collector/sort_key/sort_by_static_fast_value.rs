use std::marker::PhantomData;

use columnar::Column;

use crate::collector::sort_key::{Comparator, NaturalComparator};
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer, TopNComputer};
use crate::fastfield::{FastFieldNotAvailableError, FastValue};
use crate::{DocId, Score, SegmentReader, COLLECT_BLOCK_BUFFER_LEN};

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

impl<T: FastValue> SegmentSortKeyComputer for SortByFastValueSegmentSortKeyComputer<T> {
    type SortKey = Option<T>;
    type SegmentSortKey = Option<u64>;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> Self::SegmentSortKey {
        self.sort_column.first(doc)
    }

    #[inline]
    fn compute_sort_keys_and_collect_block<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        docs: &[DocId],
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        let mut sort_keys = [None; COLLECT_BLOCK_BUFFER_LEN];
        for docs_chunk in docs.chunks(COLLECT_BLOCK_BUFFER_LEN) {
            let sort_keys = &mut sort_keys[..docs_chunk.len()];
            sort_keys.fill(None);
            self.sort_column.first_vals(docs_chunk, sort_keys);
            for (&doc, &sort_key) in docs_chunk.iter().zip(sort_keys.iter()) {
                top_n_computer.push(sort_key, doc);
            }
        }
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key.map(T::from_u64)
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::{Count, TopDocs};
    use crate::query::AllQuery;
    use crate::schema::{Schema, FAST, STRING};
    use crate::{Index, IndexWriter, Order, TantivyDocument};

    /// Checks the batched (`collect_block`) path against a brute force sort, including
    /// documents without a value, ties, pagination and both orders.
    #[test]
    fn test_order_by_fast_field_block_collection() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let value_field = schema_builder.add_u64_field("value", FAST);
        let other = schema_builder.add_text_field("other", STRING);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        let mut values: Vec<Option<u64>> = Vec::new();
        for i in 0..1_000u64 {
            let mut doc = TantivyDocument::default();
            doc.add_text(other, "x");
            let value = if i % 13 == 0 {
                None
            } else {
                Some((i * 7919) % 211)
            };
            if let Some(value) = value {
                doc.add_u64(value_field, value);
            }
            writer.add_document(doc)?;
            values.push(value);
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        for order in [Order::Desc, Order::Asc] {
            // Documents without a value always come last.
            let mut expected: Vec<Option<u64>> =
                values.iter().copied().flatten().map(Some).collect();
            expected.sort();
            if order == Order::Desc {
                expected.reverse();
            }
            expected.extend(values.iter().filter(|value| value.is_none()));
            for (offset, limit) in [(0, 10), (0, 100), (95, 50), (900, 200)] {
                let (count, top_docs) = searcher.search(
                    &AllQuery,
                    &(
                        Count,
                        TopDocs::with_limit(limit)
                            .and_offset(offset)
                            .order_by_fast_field::<u64>("value", order),
                    ),
                )?;
                assert_eq!(count, values.len());
                let got: Vec<Option<u64>> = top_docs.iter().map(|(value, _)| *value).collect();
                let end = (offset + limit).min(expected.len());
                assert_eq!(
                    got,
                    expected[offset..end].to_vec(),
                    "{order:?} {offset} {limit}"
                );
                for (value, doc_address) in top_docs {
                    assert_eq!(values[doc_address.doc_id as usize], value);
                }
            }
        }
        Ok(())
    }
}
