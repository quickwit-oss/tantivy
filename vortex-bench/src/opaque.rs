//! The physical tantivy columns, hidden behind a registered `ValueSource`.
//!
//! Aggregations have fast paths that need the physical column (`ValueSource::as_column`). A
//! registered source does not expose it, so these fast paths are disabled. This source reads the
//! regular columnar, but hides it: comparing it with the Vortex source isolates the cost of the
//! storage itself from the cost of losing these fast paths.

use tantivy::aggregation::{ValueSource, ValueSourceDictionary, ValueSourceProvider};
use tantivy::columnar::{Cardinality, Column, ColumnType, DynamicColumn, RowId};
use tantivy::{DocId, SegmentReader};

pub struct OpaqueColumnarProvider {
    field_name: String,
}

impl OpaqueColumnarProvider {
    pub fn new(field_name: &str) -> Self {
        OpaqueColumnarProvider {
            field_name: field_name.to_string(),
        }
    }
}

impl ValueSourceProvider for OpaqueColumnarProvider {
    fn for_segment(
        &self,
        reader: &SegmentReader,
        allowed_column_types: Option<&[ColumnType]>,
    ) -> tantivy::Result<Option<Box<dyn ValueSource>>> {
        let handles = reader
            .fast_fields()
            .dynamic_column_handles(&self.field_name)?;
        for handle in handles {
            if let Some(allowed_column_types) = allowed_column_types {
                if !allowed_column_types.contains(&handle.column_type()) {
                    continue;
                }
            }
            let column_type = handle.column_type();
            let inner: Box<dyn ValueSource> = if column_type == ColumnType::Str {
                let DynamicColumn::Str(str_column) = handle.open()? else {
                    continue;
                };
                Box::new(str_column)
            } else {
                let Some(column) = handle.open_u64_lenient()? else {
                    continue;
                };
                Box::new((column, column_type))
            };
            let column: &Column<u64> = inner
                .as_column()
                .expect("physical sources expose their column");
            let bounds = if column.num_docs() > 0 && column.values.num_vals() > 0 {
                Some((column.min_value(), column.max_value()))
            } else {
                None
            };
            return Ok(Some(Box::new(OpaqueSource { inner, bounds })));
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct OpaqueSource {
    inner: Box<dyn ValueSource>,
    bounds: Option<(u64, u64)>,
}

impl ValueSource for OpaqueSource {
    fn column_type(&self) -> ColumnType {
        self.inner.column_type()
    }

    #[inline]
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        self.inner.load_block(docs, values, docids, row_ids)
    }

    fn bounds(&self) -> Option<(u64, u64)> {
        self.bounds
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        self.inner.term_dictionary()
    }
}
