use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use tantivy::columnar::Cardinality;
use tantivy::schema::FieldType;
use tantivy::Index;

/// Maps a tantivy field type to the scalar Arrow data type.
fn scalar_arrow_type(field_type: &FieldType) -> Option<DataType> {
    match field_type {
        FieldType::U64(_) => Some(DataType::UInt64),
        FieldType::I64(_) => Some(DataType::Int64),
        FieldType::F64(_) => Some(DataType::Float64),
        FieldType::Bool(_) => Some(DataType::Boolean),
        FieldType::Date(_) => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        FieldType::Str(_) => Some(DataType::Utf8),
        FieldType::Bytes(_) => Some(DataType::Binary),
        FieldType::IpAddr(_) => Some(DataType::Utf8),
        FieldType::Facet(_) | FieldType::JsonObject(_) => None,
    }
}

/// Converts a tantivy schema to an Arrow schema, including only fast fields.
///
/// All fields are mapped to scalar types regardless of actual cardinality.
/// Use [`tantivy_schema_to_arrow_from_index`] to detect multi-valued fields.
pub fn tantivy_schema_to_arrow(schema: &tantivy::schema::Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .filter_map(|(_field, field_entry)| {
            if !field_entry.is_fast() {
                return None;
            }
            let arrow_type = scalar_arrow_type(field_entry.field_type())?;
            Some(Field::new(field_entry.name(), arrow_type, true))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Converts a tantivy index's schema to an Arrow schema, inspecting actual
/// segment data to detect multi-valued fields.
///
/// If ANY segment has `Cardinality::Multivalued` for a field, the Arrow type
/// is `List<T>` instead of the scalar `T`.
pub fn tantivy_schema_to_arrow_from_index(index: &Index) -> SchemaRef {
    let schema = index.schema();

    // Try to open a reader to inspect segment cardinalities.
    // If the index has no segments yet, fall back to all-scalar.
    let reader = match index.reader() {
        Ok(r) => r,
        Err(_) => return tantivy_schema_to_arrow(&schema),
    };
    let searcher = reader.searcher();
    let segment_readers = searcher.segment_readers();
    if segment_readers.is_empty() {
        return tantivy_schema_to_arrow(&schema);
    }

    let fields: Vec<Field> = schema
        .fields()
        .filter_map(|(_tantivy_field, field_entry)| {
            if !field_entry.is_fast() {
                return None;
            }
            let inner_type = scalar_arrow_type(field_entry.field_type())?;
            let name = field_entry.name();

            let is_multivalued = segment_readers.iter().any(|seg| {
                field_cardinality(seg, name, field_entry.field_type())
                    == Some(Cardinality::Multivalued)
            });

            let arrow_type = if is_multivalued {
                DataType::List(Arc::new(Field::new("item", inner_type, true)))
            } else {
                inner_type
            };

            Some(Field::new(name, arrow_type, true))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Returns the cardinality of a fast field in a given segment, or `None` on error.
fn field_cardinality(
    segment_reader: &tantivy::SegmentReader,
    name: &str,
    field_type: &FieldType,
) -> Option<Cardinality> {
    let ff = segment_reader.fast_fields();
    match field_type {
        FieldType::U64(_) => ff.u64(name).ok().map(|c| c.get_cardinality()),
        FieldType::I64(_) => ff.i64(name).ok().map(|c| c.get_cardinality()),
        FieldType::F64(_) => ff.f64(name).ok().map(|c| c.get_cardinality()),
        FieldType::Bool(_) => ff.bool(name).ok().map(|c| c.get_cardinality()),
        FieldType::Date(_) => ff.date(name).ok().map(|c| c.get_cardinality()),
        FieldType::Str(_) => ff
            .str(name)
            .ok()
            .flatten()
            .map(|s| s.ords().get_cardinality()),
        FieldType::Bytes(_) => ff
            .bytes(name)
            .ok()
            .flatten()
            .map(|b| b.ords().get_cardinality()),
        FieldType::IpAddr(_) => ff.ip_addr(name).ok().map(|c| c.get_cardinality()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::schema::{SchemaBuilder, FAST, TEXT};

    #[test]
    fn test_schema_mapping() {
        let mut builder = SchemaBuilder::new();
        builder.add_u64_field("id", FAST);
        builder.add_i64_field("score", FAST);
        builder.add_f64_field("price", FAST);
        builder.add_bool_field("active", FAST);
        builder.add_text_field("title", TEXT); // not FAST → should be skipped
        let schema = builder.build();

        let arrow_schema = tantivy_schema_to_arrow(&schema);
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(arrow_schema.field(1).name(), "score");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(arrow_schema.field(2).name(), "price");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(arrow_schema.field(3).name(), "active");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Boolean);
    }
}
