use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use tantivy::schema::FieldType;

/// Converts a tantivy schema to an Arrow schema, including only fast fields.
///
/// The mapping from tantivy field types to Arrow data types is:
/// - `U64` → `UInt64`
/// - `I64` → `Int64`
/// - `F64` → `Float64`
/// - `Bool` → `Boolean`
/// - `Date` → `Timestamp(Microsecond, None)`
/// - `Str` → `Utf8`
/// - `Bytes` → `Binary`
/// - `IpAddr` → `Utf8` (formatted as string)
/// - `Facet`, `JsonObject` → skipped
pub fn tantivy_schema_to_arrow(schema: &tantivy::schema::Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .filter_map(|(_field, field_entry)| {
            if !field_entry.is_fast() {
                return None;
            }
            let arrow_type = match field_entry.field_type() {
                FieldType::U64(_) => DataType::UInt64,
                FieldType::I64(_) => DataType::Int64,
                FieldType::F64(_) => DataType::Float64,
                FieldType::Bool(_) => DataType::Boolean,
                FieldType::Date(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
                FieldType::Str(_) => DataType::Utf8,
                FieldType::Bytes(_) => DataType::Binary,
                FieldType::IpAddr(_) => DataType::Utf8,
                FieldType::Facet(_) | FieldType::JsonObject(_) => return None,
            };
            Some(Field::new(field_entry.name(), arrow_type, true))
        })
        .collect();
    Arc::new(Schema::new(fields))
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
