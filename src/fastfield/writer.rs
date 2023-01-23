use std::io;

use columnar::{ColumnType, ColumnarWriter, NumericalValue};

use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::schema::{Document, FieldType, Schema, Type, Value};
use crate::{DatePrecision, DocId};

/// The `FastFieldsWriter` groups all of the fast field writers.
pub struct FastFieldsWriter {
    columnar_writer: ColumnarWriter,
    fast_field_names: Vec<Option<String>>, //< TODO see if we can cash the field name hash too.
    date_precisions: Vec<DatePrecision>,
    num_docs: DocId,
}

impl FastFieldsWriter {
    /// Create all `FastFieldWriter` required by the schema.
    pub fn from_schema(schema: &Schema) -> FastFieldsWriter {
        let mut columnar_writer = ColumnarWriter::default();
        let mut fast_fields: Vec<Option<String>> = vec![None; schema.num_fields()];
        let mut date_precisions: Vec<DatePrecision> =
            std::iter::repeat_with(DatePrecision::default)
                .take(schema.num_fields())
                .collect();
        // TODO see other types
        for (field_id, field_entry) in schema.fields() {
            if !field_entry.field_type().is_fast() {
                continue;
            }
            fast_fields[field_id.field_id() as usize] = Some(field_entry.name().to_string());
            let value_type = field_entry.field_type().value_type();
            let column_type = match value_type {
                Type::Str => ColumnType::Str,
                Type::U64 => ColumnType::U64,
                Type::I64 => ColumnType::I64,
                Type::F64 => ColumnType::F64,
                Type::Bool => ColumnType::Bool,
                Type::Date => ColumnType::DateTime,
                Type::Facet => ColumnType::Str,
                Type::Bytes => ColumnType::Bytes,
                Type::Json => {
                    continue;
                }
                Type::IpAddr => ColumnType::IpAddr,
            };
            if let FieldType::Date(date_options) = field_entry.field_type() {
                date_precisions[field_id.field_id() as usize] = date_options.get_precision();
            }
            let sort_values_within_row = value_type == Type::Facet;
            columnar_writer.record_column_type(
                field_entry.name(),
                column_type,
                sort_values_within_row,
            );
        }
        FastFieldsWriter {
            columnar_writer,
            fast_field_names: fast_fields,
            num_docs: 0u32,
            date_precisions,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.columnar_writer.mem_usage()
    }

    /// Indexes all of the fastfields of a new document.
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        let doc_id = self.num_docs;
        for field_value in doc.field_values() {
            if let Some(field_name) =
                self.fast_field_names[field_value.field().field_id() as usize].as_ref()
            {
                match &field_value.value {
                    Value::U64(u64_val) => {
                        self.columnar_writer.record_numerical(
                            doc_id,
                            field_name.as_str(),
                            NumericalValue::from(*u64_val),
                        );
                    }
                    Value::I64(i64_val) => {
                        self.columnar_writer.record_numerical(
                            doc_id,
                            field_name.as_str(),
                            NumericalValue::from(*i64_val),
                        );
                    }
                    Value::F64(f64_val) => {
                        self.columnar_writer.record_numerical(
                            doc_id,
                            field_name.as_str(),
                            NumericalValue::from(*f64_val),
                        );
                    }
                    Value::Str(text_val) => {
                        self.columnar_writer
                            .record_str(doc_id, field_name.as_str(), text_val);
                    }
                    Value::Bytes(bytes_val) => {
                        self.columnar_writer
                            .record_bytes(doc_id, field_name.as_str(), bytes_val);
                    }
                    Value::PreTokStr(_) => todo!(),
                    Value::Bool(bool_val) => {
                        self.columnar_writer
                            .record_bool(doc_id, field_name.as_str(), *bool_val);
                    }
                    Value::Date(datetime) => {
                        let date_precision =
                            self.date_precisions[field_value.field().field_id() as usize];
                        let truncated_datetime = datetime.truncate(date_precision);
                        self.columnar_writer.record_datetime(
                            doc_id,
                            field_name.as_str(),
                            truncated_datetime.into(),
                        );
                    }
                    Value::Facet(facet) => {
                        self.columnar_writer.record_str(
                            doc_id,
                            field_name.as_str(),
                            facet.encoded_str(),
                        );
                    }
                    Value::JsonObject(_) => todo!(),
                    Value::IpAddr(ip_addr) => {
                        self.columnar_writer
                            .record_ip_addr(doc_id, field_name.as_str(), *ip_addr);
                    }
                }
            }
        }
        self.num_docs += 1;
        Ok(())
    }

    /// Serializes all of the `FastFieldWriter`s by pushing them in
    /// order to the fast field serializer.
    pub fn serialize(
        mut self,
        wrt: &mut dyn io::Write,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        assert!(doc_id_map.is_none()); // TODO handle doc id map
        let num_docs = self.num_docs;
        self.columnar_writer.serialize(num_docs, wrt)?;
        Ok(())
    }
}
