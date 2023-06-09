use std::io;

use columnar::{ColumnarWriter, NumericalValue};
use common::replace_in_place;
use tokenizer_api::Token;

use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::schema::term::{JSON_PATH_SEGMENT_SEP, JSON_PATH_SEGMENT_SEP_STR};
use crate::schema::{value_type_to_column_type, Document, FieldType, Schema, Type, Value};
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::{DateTimePrecision, DocId, TantivyError};

/// Only index JSON down to a depth of 20.
/// This is mostly to guard us from a stack overflow triggered by malicious input.
const JSON_DEPTH_LIMIT: usize = 20;

/// The `FastFieldsWriter` groups all of the fast field writers.
pub struct FastFieldsWriter {
    columnar_writer: ColumnarWriter,
    fast_field_names: Vec<Option<String>>, //< TODO see if we can hash the field name hash too.
    per_field_tokenizer: Vec<Option<TextAnalyzer>>,
    date_precisions: Vec<DateTimePrecision>,
    expand_dots: Vec<bool>,
    num_docs: DocId,
    // Buffer that we recycle to avoid allocation.
    json_path_buffer: String,
}

impl FastFieldsWriter {
    /// Create all `FastFieldWriter` required by the schema.
    #[cfg(test)]
    pub fn from_schema(schema: &Schema) -> crate::Result<FastFieldsWriter> {
        FastFieldsWriter::from_schema_and_tokenizer_manager(schema, TokenizerManager::new())
    }

    /// Create all `FastFieldWriter` required by the schema.
    pub fn from_schema_and_tokenizer_manager(
        schema: &Schema,
        tokenizer_manager: TokenizerManager,
    ) -> crate::Result<FastFieldsWriter> {
        let mut columnar_writer = ColumnarWriter::default();

        let mut fast_field_names: Vec<Option<String>> = vec![None; schema.num_fields()];
        let mut date_precisions: Vec<DateTimePrecision> =
            std::iter::repeat_with(DateTimePrecision::default)
                .take(schema.num_fields())
                .collect();
        let mut expand_dots = vec![false; schema.num_fields()];
        let mut per_field_tokenizer: Vec<Option<TextAnalyzer>> = vec![None; schema.num_fields()];
        // TODO see other types
        for (field_id, field_entry) in schema.fields() {
            if !field_entry.field_type().is_fast() {
                continue;
            }
            fast_field_names[field_id.field_id() as usize] = Some(field_entry.name().to_string());
            let value_type = field_entry.field_type().value_type();
            if let FieldType::Date(date_options) = field_entry.field_type() {
                date_precisions[field_id.field_id() as usize] = date_options.get_precision();
            }
            if let FieldType::JsonObject(json_object_options) = field_entry.field_type() {
                if let Some(tokenizer_name) = json_object_options.get_fast_field_tokenizer_name() {
                    let text_analyzer = tokenizer_manager.get(tokenizer_name).ok_or_else(|| {
                        TantivyError::InvalidArgument(format!(
                            "Tokenizer {tokenizer_name:?} not found"
                        ))
                    })?;
                    per_field_tokenizer[field_id.field_id() as usize] = Some(text_analyzer);
                }

                expand_dots[field_id.field_id() as usize] =
                    json_object_options.is_expand_dots_enabled();
            }
            if let FieldType::Str(text_options) = field_entry.field_type() {
                if let Some(tokenizer_name) = text_options.get_fast_field_tokenizer_name() {
                    let text_analyzer = tokenizer_manager.get(tokenizer_name).ok_or_else(|| {
                        TantivyError::InvalidArgument(format!(
                            "Tokenizer {tokenizer_name:?} not found"
                        ))
                    })?;
                    per_field_tokenizer[field_id.field_id() as usize] = Some(text_analyzer);
                }
            }

            let sort_values_within_row = value_type == Type::Facet;
            if let Some(column_type) = value_type_to_column_type(value_type) {
                columnar_writer.record_column_type(
                    field_entry.name(),
                    column_type,
                    sort_values_within_row,
                );
            }
        }
        Ok(FastFieldsWriter {
            columnar_writer,
            fast_field_names,
            per_field_tokenizer,
            num_docs: 0u32,
            date_precisions,
            expand_dots,
            json_path_buffer: String::new(),
        })
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.columnar_writer.mem_usage()
    }

    pub(crate) fn sort_order(
        &self,
        sort_field: &str,
        num_docs: DocId,
        reversed: bool,
    ) -> Vec<DocId> {
        self.columnar_writer
            .sort_order(sort_field, num_docs, reversed)
    }

    /// Indexes all of the fastfields of a new document.
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        let doc_id = self.num_docs;
        for field_value in doc.field_values() {
            if let Some(field_name) =
                &self.fast_field_names[field_value.field().field_id() as usize]
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
                        if let Some(tokenizer) =
                            &mut self.per_field_tokenizer[field_value.field().field_id() as usize]
                        {
                            let mut token_stream = tokenizer.token_stream(text_val);
                            token_stream.process(&mut |token: &Token| {
                                self.columnar_writer.record_str(
                                    doc_id,
                                    field_name.as_str(),
                                    &token.text,
                                );
                            })
                        } else {
                            self.columnar_writer
                                .record_str(doc_id, field_name.as_str(), text_val);
                        }
                    }
                    Value::Bytes(bytes_val) => {
                        self.columnar_writer
                            .record_bytes(doc_id, field_name.as_str(), bytes_val);
                    }
                    Value::PreTokStr(pre_tok) => {
                        for token in &pre_tok.tokens {
                            self.columnar_writer.record_str(
                                doc_id,
                                field_name.as_str(),
                                &token.text,
                            );
                        }
                    }
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
                            truncated_datetime,
                        );
                    }
                    Value::Facet(facet) => {
                        self.columnar_writer.record_str(
                            doc_id,
                            field_name.as_str(),
                            facet.encoded_str(),
                        );
                    }
                    Value::JsonObject(json_obj) => {
                        let expand_dots = self.expand_dots[field_value.field().field_id() as usize];
                        self.json_path_buffer.clear();
                        self.json_path_buffer.push_str(field_name);

                        let text_analyzer =
                            &mut self.per_field_tokenizer[field_value.field().field_id() as usize];

                        record_json_obj_to_columnar_writer(
                            doc_id,
                            json_obj,
                            expand_dots,
                            JSON_DEPTH_LIMIT,
                            &mut self.json_path_buffer,
                            &mut self.columnar_writer,
                            text_analyzer,
                        );
                    }
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
        doc_id_map_opt: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        let num_docs = self.num_docs;
        let old_to_new_row_ids =
            doc_id_map_opt.map(|doc_id_mapping| doc_id_mapping.old_to_new_ids());
        self.columnar_writer
            .serialize(num_docs, old_to_new_row_ids, wrt)?;
        Ok(())
    }
}

#[inline]
fn columnar_numerical_value(json_number: &serde_json::Number) -> Option<NumericalValue> {
    if let Some(num_i64) = json_number.as_i64() {
        return Some(num_i64.into());
    }
    if let Some(num_u64) = json_number.as_u64() {
        return Some(num_u64.into());
    }
    if let Some(num_f64) = json_number.as_f64() {
        return Some(num_f64.into());
    }
    // This can happen with arbitrary precision.... but we do not handle it.
    None
}

fn record_json_obj_to_columnar_writer(
    doc: DocId,
    json_obj: &serde_json::Map<String, serde_json::Value>,
    expand_dots: bool,
    remaining_depth_limit: usize,
    json_path_buffer: &mut String,
    columnar_writer: &mut columnar::ColumnarWriter,
    tokenizer: &mut Option<TextAnalyzer>,
) {
    for (key, child) in json_obj {
        let len_path = json_path_buffer.len();
        if !json_path_buffer.is_empty() {
            json_path_buffer.push_str(JSON_PATH_SEGMENT_SEP_STR);
        }
        json_path_buffer.push_str(key);
        if expand_dots {
            // This might include the separation byte, which is ok because it is not a dot.
            let appended_segment = &mut json_path_buffer[len_path..];
            // The unsafe below is safe as long as b'.' and JSON_PATH_SEGMENT_SEP are
            // valid single byte ut8 strings.
            // By utf-8 design, they cannot be part of another codepoint.
            replace_in_place(b'.', JSON_PATH_SEGMENT_SEP, unsafe {
                appended_segment.as_bytes_mut()
            });
        }
        record_json_value_to_columnar_writer(
            doc,
            child,
            expand_dots,
            remaining_depth_limit,
            json_path_buffer,
            columnar_writer,
            tokenizer,
        );
        // popping our sub path.
        json_path_buffer.truncate(len_path);
    }
}

fn record_json_value_to_columnar_writer(
    doc: DocId,
    json_val: &serde_json::Value,
    expand_dots: bool,
    mut remaining_depth_limit: usize,
    json_path_writer: &mut String,
    columnar_writer: &mut columnar::ColumnarWriter,
    tokenizer: &mut Option<TextAnalyzer>,
) {
    if remaining_depth_limit == 0 {
        return;
    }
    remaining_depth_limit -= 1;
    match json_val {
        serde_json::Value::Null => {
            // TODO handle null
        }
        serde_json::Value::Bool(bool_val) => {
            columnar_writer.record_bool(doc, json_path_writer, *bool_val);
        }
        serde_json::Value::Number(json_number) => {
            if let Some(numerical_value) = columnar_numerical_value(json_number) {
                columnar_writer.record_numerical(doc, json_path_writer.as_str(), numerical_value);
            }
        }
        serde_json::Value::String(text) => {
            if let Some(text_analyzer) = tokenizer.as_mut() {
                let mut token_stream = text_analyzer.token_stream(text);
                token_stream.process(&mut |token| {
                    columnar_writer.record_str(doc, json_path_writer.as_str(), &token.text);
                })
            } else {
                columnar_writer.record_str(doc, json_path_writer.as_str(), text);
            }
        }
        serde_json::Value::Array(arr) => {
            for el in arr {
                record_json_value_to_columnar_writer(
                    doc,
                    el,
                    expand_dots,
                    remaining_depth_limit,
                    json_path_writer,
                    columnar_writer,
                    tokenizer,
                );
            }
        }
        serde_json::Value::Object(json_obj) => {
            record_json_obj_to_columnar_writer(
                doc,
                json_obj,
                expand_dots,
                remaining_depth_limit,
                json_path_writer,
                columnar_writer,
                tokenizer,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use columnar::{Column, ColumnarReader, ColumnarWriter, StrColumn};

    use super::record_json_value_to_columnar_writer;
    use crate::fastfield::writer::JSON_DEPTH_LIMIT;
    use crate::DocId;

    fn test_columnar_from_jsons_aux(
        json_docs: &[serde_json::Value],
        expand_dots: bool,
    ) -> ColumnarReader {
        let mut columnar_writer = ColumnarWriter::default();
        let mut json_path = String::new();
        for (doc, json_doc) in json_docs.iter().enumerate() {
            record_json_value_to_columnar_writer(
                doc as u32,
                json_doc,
                expand_dots,
                JSON_DEPTH_LIMIT,
                &mut json_path,
                &mut columnar_writer,
                &mut None,
            );
        }
        let mut buffer = Vec::new();
        columnar_writer
            .serialize(json_docs.len() as DocId, None, &mut buffer)
            .unwrap();
        ColumnarReader::open(buffer).unwrap()
    }

    #[test]
    fn test_json_fastfield_record_simple() {
        let json_doc = serde_json::json!({
            "float": 1.02,
            "text": "hello happy tax payer",
            "nested": {"child": 3, "child2": 5},
            "arr": ["hello", "happy", "tax", "payer"]
        });
        let columnar_reader = test_columnar_from_jsons_aux(&[json_doc], false);
        let columns = columnar_reader.list_columns().unwrap();
        {
            assert_eq!(columns[0].0, "arr");
            let column_arr_opt: Option<StrColumn> = columns[0].1.open().unwrap().into();
            assert!(column_arr_opt
                .unwrap()
                .term_ords(0)
                .eq([1, 0, 3, 2].into_iter()));
        }
        {
            assert_eq!(columns[1].0, "float");
            let column_float_opt: Option<Column<f64>> = columns[1].1.open().unwrap().into();
            assert!(column_float_opt
                .unwrap()
                .values_for_doc(0)
                .eq([1.02f64].into_iter()));
        }
        {
            assert_eq!(columns[2].0, "nested\u{1}child");
            let column_nest_child_opt: Option<Column<i64>> = columns[2].1.open().unwrap().into();
            assert!(column_nest_child_opt
                .unwrap()
                .values_for_doc(0)
                .eq([3].into_iter()));
        }
        {
            assert_eq!(columns[3].0, "nested\u{1}child2");
            let column_nest_child2_opt: Option<Column<i64>> = columns[3].1.open().unwrap().into();
            assert!(column_nest_child2_opt
                .unwrap()
                .values_for_doc(0)
                .eq([5].into_iter()));
        }
        {
            assert_eq!(columns[4].0, "text");
            let column_text_opt: Option<StrColumn> = columns[4].1.open().unwrap().into();
            assert!(column_text_opt.unwrap().term_ords(0).eq([0].into_iter()));
        }
    }

    #[test]
    fn test_json_fastfield_deep_obj() {
        let json_doc = serde_json::json!(
            {"a": {"a": {"a": {"a": {"a":
            {"a": {"a": {"a": {"a": {"a":
            {"a": {"a": {"a": {"a": {"a":
            {"a": {"a": {"a": {"depth_accepted": 19, "a": {  "depth_truncated": 20}
        }}}}}}}}}}}}}}}}}}});
        let columnar_reader = test_columnar_from_jsons_aux(&[json_doc], false);
        let columns = columnar_reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);
        assert!(columns[0].0.ends_with("a\u{1}a\u{1}a\u{1}depth_accepted"));
    }

    #[test]
    fn test_json_fastfield_deep_arr() {
        let json_doc = json!(
        {"obj":
        [[[[[,
        [[[[[,
        [[[[[,
        [[18, [19, //< within limits
        [20]]]]]]]]]]]]]]]]]]]});
        let columnar_reader = test_columnar_from_jsons_aux(&[json_doc], false);
        let columns = columnar_reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].0, "obj");
        let dynamic_column = columns[0].1.open().unwrap();
        let col: Option<Column<i64>> = dynamic_column.into();
        let vals: Vec<i64> = col.unwrap().values_for_doc(0).collect();
        assert_eq!(&vals, &[18, 19])
    }

    #[test]
    fn test_json_fast_field_do_not_expand_dots() {
        let json_doc = json!({"field.with.dots": {"child.with.dot": "hello"}});
        let columnar_reader = test_columnar_from_jsons_aux(&[json_doc], false);
        let columns = columnar_reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].0, "field.with.dots\u{1}child.with.dot");
    }

    #[test]
    fn test_json_fast_field_expand_dots() {
        let json_doc = json!({"field.with.dots": {"child.with.dot": "hello"}});
        let columnar_reader = test_columnar_from_jsons_aux(&[json_doc], true);
        let columns = columnar_reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(
            columns[0].0,
            "field\u{1}with\u{1}dots\u{1}child\u{1}with\u{1}dot"
        );
    }
}
