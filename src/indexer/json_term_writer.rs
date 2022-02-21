use chrono::Utc;
use fnv::FnvHashMap;
use murmurhash32::murmurhash2;

use crate::fastfield::FastValue;
use crate::postings::{IndexingContext, IndexingPosition, PostingsWriter};
use crate::schema::term::{JSON_END_OF_PATH, JSON_PATH_SEGMENT_SEP};
use crate::schema::Type;
use crate::tokenizer::TextAnalyzer;
use crate::{DocId, Term};

/// This object is a map storing the last position for a given path for the current document
/// being indexed.
///
/// It is key to solve the following problem:
/// If we index a JsonObject emitting several terms with the same path
/// we do not want to create false positive in phrase queries.
///
/// For instance:
///
/// ```json
/// {"bands": [
///     {"band_name": "Elliot Smith"},
///     {"band_name": "The Who"},
/// ]}
/// ```
///
/// If we are careless and index each band names independently,
/// `Elliot` and `The` will end up indexed at position 0, and `Smith` and `Who` will be indexed at
/// position 1.
/// As a result, with lemmatization, "The Smiths" will match our object.
///
/// Worse, if a same term is appears in the second object, a non increasing value would be pushed
/// to the position recorder probably provoking a panic.
///
/// This problem is solved for regular multivalued object by offsetting the position
/// of values, with a position gap. Here we would like `The` and `Who` to get indexed at
/// position 2 and 3 respectively.
///
/// With regular fields, we sort the fields beforehands, so that all terms with the same
/// path are indexed consecutively.
///
/// In JSON object, we do not have this confort, so we need to record these position offsets in
/// a map.
///
/// Note that using a single position for the entire object would not hurt correctness.
/// It would however hurt compression.
///
/// We can therefore afford working with a map that is not imperfect. It is fine if several
/// path map to the same index position as long as the probability is relatively low.
#[derive(Default)]
struct IndexingPositionsPerPath {
    positions_per_path: FnvHashMap<u32, IndexingPosition>,
}

impl IndexingPositionsPerPath {
    fn get_position(&mut self, term: &Term) -> &mut IndexingPosition {
        self.positions_per_path
            .entry(murmurhash2(term.as_slice()))
            .or_insert_with(Default::default)
    }
}

pub(crate) fn index_json_values<'a>(
    doc: DocId,
    json_values: impl Iterator<Item = crate::Result<&'a serde_json::Map<String, serde_json::Value>>>,
    text_analyzer: &TextAnalyzer,
    term_buffer: &mut Term,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
) -> crate::Result<()> {
    let mut json_term_writer = JsonTermWriter::wrap(term_buffer);
    let mut positions_per_path: IndexingPositionsPerPath = Default::default();
    for json_value_res in json_values {
        let json_value = json_value_res?;
        index_json_object(
            doc,
            json_value,
            text_analyzer,
            &mut json_term_writer,
            postings_writer,
            ctx,
            &mut positions_per_path,
        );
    }
    Ok(())
}

fn index_json_object<'a>(
    doc: DocId,
    json_value: &serde_json::Map<String, serde_json::Value>,
    text_analyzer: &TextAnalyzer,
    json_term_writer: &mut JsonTermWriter<'a>,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
    positions_per_path: &mut IndexingPositionsPerPath,
) {
    for (json_path_segment, json_value) in json_value {
        json_term_writer.push_path_segment(json_path_segment);
        index_json_value(
            doc,
            json_value,
            text_analyzer,
            json_term_writer,
            postings_writer,
            ctx,
            positions_per_path,
        );
        json_term_writer.pop_path_segment();
    }
}

fn index_json_value<'a>(
    doc: DocId,
    json_value: &serde_json::Value,
    text_analyzer: &TextAnalyzer,
    json_term_writer: &mut JsonTermWriter<'a>,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
    positions_per_path: &mut IndexingPositionsPerPath,
) {
    match json_value {
        serde_json::Value::Null => {}
        serde_json::Value::Bool(val_bool) => {
            let bool_u64 = if *val_bool { 1u64 } else { 0u64 };
            json_term_writer.set_fast_value(bool_u64);
            postings_writer.subscribe(doc, 0u32, json_term_writer.term(), ctx);
        }
        serde_json::Value::Number(number) => {
            if let Some(number_u64) = number.as_u64() {
                json_term_writer.set_fast_value(number_u64);
            } else if let Some(number_i64) = number.as_i64() {
                json_term_writer.set_fast_value(number_i64);
            } else if let Some(number_f64) = number.as_f64() {
                json_term_writer.set_fast_value(number_f64);
            }
            postings_writer.subscribe(doc, 0u32, json_term_writer.term(), ctx);
        }
        serde_json::Value::String(text) => match infer_type_from_str(text) {
            TextOrDateTime::Text(text) => {
                let mut token_stream = text_analyzer.token_stream(text);
                // TODO make sure the chain position works out.
                json_term_writer.close_path_and_set_type(Type::Str);
                let indexing_position = positions_per_path.get_position(json_term_writer.term());
                postings_writer.index_text(
                    doc,
                    &mut *token_stream,
                    json_term_writer.term_buffer,
                    ctx,
                    indexing_position,
                );
            }
            TextOrDateTime::DateTime(dt) => {
                json_term_writer.set_fast_value(dt);
                postings_writer.subscribe(doc, 0u32, json_term_writer.term(), ctx);
            }
        },
        serde_json::Value::Array(arr) => {
            for val in arr {
                index_json_value(
                    doc,
                    val,
                    text_analyzer,
                    json_term_writer,
                    postings_writer,
                    ctx,
                    positions_per_path,
                );
            }
        }
        serde_json::Value::Object(map) => {
            index_json_object(
                doc,
                map,
                text_analyzer,
                json_term_writer,
                postings_writer,
                ctx,
                positions_per_path,
            );
        }
    }
}

enum TextOrDateTime<'a> {
    Text(&'a str),
    DateTime(crate::DateTime),
}

fn infer_type_from_str(text: &str) -> TextOrDateTime {
    match chrono::DateTime::parse_from_rfc3339(text) {
        Ok(dt) => {
            let dt_utc = dt.with_timezone(&Utc);
            TextOrDateTime::DateTime(dt_utc)
        }
        Err(_) => TextOrDateTime::Text(text),
    }
}

pub struct JsonTermWriter<'a> {
    term_buffer: &'a mut Term,
    path_stack: Vec<usize>,
}

impl<'a> JsonTermWriter<'a> {
    pub fn wrap(term_buffer: &'a mut Term) -> Self {
        term_buffer.clear_with_type(Type::Json);
        let mut path_stack = Vec::with_capacity(10);
        path_stack.push(5);
        Self {
            term_buffer,
            path_stack,
        }
    }

    fn trim_to_end_of_path(&mut self) {
        let end_of_path = *self.path_stack.last().unwrap();
        self.term_buffer.truncate(end_of_path);
    }

    pub fn close_path_and_set_type(&mut self, typ: Type) {
        self.trim_to_end_of_path();
        let buffer = self.term_buffer.as_mut();
        let buffer_len = buffer.len();
        buffer[buffer_len - 1] = JSON_END_OF_PATH;
        buffer.push(typ.to_code());
    }

    pub fn push_path_segment(&mut self, segment: &str) {
        // the path stack should never be empty.
        self.trim_to_end_of_path();
        let buffer = self.term_buffer.as_mut();
        let buffer_len = buffer.len();
        if self.path_stack.len() > 1 {
            buffer[buffer_len - 1] = JSON_PATH_SEGMENT_SEP;
        }
        buffer.extend(segment.as_bytes());
        buffer.push(JSON_PATH_SEGMENT_SEP);
        self.path_stack.push(buffer.len());
    }

    pub fn pop_path_segment(&mut self) {
        self.path_stack.pop();
        assert!(!self.path_stack.is_empty());
        self.trim_to_end_of_path();
    }

    /// Returns the json path of the term being currently built.
    #[cfg(test)]
    pub(crate) fn path(&self) -> &[u8] {
        let end_of_path = self.path_stack.last().cloned().unwrap_or(6);
        &self.term().as_slice()[5..end_of_path - 1]
    }

    pub fn set_fast_value<T: FastValue>(&mut self, val: T) {
        self.close_path_and_set_type(T::to_type());
        self.term_buffer
            .as_mut()
            .extend_from_slice(val.to_u64().to_be_bytes().as_slice());
    }

    #[cfg(test)]
    pub(crate) fn set_str(&mut self, text: &str) {
        self.close_path_and_set_type(Type::Str);
        self.term_buffer.as_mut().extend_from_slice(text.as_bytes());
    }

    pub fn term(&self) -> &Term {
        self.term_buffer
    }
}

#[cfg(test)]
mod tests {
    use super::JsonTermWriter;
    use crate::schema::{Field, Type};
    use crate::Term;

    #[test]
    fn test_json_writer() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("attributes");
        json_writer.push_path_segment("color");
        json_writer.set_str("red");
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=Json, field=1, path=attributes.color, vtype=Str, \"red\")"
        );
        json_writer.set_str("blue");
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=Json, field=1, path=attributes.color, vtype=Str, \"blue\")"
        );
        json_writer.pop_path_segment();
        json_writer.push_path_segment("dimensions");
        json_writer.push_path_segment("width");
        json_writer.set_fast_value(400i64);
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=Json, field=1, path=attributes.dimensions.width, vtype=I64, 400)"
        );
        json_writer.pop_path_segment();
        json_writer.push_path_segment("height");
        json_writer.set_fast_value(300i64);
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=Json, field=1, path=attributes.dimensions.height, vtype=I64, 300)"
        );
    }

    #[test]
    fn test_string_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("color");
        json_writer.set_str("red");
        assert_eq!(
            json_writer.term().as_slice(),
            b"\x00\x00\x00\x01jcolor\x00sred"
        )
    }

    #[test]
    fn test_i64_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(-4i64);
        assert_eq!(
            json_writer.term().as_slice(),
            b"\x00\x00\x00\x01jcolor\x00i\x7f\xff\xff\xff\xff\xff\xff\xfc"
        )
    }

    #[test]
    fn test_u64_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(4u64);
        assert_eq!(
            json_writer.term().as_slice(),
            b"\x00\x00\x00\x01jcolor\x00u\x00\x00\x00\x00\x00\x00\x00\x04"
        )
    }

    #[test]
    fn test_f64_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(4.0f64);
        assert_eq!(
            json_writer.term().as_slice(),
            b"\x00\x00\x00\x01jcolor\x00f\xc0\x10\x00\x00\x00\x00\x00\x00"
        )
    }

    #[test]
    fn test_push_after_set_path_segment() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("attribute");
        json_writer.set_str("something");
        json_writer.push_path_segment("color");
        json_writer.set_str("red");
        assert_eq!(
            json_writer.term().as_slice(),
            b"\x00\x00\x00\x01jattribute\x01color\x00sred"
        )
    }

    #[test]
    fn test_pop_segment() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("color");
        json_writer.push_path_segment("hue");
        json_writer.pop_path_segment();
        json_writer.set_str("red");
        assert_eq!(
            json_writer.term().as_slice(),
            b"\x00\x00\x00\x01jcolor\x00sred"
        )
    }

    #[test]
    fn test_json_writer_path() {
        let field = Field::from_field_id(1);
        let mut term = Term::new();
        term.set_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term);
        json_writer.push_path_segment("color");
        assert_eq!(json_writer.path(), b"color");
        json_writer.push_path_segment("hue");
        assert_eq!(json_writer.path(), b"color\x01hue");
        json_writer.set_str("pink");
        assert_eq!(json_writer.path(), b"color\x01hue");
    }
}
