use columnar::MonotonicallyMappableToU64;
use common::replace_in_place;
use murmurhash32::murmurhash2;
use rustc_hash::FxHashMap;

use crate::fastfield::FastValue;
use crate::postings::{IndexingContext, IndexingPosition, PostingsWriter};
use crate::schema::term::{JSON_PATH_SEGMENT_SEP, JSON_PATH_SEGMENT_SEP_STR};
use crate::schema::{Field, Type, DATE_TIME_PRECISION_INDEXED};
use crate::time::format_description::well_known::Rfc3339;
use crate::time::{OffsetDateTime, UtcOffset};
use crate::tokenizer::TextAnalyzer;
use crate::{DateTime, DocId, Term};

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
/// With regular fields, we sort the fields beforehand, so that all terms with the same
/// path are indexed consecutively.
///
/// In JSON object, we do not have this comfort, so we need to record these position offsets in
/// a map.
///
/// Note that using a single position for the entire object would not hurt correctness.
/// It would however hurt compression.
///
/// We can therefore afford working with a map that is not imperfect. It is fine if several
/// path map to the same index position as long as the probability is relatively low.
#[derive(Default)]
struct IndexingPositionsPerPath {
    positions_per_path: FxHashMap<u32, IndexingPosition>,
}

impl IndexingPositionsPerPath {
    fn get_position(&mut self, term: &Term) -> &mut IndexingPosition {
        self.positions_per_path
            .entry(murmurhash2(term.serialized_term()))
            .or_default()
    }
}

pub(crate) fn index_json_values<'a>(
    doc: DocId,
    json_values: impl Iterator<Item = crate::Result<&'a serde_json::Map<String, serde_json::Value>>>,
    text_analyzer: &mut TextAnalyzer,
    expand_dots_enabled: bool,
    term_buffer: &mut Term,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
) -> crate::Result<()> {
    let mut json_term_writer = JsonTermWriter::wrap(term_buffer, expand_dots_enabled);
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

fn index_json_object(
    doc: DocId,
    json_value: &serde_json::Map<String, serde_json::Value>,
    text_analyzer: &mut TextAnalyzer,
    json_term_writer: &mut JsonTermWriter,
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

fn index_json_value(
    doc: DocId,
    json_value: &serde_json::Value,
    text_analyzer: &mut TextAnalyzer,
    json_term_writer: &mut JsonTermWriter,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
    positions_per_path: &mut IndexingPositionsPerPath,
) {
    match json_value {
        serde_json::Value::Null => {}
        serde_json::Value::Bool(val_bool) => {
            json_term_writer.set_fast_value(*val_bool);
            postings_writer.subscribe(doc, 0u32, json_term_writer.term(), ctx);
        }
        serde_json::Value::Number(number) => {
            if let Some(number_i64) = number.as_i64() {
                json_term_writer.set_fast_value(number_i64);
            } else if let Some(number_u64) = number.as_u64() {
                json_term_writer.set_fast_value(number_u64);
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
                json_term_writer.set_fast_value(DateTime::from_utc(dt));
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
    DateTime(OffsetDateTime),
}

fn infer_type_from_str(text: &str) -> TextOrDateTime {
    match OffsetDateTime::parse(text, &Rfc3339) {
        Ok(dt) => {
            let dt_utc = dt.to_offset(UtcOffset::UTC);
            TextOrDateTime::DateTime(dt_utc)
        }
        Err(_) => TextOrDateTime::Text(text),
    }
}

// Tries to infer a JSON type from a string.
pub fn convert_to_fast_value_and_get_term(
    json_term_writer: &mut JsonTermWriter,
    phrase: &str,
) -> Option<Term> {
    if let Ok(dt) = OffsetDateTime::parse(phrase, &Rfc3339) {
        let dt_utc = dt.to_offset(UtcOffset::UTC);
        return Some(set_fastvalue_and_get_term(
            json_term_writer,
            DateTime::from_utc(dt_utc),
        ));
    }
    if let Ok(i64_val) = str::parse::<i64>(phrase) {
        return Some(set_fastvalue_and_get_term(json_term_writer, i64_val));
    }
    if let Ok(u64_val) = str::parse::<u64>(phrase) {
        return Some(set_fastvalue_and_get_term(json_term_writer, u64_val));
    }
    if let Ok(f64_val) = str::parse::<f64>(phrase) {
        return Some(set_fastvalue_and_get_term(json_term_writer, f64_val));
    }
    if let Ok(bool_val) = str::parse::<bool>(phrase) {
        return Some(set_fastvalue_and_get_term(json_term_writer, bool_val));
    }
    None
}
// helper function to generate a Term from a json fastvalue
pub(crate) fn set_fastvalue_and_get_term<T: FastValue>(
    json_term_writer: &mut JsonTermWriter,
    value: T,
) -> Term {
    json_term_writer.set_fast_value(value);
    json_term_writer.term().clone()
}

// helper function to generate a list of terms with their positions from a textual json value
pub(crate) fn set_string_and_get_terms(
    json_term_writer: &mut JsonTermWriter,
    value: &str,
    text_analyzer: &mut TextAnalyzer,
) -> Vec<(usize, Term)> {
    let mut positions_and_terms = Vec::<(usize, Term)>::new();
    json_term_writer.close_path_and_set_type(Type::Str);
    let term_num_bytes = json_term_writer.term_buffer.len_bytes();
    let mut token_stream = text_analyzer.token_stream(value);
    token_stream.process(&mut |token| {
        json_term_writer
            .term_buffer
            .truncate_value_bytes(term_num_bytes);
        json_term_writer
            .term_buffer
            .append_bytes(token.text.as_bytes());
        positions_and_terms.push((token.position, json_term_writer.term().clone()));
    });
    positions_and_terms
}

/// Writes a value of a JSON field to a `Term`.
/// The Term format is as follows:
/// `[JSON_TYPE][JSON_PATH][JSON_END_OF_PATH][VALUE_BYTES]`
pub struct JsonTermWriter<'a> {
    term_buffer: &'a mut Term,
    path_stack: Vec<usize>,
    expand_dots_enabled: bool,
}

/// Splits a json path supplied to the query parser in such a way that
/// `.` can be escaped.
///
/// In other words,
/// - `k8s.node` ends up as `["k8s", "node"]`.
/// - `k8s\.node` ends up as `["k8s.node"]`.
fn split_json_path(json_path: &str) -> Vec<String> {
    let mut escaped_state: bool = false;
    let mut json_path_segments = Vec::new();
    let mut buffer = String::new();
    for ch in json_path.chars() {
        if escaped_state {
            buffer.push(ch);
            escaped_state = false;
            continue;
        }
        match ch {
            '\\' => {
                escaped_state = true;
            }
            '.' => {
                let new_segment = std::mem::take(&mut buffer);
                json_path_segments.push(new_segment);
            }
            _ => {
                buffer.push(ch);
            }
        }
    }
    json_path_segments.push(buffer);
    json_path_segments
}

/// Takes a field name, a json path as supplied by a user, and whether we should expand dots, and
/// return a column key, as expected by the columnar crate.
///
/// This function will detect unescaped dots in the path, and split over them.
/// If expand_dots is enabled, then even escaped dots will be split over.
///
/// The resulting list of segment then gets stitched together, joined by \1 separator,
/// as defined in the columnar crate.
pub(crate) fn encode_column_name(
    field_name: &str,
    json_path: &str,
    expand_dots_enabled: bool,
) -> String {
    let mut column_key: String = String::with_capacity(field_name.len() + json_path.len() + 1);
    column_key.push_str(field_name);
    for mut segment in split_json_path(json_path) {
        column_key.push_str(JSON_PATH_SEGMENT_SEP_STR);
        if expand_dots_enabled {
            // We need to replace `.` by JSON_PATH_SEGMENT_SEP.
            unsafe { replace_in_place(b'.', JSON_PATH_SEGMENT_SEP, segment.as_bytes_mut()) };
        }
        column_key.push_str(&segment);
    }
    column_key
}

impl<'a> JsonTermWriter<'a> {
    pub fn from_field_and_json_path(
        field: Field,
        json_path: &str,
        expand_dots_enabled: bool,
        term_buffer: &'a mut Term,
    ) -> Self {
        term_buffer.set_field_and_type(field, Type::Json);
        let mut json_term_writer = Self::wrap(term_buffer, expand_dots_enabled);
        for segment in split_json_path(json_path) {
            json_term_writer.push_path_segment(&segment);
        }
        json_term_writer
    }

    pub fn wrap(term_buffer: &'a mut Term, expand_dots_enabled: bool) -> Self {
        term_buffer.clear_with_type(Type::Json);
        let mut path_stack = Vec::with_capacity(10);
        path_stack.push(0);
        Self {
            term_buffer,
            path_stack,
            expand_dots_enabled,
        }
    }

    fn trim_to_end_of_path(&mut self) {
        let end_of_path = *self.path_stack.last().unwrap();
        self.term_buffer.truncate_value_bytes(end_of_path);
    }

    pub fn close_path_and_set_type(&mut self, typ: Type) {
        self.trim_to_end_of_path();
        self.term_buffer.set_json_path_end();
        self.term_buffer.append_bytes(&[typ.to_code()]);
    }

    pub fn push_path_segment(&mut self, segment: &str) {
        // the path stack should never be empty.
        self.trim_to_end_of_path();

        if self.path_stack.len() > 1 {
            self.term_buffer.set_json_path_separator();
        }
        let appended_segment = self.term_buffer.append_bytes(segment.as_bytes());
        if self.expand_dots_enabled {
            // We need to replace `.` by JSON_PATH_SEGMENT_SEP.
            replace_in_place(b'.', JSON_PATH_SEGMENT_SEP, appended_segment);
        }
        self.term_buffer.add_json_path_separator();
        self.path_stack.push(self.term_buffer.len_bytes());
    }

    pub fn pop_path_segment(&mut self) {
        self.path_stack.pop();
        assert!(!self.path_stack.is_empty());
        self.trim_to_end_of_path();
    }

    /// Returns the json path of the term being currently built.
    #[cfg(test)]
    pub(crate) fn path(&self) -> &[u8] {
        let end_of_path = self.path_stack.last().cloned().unwrap_or(1);
        &self.term().serialized_value_bytes()[..end_of_path - 1]
    }

    pub(crate) fn set_fast_value<T: FastValue>(&mut self, val: T) {
        self.close_path_and_set_type(T::to_type());
        let value = if T::to_type() == Type::Date {
            DateTime::from_u64(val.to_u64())
                .truncate(DATE_TIME_PRECISION_INDEXED)
                .to_u64()
        } else {
            val.to_u64()
        };
        self.term_buffer
            .append_bytes(value.to_be_bytes().as_slice());
    }

    pub fn set_str(&mut self, text: &str) {
        self.close_path_and_set_type(Type::Str);
        self.term_buffer.append_bytes(text.as_bytes());
    }

    pub fn term(&self) -> &Term {
        self.term_buffer
    }
}

#[cfg(test)]
mod tests {
    use super::{split_json_path, JsonTermWriter};
    use crate::schema::{Field, Type};
    use crate::Term;

    #[test]
    fn test_json_writer() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("attributes");
        json_writer.push_path_segment("color");
        json_writer.set_str("red");
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(field=1, type=Json, path=attributes.color, type=Str, \"red\")"
        );
        json_writer.set_str("blue");
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(field=1, type=Json, path=attributes.color, type=Str, \"blue\")"
        );
        json_writer.pop_path_segment();
        json_writer.push_path_segment("dimensions");
        json_writer.push_path_segment("width");
        json_writer.set_fast_value(400i64);
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(field=1, type=Json, path=attributes.dimensions.width, type=I64, 400)"
        );
        json_writer.pop_path_segment();
        json_writer.push_path_segment("height");
        json_writer.set_fast_value(300i64);
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(field=1, type=Json, path=attributes.dimensions.height, type=I64, 300)"
        );
    }

    #[test]
    fn test_string_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        json_writer.set_str("red");
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jcolor\x00sred"
        )
    }

    #[test]
    fn test_i64_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(-4i64);
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jcolor\x00i\x7f\xff\xff\xff\xff\xff\xff\xfc"
        )
    }

    #[test]
    fn test_u64_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(4u64);
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jcolor\x00u\x00\x00\x00\x00\x00\x00\x00\x04"
        )
    }

    #[test]
    fn test_f64_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(4.0f64);
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jcolor\x00f\xc0\x10\x00\x00\x00\x00\x00\x00"
        )
    }

    #[test]
    fn test_bool_term() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        json_writer.set_fast_value(true);
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jcolor\x00o\x00\x00\x00\x00\x00\x00\x00\x01"
        )
    }

    #[test]
    fn test_push_after_set_path_segment() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("attribute");
        json_writer.set_str("something");
        json_writer.push_path_segment("color");
        json_writer.set_str("red");
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jattribute\x01color\x00sred"
        )
    }

    #[test]
    fn test_pop_segment() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        json_writer.push_path_segment("hue");
        json_writer.pop_path_segment();
        json_writer.set_str("red");
        assert_eq!(
            json_writer.term().serialized_term(),
            b"\x00\x00\x00\x01jcolor\x00sred"
        )
    }

    #[test]
    fn test_json_writer_path() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color");
        assert_eq!(json_writer.path(), b"color");
        json_writer.push_path_segment("hue");
        assert_eq!(json_writer.path(), b"color\x01hue");
        json_writer.set_str("pink");
        assert_eq!(json_writer.path(), b"color\x01hue");
    }

    #[test]
    fn test_json_path_expand_dots_disabled() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, false);
        json_writer.push_path_segment("color.hue");
        assert_eq!(json_writer.path(), b"color.hue");
    }

    #[test]
    fn test_json_path_expand_dots_enabled() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, true);
        json_writer.push_path_segment("color.hue");
        assert_eq!(json_writer.path(), b"color\x01hue");
    }

    #[test]
    fn test_json_path_expand_dots_enabled_pop_segment() {
        let field = Field::from_field_id(1);
        let mut term = Term::with_type_and_field(Type::Json, field);
        let mut json_writer = JsonTermWriter::wrap(&mut term, true);
        json_writer.push_path_segment("hello");
        assert_eq!(json_writer.path(), b"hello");
        json_writer.push_path_segment("color.hue");
        assert_eq!(json_writer.path(), b"hello\x01color\x01hue");
        json_writer.pop_path_segment();
        assert_eq!(json_writer.path(), b"hello");
    }

    #[test]
    fn test_split_json_path_simple() {
        let json_path = split_json_path("titi.toto");
        assert_eq!(&json_path, &["titi", "toto"]);
    }

    #[test]
    fn test_split_json_path_single_segment() {
        let json_path = split_json_path("toto");
        assert_eq!(&json_path, &["toto"]);
    }

    #[test]
    fn test_split_json_path_trailing_dot() {
        let json_path = split_json_path("toto.");
        assert_eq!(&json_path, &["toto", ""]);
    }

    #[test]
    fn test_split_json_path_heading_dot() {
        let json_path = split_json_path(".toto");
        assert_eq!(&json_path, &["", "toto"]);
    }

    #[test]
    fn test_split_json_path_escaped_dot() {
        let json_path = split_json_path(r"toto\.titi");
        assert_eq!(&json_path, &["toto.titi"]);
        let json_path_2 = split_json_path(r"k8s\.container\.name");
        assert_eq!(&json_path_2, &["k8s.container.name"]);
    }

    #[test]
    fn test_split_json_path_escaped_backslash() {
        let json_path = split_json_path(r"toto\\titi");
        assert_eq!(&json_path, &[r"toto\titi"]);
    }

    #[test]
    fn test_split_json_path_escaped_normal_letter() {
        let json_path = split_json_path(r"toto\titi");
        assert_eq!(&json_path, &[r#"tototiti"#]);
    }
}
