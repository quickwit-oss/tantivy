use columnar::MonotonicallyMappableToU64;
use common::{replace_in_place, JsonPathWriter};
use rustc_hash::FxHashMap;

use crate::fastfield::FastValue;
use crate::postings::{IndexingContext, IndexingPosition, PostingsWriter};
use crate::schema::document::{ReferenceValue, ReferenceValueLeaf, Value};
use crate::schema::term::JSON_PATH_SEGMENT_SEP;
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
    fn get_position_from_id(&mut self, id: u32) -> &mut IndexingPosition {
        self.positions_per_path.entry(id).or_default()
    }
}

/// Convert JSON_PATH_SEGMENT_SEP to a dot.
pub fn json_path_sep_to_dot(path: &mut str) {
    // This is safe since we are replacing a ASCII character by another ASCII character.
    unsafe {
        replace_in_place(JSON_PATH_SEGMENT_SEP, b'.', path.as_bytes_mut());
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn index_json_values<'a, V: Value<'a>>(
    doc: DocId,
    json_visitors: impl Iterator<Item = crate::Result<V::ObjectIter>>,
    text_analyzer: &mut TextAnalyzer,
    expand_dots_enabled: bool,
    term_buffer: &mut Term,
    postings_writer: &mut dyn PostingsWriter,
    json_path_writer: &mut JsonPathWriter,
    ctx: &mut IndexingContext,
) -> crate::Result<()> {
    json_path_writer.clear();
    json_path_writer.set_expand_dots(expand_dots_enabled);
    let mut positions_per_path: IndexingPositionsPerPath = Default::default();
    for json_visitor_res in json_visitors {
        let json_visitor = json_visitor_res?;
        index_json_object::<V>(
            doc,
            json_visitor,
            text_analyzer,
            term_buffer,
            json_path_writer,
            postings_writer,
            ctx,
            &mut positions_per_path,
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn index_json_object<'a, V: Value<'a>>(
    doc: DocId,
    json_visitor: V::ObjectIter,
    text_analyzer: &mut TextAnalyzer,
    term_buffer: &mut Term,
    json_path_writer: &mut JsonPathWriter,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
    positions_per_path: &mut IndexingPositionsPerPath,
) {
    for (json_path_segment, json_value_visitor) in json_visitor {
        json_path_writer.push(json_path_segment);
        index_json_value(
            doc,
            json_value_visitor,
            text_analyzer,
            term_buffer,
            json_path_writer,
            postings_writer,
            ctx,
            positions_per_path,
        );
        json_path_writer.pop();
    }
}

#[allow(clippy::too_many_arguments)]
fn index_json_value<'a, V: Value<'a>>(
    doc: DocId,
    json_value: V,
    text_analyzer: &mut TextAnalyzer,
    term_buffer: &mut Term,
    json_path_writer: &mut JsonPathWriter,
    postings_writer: &mut dyn PostingsWriter,
    ctx: &mut IndexingContext,
    positions_per_path: &mut IndexingPositionsPerPath,
) {
    let set_path_id = |term_buffer: &mut Term, unordered_id: u32| {
        term_buffer.truncate_value_bytes(0);
        term_buffer.append_bytes(&unordered_id.to_be_bytes());
    };
    let set_type = |term_buffer: &mut Term, typ: Type| {
        term_buffer.append_bytes(&[typ.to_code()]);
    };

    match json_value.as_value() {
        ReferenceValue::Leaf(leaf) => match leaf {
            ReferenceValueLeaf::Null => {}
            ReferenceValueLeaf::Str(val) => {
                let mut token_stream = text_analyzer.token_stream(val);
                let unordered_id = ctx
                    .path_to_unordered_id
                    .get_or_allocate_unordered_id(json_path_writer.as_str());

                // TODO: make sure the chain position works out.
                set_path_id(term_buffer, unordered_id);
                set_type(term_buffer, Type::Str);
                let indexing_position = positions_per_path.get_position_from_id(unordered_id);
                postings_writer.index_text(
                    doc,
                    &mut *token_stream,
                    term_buffer,
                    ctx,
                    indexing_position,
                );
            }
            ReferenceValueLeaf::U64(val) => {
                set_path_id(
                    term_buffer,
                    ctx.path_to_unordered_id
                        .get_or_allocate_unordered_id(json_path_writer.as_str()),
                );
                term_buffer.append_type_and_fast_value(val);
                postings_writer.subscribe(doc, 0u32, term_buffer, ctx);
            }
            ReferenceValueLeaf::I64(val) => {
                set_path_id(
                    term_buffer,
                    ctx.path_to_unordered_id
                        .get_or_allocate_unordered_id(json_path_writer.as_str()),
                );
                term_buffer.append_type_and_fast_value(val);
                postings_writer.subscribe(doc, 0u32, term_buffer, ctx);
            }
            ReferenceValueLeaf::F64(val) => {
                set_path_id(
                    term_buffer,
                    ctx.path_to_unordered_id
                        .get_or_allocate_unordered_id(json_path_writer.as_str()),
                );
                term_buffer.append_type_and_fast_value(val);
                postings_writer.subscribe(doc, 0u32, term_buffer, ctx);
            }
            ReferenceValueLeaf::Bool(val) => {
                set_path_id(
                    term_buffer,
                    ctx.path_to_unordered_id
                        .get_or_allocate_unordered_id(json_path_writer.as_str()),
                );
                term_buffer.append_type_and_fast_value(val);
                postings_writer.subscribe(doc, 0u32, term_buffer, ctx);
            }
            ReferenceValueLeaf::Date(val) => {
                set_path_id(
                    term_buffer,
                    ctx.path_to_unordered_id
                        .get_or_allocate_unordered_id(json_path_writer.as_str()),
                );
                term_buffer.append_type_and_fast_value(val);
                postings_writer.subscribe(doc, 0u32, term_buffer, ctx);
            }
            ReferenceValueLeaf::PreTokStr(_) => {
                unimplemented!(
                    "Pre-tokenized string support in dynamic fields is not yet implemented"
                )
            }
            ReferenceValueLeaf::Bytes(_) => {
                unimplemented!("Bytes support in dynamic fields is not yet implemented")
            }
            ReferenceValueLeaf::Facet(_) => {
                unimplemented!("Facet support in dynamic fields is not yet implemented")
            }
            ReferenceValueLeaf::IpAddr(_) => {
                unimplemented!("IP address support in dynamic fields is not yet implemented")
            }
        },
        ReferenceValue::Array(elements) => {
            for val in elements {
                index_json_value(
                    doc,
                    val,
                    text_analyzer,
                    term_buffer,
                    json_path_writer,
                    postings_writer,
                    ctx,
                    positions_per_path,
                );
            }
        }
        ReferenceValue::Object(object) => {
            index_json_object::<V>(
                doc,
                object,
                text_analyzer,
                term_buffer,
                json_path_writer,
                postings_writer,
                ctx,
                positions_per_path,
            );
        }
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
pub fn split_json_path(json_path: &str) -> Vec<String> {
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
    let mut path = JsonPathWriter::default();
    path.push(field_name);
    path.set_expand_dots(expand_dots_enabled);
    for segment in split_json_path(json_path) {
        path.push(&segment);
    }
    path.into()
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

    // TODO: Remove this function and use JsonPathWriter instead.
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
