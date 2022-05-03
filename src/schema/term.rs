use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::{fmt, str};

use super::Field;
use crate::fastfield::FastValue;
use crate::schema::{Facet, Type};
use crate::DateTime;

/// Size (in bytes) of the buffer of a fast value (u64, i64, f64, or date) term.
/// <field> + <type byte> + <value len>
///
/// - <field> is a big endian encoded u32 field id
/// - <type_byte>'s most significant bit expresses whether the term is a json term or not
/// The remaining 7 bits are used to encode the type of the value.
/// If this is a JSON term, the type is the type of the leaf of the json.
///
/// - <value> is,  if this is not the json term, a binary representation specific to the type.
/// If it is a JSON Term, then it is preprended with the path that leads to this leaf value.
const FAST_VALUE_TERM_LEN: usize = 8;

/// Separates the different segments of
/// the json path.
pub const JSON_PATH_SEGMENT_SEP: u8 = 1u8;
pub const JSON_PATH_SEGMENT_SEP_STR: &str =
    unsafe { std::str::from_utf8_unchecked(&[JSON_PATH_SEGMENT_SEP]) };

/// Separates the json path and the value in
/// a JSON term binary representation.
pub const JSON_END_OF_PATH: u8 = 0u8;

/// Term represents the value that the token can take.
///
/// It actually wraps a `Vec<u8>`.
#[derive(Clone)]
pub struct Term<B = Vec<u8>> {
    data: B,
    field: Field,
    field_type: Type,
}

impl AsMut<Vec<u8>> for Term {
    fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

impl Term {
    pub(crate) fn new() -> Term {
        Self::with_capacity(32)
    }

    pub(crate) fn with_capacity(cap: usize) -> Term {
        Term {
            data: Vec::with_capacity(cap),
            field: Field::from_field_id(0),
            field_type: Type::Str,
        }
    }

    fn from_fast_value<T: FastValue>(field: Field, val: &T) -> Term {
        let mut term = Term::with_capacity(FAST_VALUE_TERM_LEN);
        term.set_field(T::to_type(), field);
        term.set_u64(val.to_u64());
        term
    }

    /// Builds a term given a field, and a u64-value
    pub fn from_field_u64(field: Field, val: u64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a i64-value
    pub fn from_field_i64(field: Field, val: i64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a f64-value
    pub fn from_field_f64(field: Field, val: f64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a DateTime value
    pub fn from_field_date(field: Field, val: DateTime) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Creates a `Term` given a facet.
    pub fn from_facet(field: Field, facet: &Facet) -> Term {
        let facet_encoded_str = facet.encoded_str();
        Term::create_bytes_term(Type::Facet, field, facet_encoded_str.as_bytes())
    }

    /// Builds a term given a field, and a string value
    pub fn from_field_text(field: Field, text: &str) -> Term {
        Term::create_bytes_term(Type::Str, field, text.as_bytes())
    }

    fn create_bytes_term(typ: Type, field: Field, bytes: &[u8]) -> Term {
        let mut term = Term::with_capacity(bytes.len());
        term.set_field(typ, field);
        term.data.extend_from_slice(bytes);
        term
    }

    /// Builds a term bytes.
    pub fn from_field_bytes(field: Field, bytes: &[u8]) -> Term {
        Term::create_bytes_term(Type::Bytes, field, bytes)
    }

    pub(crate) fn set_field(&mut self, typ: Type, field: Field) {
        self.field = field;
        self.field_type = typ;
        self.data.clear();
    }

    /// Sets a u64 value in the term.
    ///
    /// U64 are serialized using (8-byte) BigEndian
    /// representation.
    /// The use of BigEndian has the benefit of preserving
    /// the natural order of the values.
    pub fn set_u64(&mut self, val: u64) {
        self.set_fast_value(val);
    }

    fn set_fast_value<T: FastValue>(&mut self, val: T) {
        self.set_bytes(val.to_u64().to_be_bytes().as_ref());
    }

    /// Sets a `i64` value in the term.
    pub fn set_i64(&mut self, val: i64) {
        self.set_fast_value(val);
    }

    /// Sets a `i64` value in the term.
    pub fn set_date(&mut self, date: DateTime) {
        self.set_fast_value(date);
    }

    /// Sets a `f64` value in the term.
    pub fn set_f64(&mut self, val: f64) {
        self.set_fast_value(val);
    }

    /// Sets the value of a `Bytes` field.
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.data.clear();
        self.data.extend(bytes);
    }

    /// Set the texts only, keeping the field untouched.
    pub fn set_text(&mut self, text: &str) {
        self.set_bytes(text.as_bytes());
    }

    /// Removes the value_bytes and set the type code.
    pub fn clear_with_type(&mut self, typ: Type) {
        self.data.clear();
        self.field_type = typ;
    }

    /// Truncate the term right after the field and the type code.
    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
    }

    /// Truncate the term right after the field and the type code.
    pub fn append_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }
}

impl<B> Ord for Term<B>
where B: AsRef<[u8]>
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value_bytes().cmp(other.value_bytes())
    }
}

impl<B> PartialOrd for Term<B>
where B: AsRef<[u8]>
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<B> PartialEq for Term<B>
where B: AsRef<[u8]>
{
    fn eq(&self, other: &Self) -> bool {
        self.value_bytes() == other.value_bytes()
    }
}

impl<B> Eq for Term<B> where B: AsRef<[u8]> {}

impl<B> Hash for Term<B>
where B: AsRef<[u8]>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.as_ref().hash(state)
    }
}

impl<B> Term<B>
where B: AsRef<[u8]>
{
    /// Wraps a object holding bytes
    pub fn wrap(data: B) -> Term<B> {
        Term {
            data,
            field: Field::from_field_id(0),
            field_type: Type::Str,
        }
    }

    fn typ_code(&self) -> u8 {
        self.field_type as u8
    }

    /// Return the type of the term.
    pub fn typ(&self) -> Type {
        Type::from_code(self.typ_code()).expect("The term has an invalid type code")
    }

    /// Returns the field.
    pub fn field(&self) -> Field {
        self.field
    }

    /// Returns the text associated with the term.
    ///
    /// Returns None if the field is not of string type
    /// or if the bytes are not valid utf-8.
    pub fn as_str(&self) -> Option<&str> {
        if self.typ() != Type::Str {
            return None;
        }
        str::from_utf8(self.value_bytes()).ok()
    }

    /// Returns the serialized value of the term.
    /// (this does not include the field.)
    ///
    /// If the term is a string, its value is utf-8 encoded.
    /// If the term is a u64, its value is encoded according
    /// to `byteorder::LittleEndian`.
    pub fn value_bytes(&self) -> &[u8] {
        &self.data.as_ref()
    }
}

fn write_opt<T: std::fmt::Debug>(f: &mut fmt::Formatter, val_opt: Option<T>) -> fmt::Result {
    if let Some(val) = val_opt {
        write!(f, "{:?}", val)?;
    }
    Ok(())
}

fn as_str(value_bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(value_bytes).ok()
}

fn get_fast_type<T: FastValue>(bytes: &[u8]) -> Option<T> {
    let value_u64 = u64::from_be_bytes(bytes.try_into().ok()?);
    Some(FastValue::from_u64(value_u64))
}

/// Returns the json path (without non-human friendly separators, the type of the value, and the
/// value bytes). Returns None if the value is not JSON or is not valid.
pub(crate) fn as_json_path_type_value_bytes(bytes: &[u8]) -> Option<(&str, Type, &[u8])> {
    let pos = bytes.iter().cloned().position(|b| b == JSON_END_OF_PATH)?;
    let json_path = str::from_utf8(&bytes[..pos]).ok()?;
    let type_code = *bytes.get(pos + 1)?;
    let typ = Type::from_code(type_code)?;
    Some((json_path, typ, &bytes[pos + 2..]))
}

fn debug_value_bytes(typ: Type, bytes: &[u8], f: &mut fmt::Formatter) -> fmt::Result {
    match typ {
        Type::Str => {
            let s = as_str(bytes);
            write_opt(f, s)?;
        }
        Type::U64 => {
            write_opt(f, get_fast_type::<u64>(bytes))?;
        }
        Type::I64 => {
            write_opt(f, get_fast_type::<i64>(bytes))?;
        }
        Type::F64 => {
            write_opt(f, get_fast_type::<f64>(bytes))?;
        }
        // TODO pretty print these types too.
        Type::Date => {
            write_opt(f, get_fast_type::<DateTime>(bytes))?;
        }
        Type::Facet => {
            let facet_str = str::from_utf8(bytes)
                .ok()
                .map(ToString::to_string)
                .map(Facet::from_encoded_string)
                .map(|facet| facet.to_path_string());
            write_opt(f, facet_str)?;
        }
        Type::Bytes => {
            write_opt(f, Some(bytes))?;
        }
        Type::Json => {
            if let Some((path, typ, bytes)) = as_json_path_type_value_bytes(bytes) {
                let path_pretty = path.replace(JSON_PATH_SEGMENT_SEP_STR, ".");
                write!(f, "path={path_pretty}, vtype={typ:?}, ")?;
                debug_value_bytes(typ, bytes, f)?;
            }
        }
    }
    Ok(())
}

impl<B> fmt::Debug for Term<B>
where B: AsRef<[u8]>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let field_id = self.field().field_id();
        let typ = self.typ();
        write!(f, "Term(type={typ:?}, field={field_id}, ")?;
        debug_value_bytes(typ, self.value_bytes(), f)?;
        write!(f, ")",)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::schema::*;

    #[test]
    pub fn test_term_str() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let term = Term::from_field_text(title_field, "test");
        assert_eq!(term.field(), title_field);
        assert_eq!(term.typ(), Type::Str);
        assert_eq!(term.as_str(), Some("test"))
    }

    #[test]
    pub fn test_term_u64() {
        let mut schema_builder = Schema::builder();
        let count_field = schema_builder.add_u64_field("count", INDEXED);
        let term = Term::from_field_u64(count_field, 983u64);
        assert_eq!(term.field(), count_field);
        assert_eq!(term.typ(), Type::U64);
        assert_eq!(term.value_bytes().len(), super::FAST_VALUE_TERM_LEN);
    }
}
