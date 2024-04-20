use std::hash::Hash;
use std::net::Ipv6Addr;
use std::{fmt, str};

use columnar::{MonotonicallyMappableToU128, MonotonicallyMappableToU64};
use common::json_path_writer::{JSON_END_OF_PATH, JSON_PATH_SEGMENT_SEP_STR};

use super::date_time_options::DATE_TIME_PRECISION_INDEXED;
use super::Field;
use crate::fastfield::FastValue;
use crate::schema::{Facet, Type};
use crate::DateTime;

/// Term represents the value that the token can take.
/// It's a serialized representation over different types.
///
/// It actually wraps a `Vec<u8>`. The first 5 bytes are metadata.
/// 4 bytes are the field id, and the last byte is the type.
///
/// The serialized value `ValueBytes` is considered everything after the 4 first bytes (term id).
#[derive(Clone, Hash, PartialEq, Ord, PartialOrd, Eq)]
pub struct Term(Vec<u8>);

/// The number of bytes used as metadata by `Term`.
const TERM_METADATA_LENGTH: usize = 5;

impl Term {
    /// Create a new Term
    pub fn new() -> Term {
        let mut data = Vec::with_capacity(TERM_METADATA_LENGTH + 32);
        data.resize(TERM_METADATA_LENGTH, 0u8);
        Term(data)
    }

    pub(crate) fn with_type_and_field(typ: Type, field: Field) -> Term {
        Self::with_bytes_and_field_and_payload(typ, field, &[])
    }

    fn with_bytes_and_field_and_payload(typ: Type, field: Field, bytes: &[u8]) -> Term {
        let mut term = Self::new();
        term.set_field_and_type(field, typ);
        term.0.extend_from_slice(bytes);
        term
    }

    /// Sets a fast value in the term.
    ///
    /// fast values are converted to u64 and then serialized using (8-byte) BigEndian
    /// representation.
    /// The use of BigEndian has the benefit of preserving
    /// the natural order of the values.
    fn from_fast_value<T: FastValue>(field: Field, val: &T) -> Term {
        let mut term = Self::with_type_and_field(T::to_type(), field);
        term.set_bytes(val.to_u64().to_be_bytes().as_ref());
        term
    }

    /// Panics when the term is not empty... ie: some value is set.
    /// Use `clear_with_field_and_type` in that case.
    ///
    /// Sets field and the type.
    pub(crate) fn set_field_and_type(&mut self, field: Field, typ: Type) {
        assert!(self.is_empty());
        self.0[0..4].clone_from_slice(field.field_id().to_be_bytes().as_ref());
        self.0[4] = typ.to_code();
    }

    /// Is empty if there are no value bytes.
    pub fn is_empty(&self) -> bool {
        self.0.len() == TERM_METADATA_LENGTH
    }

    /// Builds a term given a field, and a `Ipv6Addr`-value
    pub fn from_field_ip_addr(field: Field, ip_addr: Ipv6Addr) -> Term {
        let mut term = Self::with_type_and_field(Type::IpAddr, field);
        term.set_bytes(ip_addr.to_u128().to_be_bytes().as_ref());
        term
    }

    /// Builds a term given a field, and a `u64`-value
    pub fn from_field_u64(field: Field, val: u64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `i64`-value
    pub fn from_field_i64(field: Field, val: i64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `f64`-value
    pub fn from_field_f64(field: Field, val: f64) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `bool`-value
    pub fn from_field_bool(field: Field, val: bool) -> Term {
        Term::from_fast_value(field, &val)
    }

    /// Builds a term given a field, and a `DateTime` value
    pub fn from_field_date(field: Field, val: DateTime) -> Term {
        Term::from_fast_value(field, &val.truncate(DATE_TIME_PRECISION_INDEXED))
    }

    /// Creates a `Term` given a facet.
    pub fn from_facet(field: Field, facet: &Facet) -> Term {
        let facet_encoded_str = facet.encoded_str();
        Term::with_bytes_and_field_and_payload(Type::Facet, field, facet_encoded_str.as_bytes())
    }

    /// Builds a term given a field, and a string value
    pub fn from_field_text(field: Field, text: &str) -> Term {
        Term::with_bytes_and_field_and_payload(Type::Str, field, text.as_bytes())
    }

    /// Builds a term bytes.
    pub fn from_field_bytes(field: Field, bytes: &[u8]) -> Term {
        Term::with_bytes_and_field_and_payload(Type::Bytes, field, bytes)
    }

    /// Removes the value_bytes and set the type code.
    pub fn clear_with_type(&mut self, typ: Type) {
        self.truncate_value_bytes(0);
        self.0[4] = typ.to_code();
    }

    /// Append a type marker + fast value to a term.
    /// This is used in JSON type to append a fast value after the path.
    ///
    /// It will not clear existing bytes.
    pub(crate) fn append_type_and_fast_value<T: FastValue>(&mut self, val: T) {
        self.0.push(T::to_type().to_code());
        let value = if T::to_type() == Type::Date {
            DateTime::from_u64(val.to_u64())
                .truncate(DATE_TIME_PRECISION_INDEXED)
                .to_u64()
        } else {
            val.to_u64()
        };
        self.0.extend(value.to_be_bytes().as_ref());
    }

    /// Append a string type marker + string to a term.
    /// This is used in JSON type to append a str after the path.
    ///
    /// It will not clear existing bytes.
    pub(crate) fn append_type_and_str(&mut self, val: &str) {
        self.0.push(Type::Str.to_code());
        self.0.extend(val.as_bytes().as_ref());
    }

    /// Sets the value of a `Bytes` field.
    fn set_bytes(&mut self, bytes: &[u8]) {
        self.truncate_value_bytes(0);
        self.0.extend(bytes);
    }

    /// Truncates the value bytes of the term. Value and field type stays the same.
    pub fn truncate_value_bytes(&mut self, len: usize) {
        self.0.truncate(len + TERM_METADATA_LENGTH);
    }

    /// Appends value bytes to the Term.
    ///
    /// This function returns the segment that has just been added.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) -> &mut [u8] {
        let len_before = self.0.len();
        self.0.extend_from_slice(bytes);
        &mut self.0[len_before..]
    }

    /// Return the type of the term.
    pub fn typ(&self) -> Type {
        self.value().typ()
    }

    /// Returns the serialized representation of the value.
    /// (this does neither include the field id nor the value type.)
    ///
    /// If the term is a string, its value is utf-8 encoded.
    /// If the term is a u64, its value is encoded according
    /// to `byteorder::BigEndian`.
    pub(crate) fn serialized_value_bytes(&self) -> &[u8] {
        &self.0[TERM_METADATA_LENGTH..]
    }

    /// Returns the serialized representation of Term.
    /// This includes field_id, value type and value.
    ///
    /// Do NOT rely on this byte representation in the index.
    /// This value is likely to change in the future.
    #[inline]
    #[cfg(test)]
    pub fn serialized_term(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Returns the field.
    pub fn field(&self) -> Field {
        let field_id_bytes: [u8; 4] = (&self.0[..4]).try_into().unwrap();
        Field::from_field_id(u32::from_be_bytes(field_id_bytes))
    }

    /// Returns the value of the term.
    /// address or JSON path + value. (this does not include the field.)
    pub fn value(&self) -> ValueBytes<&[u8]> {
        ValueBytes::wrap(&self.0[4..])
    }
}

/// ValueBytes represents a serialized value.
/// The value can be of any type of [`Type`] (e.g. string, u64, f64, bool, date, JSON).
/// The serialized representation matches the lexographical order of the type.
///
/// The `ValueBytes` format is as follow:
/// `[type code: u8][serialized value]`
///
/// For JSON `ValueBytes` equals to:
/// `[type code=JSON][JSON path][JSON_END_OF_PATH][ValueBytes]`
///
/// The nested ValueBytes in JSON is never of type JSON. (there's no recursion)
#[derive(Clone)]
pub struct ValueBytes<B>(B)
where B: AsRef<[u8]>;

impl<B> ValueBytes<B>
where B: AsRef<[u8]>
{
    /// Wraps a object holding bytes
    pub fn wrap(data: B) -> ValueBytes<B> {
        ValueBytes(data)
    }

    fn typ_code(&self) -> u8 {
        self.0.as_ref()[0]
    }

    /// Return the type of the term.
    pub(crate) fn typ(&self) -> Type {
        Type::from_code(self.typ_code()).expect("The term has an invalid type code")
    }

    fn get_fast_type<T: FastValue>(&self) -> Option<T> {
        if self.typ() != T::to_type() {
            return None;
        }
        let value_bytes = self.value_bytes();
        let value_u64 = u64::from_be_bytes(value_bytes.try_into().ok()?);
        Some(T::from_u64(value_u64))
    }

    /// Returns the text associated with the term.
    ///
    /// Returns `None` if the field is not of string type
    /// or if the bytes are not valid utf-8.
    pub fn as_str(&self) -> Option<&str> {
        if self.typ() != Type::Str {
            return None;
        }
        str::from_utf8(self.value_bytes()).ok()
    }

    /// Returns the facet associated with the term.
    ///
    /// Returns `None` if the field is not of facet type
    /// or if the bytes are not valid utf-8.
    pub(crate) fn as_facet(&self) -> Option<Facet> {
        if self.typ() != Type::Facet {
            return None;
        }
        let facet_encode_str = str::from_utf8(self.value_bytes()).ok()?;
        Some(Facet::from_encoded_string(facet_encode_str.to_string()))
    }

    /// Returns the bytes associated with the term.
    ///
    /// Returns `None` if the field is not of bytes type.
    pub(crate) fn as_bytes(&self) -> Option<&[u8]> {
        if self.typ() != Type::Bytes {
            return None;
        }
        Some(self.value_bytes())
    }

    /// Returns a `Ipv6Addr` value from the term.
    pub(crate) fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        if self.typ() != Type::IpAddr {
            return None;
        }
        let ip_u128 = u128::from_be_bytes(self.value_bytes().try_into().ok()?);
        Some(Ipv6Addr::from_u128(ip_u128))
    }

    /// Returns the json path bytes (including the JSON_END_OF_PATH byte),
    /// and the encoded ValueBytes after the json path.
    ///
    /// Returns `None` if the value is not JSON.
    pub(crate) fn as_json(&self) -> Option<(&[u8], ValueBytes<&[u8]>)> {
        if self.typ() != Type::Json {
            return None;
        }
        let bytes = self.value_bytes();

        let pos = bytes.iter().cloned().position(|b| b == JSON_END_OF_PATH)?;
        // split at pos + 1, so that json_path_bytes includes the JSON_END_OF_PATH byte.
        let (json_path_bytes, term) = bytes.split_at(pos + 1);
        Some((json_path_bytes, ValueBytes::wrap(term)))
    }

    /// Returns the serialized value of ValueBytes without the type.
    fn value_bytes(&self) -> &[u8] {
        &self.0.as_ref()[1..]
    }

    /// Returns the serialized representation of Term.
    ///
    /// Do NOT rely on this byte representation in the index.
    /// This value is likely to change in the future.
    pub fn as_serialized(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn debug_value_bytes(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let typ = self.typ();
        write!(f, "type={typ:?}, ")?;
        match typ {
            Type::Str => {
                let s = self.as_str();
                write_opt(f, s)?;
            }
            Type::U64 => {
                write_opt(f, self.get_fast_type::<u64>())?;
            }
            Type::I64 => {
                write_opt(f, self.get_fast_type::<i64>())?;
            }
            Type::F64 => {
                write_opt(f, self.get_fast_type::<f64>())?;
            }
            Type::Bool => {
                write_opt(f, self.get_fast_type::<bool>())?;
            }
            // TODO pretty print these types too.
            Type::Date => {
                write_opt(f, self.get_fast_type::<DateTime>())?;
            }
            Type::Facet => {
                write_opt(f, self.as_facet())?;
            }
            Type::Bytes => {
                write_opt(f, self.as_bytes())?;
            }
            Type::Json => {
                if let Some((path_bytes, sub_value_bytes)) = self.as_json() {
                    // Remove the JSON_END_OF_PATH byte & convert to utf8.
                    let path = str::from_utf8(&path_bytes[..path_bytes.len() - 1])
                        .map_err(|_| std::fmt::Error)?;
                    let path_pretty = path.replace(JSON_PATH_SEGMENT_SEP_STR, ".");
                    write!(f, "path={path_pretty}, ")?;
                    sub_value_bytes.debug_value_bytes(f)?;
                }
            }
            Type::IpAddr => {
                write_opt(f, self.as_ip_addr())?;
            }
        }
        Ok(())
    }
}

fn write_opt<T: std::fmt::Debug>(f: &mut fmt::Formatter, val_opt: Option<T>) -> fmt::Result {
    if let Some(val) = val_opt {
        write!(f, "{val:?}")?;
    }
    Ok(())
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let field_id = self.field().field_id();
        write!(f, "Term(field={field_id}, ")?;
        let value_bytes = ValueBytes::wrap(&self.0[4..]);
        value_bytes.debug_value_bytes(f)?;
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
        assert_eq!(term.value().as_str(), Some("test"))
    }
}
