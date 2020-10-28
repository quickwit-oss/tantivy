use std::fmt;

use super::Field;
use crate::common;
use crate::schema::Facet;
use crate::DateTime;
use std::str;

/// Size (in bytes) of the buffer of a int field.
const INT_TERM_LEN: usize = 4 + 8;

/// Term represents the value that the token can take.
///
/// It actually wraps a `Vec<u8>`.
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Term<B = Vec<u8>>(B)
where
    B: AsRef<[u8]>;

impl Term {
    pub(crate) fn new() -> Term {
        Term(Vec::with_capacity(100))
    }

    /// Builds a term given a field, and a i64-value
    ///
    /// Assuming the term has a field id of 1, and a i64 value of 3234,
    /// the Term will have 12 bytes.
    ///
    /// The first four byte are dedicated to storing the field id as a u64.
    /// The 8 following bytes are encoding the u64 value.
    pub fn from_field_i64(field: Field, val: i64) -> Term {
        let val_u64: u64 = common::i64_to_u64(val);
        Term::from_field_u64(field, val_u64)
    }

    /// Builds a term given a field, and a f64-value
    ///
    /// Assuming the term has a field id of 1, and a f64 value of 1.5,
    /// the Term will have 12 bytes.
    ///
    /// The first four byte are dedicated to storing the field id as a u64.
    /// The 8 following bytes are encoding the f64 as a u64 value.
    pub fn from_field_f64(field: Field, val: f64) -> Term {
        let val_u64: u64 = common::f64_to_u64(val);
        Term::from_field_u64(field, val_u64)
    }

    /// Builds a term given a field, and a DateTime value
    ///
    /// Assuming the term has a field id of 1, and a timestamp i64 value of 3234,
    /// the Term will have 12 bytes.
    ///
    /// The first four byte are dedicated to storing the field id as a u64.
    /// The 8 following bytes are encoding the DateTime as i64 timestamp value.
    pub fn from_field_date(field: Field, val: &DateTime) -> Term {
        let val_timestamp = val.timestamp();
        Term::from_field_i64(field, val_timestamp)
    }

    /// Creates a `Term` given a facet.
    pub fn from_facet(field: Field, facet: &Facet) -> Term {
        let bytes = facet.encoded_str().as_bytes();
        let buffer = Vec::with_capacity(4 + bytes.len());
        let mut term = Term(buffer);
        term.set_field(field);
        term.set_bytes(bytes);
        term
    }

    /// Builds a term given a field, and a string value
    ///
    /// Assuming the term has a field id of 2, and a text value of "abc",
    /// the Term will have 4 bytes.
    /// The first byte is 2, and the three following bytes are the utf-8
    /// representation of "abc".
    pub fn from_field_text(field: Field, text: &str) -> Term {
        let buffer = Vec::with_capacity(4 + text.len());
        let mut term = Term(buffer);
        term.set_field(field);
        term.set_text(text);
        term
    }

    /// Builds a term given a field, and a u64-value
    ///
    /// Assuming the term has a field id of 1, and a u64 value of 3234,
    /// the Term will have 12 bytes.
    ///
    /// The first four byte are dedicated to storing the field id as a u64.
    /// The 8 following bytes are encoding the u64 value.
    pub fn from_field_u64(field: Field, val: u64) -> Term {
        let mut term = Term(vec![0u8; INT_TERM_LEN]);
        term.set_field(field);
        term.set_u64(val);
        term
    }

    /// Builds a term bytes.
    pub fn from_field_bytes(field: Field, bytes: &[u8]) -> Term {
        let mut term = Term::for_field(field);
        term.set_bytes(bytes);
        term
    }

    /// Creates a new Term for a given field.
    pub(crate) fn for_field(field: Field) -> Term {
        let mut term = Term(Vec::with_capacity(100));
        term.set_field(field);
        term
    }

    pub(crate) fn set_field(&mut self, field: Field) {
        self.0.clear();
        self.0
            .extend_from_slice(field.field_id().to_be_bytes().as_ref());
    }

    /// Sets a u64 value in the term.
    ///
    /// U64 are serialized using (8-byte) BigEndian
    /// representation.
    /// The use of BigEndian has the benefit of preserving
    /// the natural order of the values.
    pub fn set_u64(&mut self, val: u64) {
        self.0.resize(INT_TERM_LEN, 0u8);
        self.set_bytes(val.to_be_bytes().as_ref());
    }

    /// Sets a `i64` value in the term.
    pub fn set_i64(&mut self, val: i64) {
        self.set_u64(common::i64_to_u64(val));
    }

    /// Sets a `f64` value in the term.
    pub fn set_f64(&mut self, val: f64) {
        self.set_u64(common::f64_to_u64(val));
    }

    /// Sets the value of a `Bytes` field.
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.0.resize(4, 0u8);
        self.0.extend(bytes);
    }

    /// Set the texts only, keeping the field untouched.
    pub fn set_text(&mut self, text: &str) {
        self.set_bytes(text.as_bytes());
    }
}

impl<B> Term<B>
where
    B: AsRef<[u8]>,
{
    /// Wraps a object holding bytes
    pub fn wrap(data: B) -> Term<B> {
        Term(data)
    }

    /// Returns the field.
    pub fn field(&self) -> Field {
        let mut field_id_bytes = [0u8; 4];
        field_id_bytes.copy_from_slice(&self.0.as_ref()[..4]);
        Field::from_field_id(u32::from_be_bytes(field_id_bytes))
    }

    /// Returns the `u64` value stored in a term.
    ///
    /// # Panics
    /// ... or returns an invalid value
    /// if the term is not a `u64` field.
    pub fn get_u64(&self) -> u64 {
        let mut field_id_bytes = [0u8; 8];
        field_id_bytes.copy_from_slice(self.value_bytes());
        u64::from_be_bytes(field_id_bytes)
    }

    /// Returns the `i64` value stored in a term.
    ///
    /// # Panics
    /// ... or returns an invalid value
    /// if the term is not a `i64` field.
    pub fn get_i64(&self) -> i64 {
        common::u64_to_i64(self.get_u64())
    }

    /// Returns the `f64` value stored in a term.
    ///
    /// # Panics
    /// ... or returns an invalid value
    /// if the term is not a `f64` field.
    pub fn get_f64(&self) -> f64 {
        common::u64_to_f64(self.get_u64())
    }

    /// Returns the text associated with the term.
    ///
    /// # Panics
    /// If the value is not valid utf-8. This may happen
    /// if the index is corrupted or if you try to
    /// call this method on a non-string type.
    pub fn text(&self) -> &str {
        str::from_utf8(self.value_bytes()).expect("Term does not contain valid utf-8.")
    }

    /// Returns the serialized value of the term.
    /// (this does not include the field.)
    ///
    /// If the term is a string, its value is utf-8 encoded.
    /// If the term is a u64, its value is encoded according
    /// to `byteorder::LittleEndian`.
    pub fn value_bytes(&self) -> &[u8] {
        &self.0.as_ref()[4..]
    }

    /// Returns the underlying `&[u8]`
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<B> AsRef<[u8]> for Term<B>
where
    B: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Term(field={},bytes={:?})",
            self.field().field_id(),
            self.value_bytes()
        )
    }
}

#[cfg(test)]
mod tests {

    use crate::schema::*;

    #[test]
    pub fn test_term() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let count_field = schema_builder.add_text_field("count", STRING);
        {
            let term = Term::from_field_text(title_field, "test");
            assert_eq!(term.field(), title_field);
            assert_eq!(&term.as_slice()[0..4], &[0u8, 0u8, 0u8, 1u8]);
            assert_eq!(&term.as_slice()[4..], "test".as_bytes());
        }
        {
            let term = Term::from_field_u64(count_field, 983u64);
            assert_eq!(term.field(), count_field);
            assert_eq!(&term.as_slice()[0..4], &[0u8, 0u8, 0u8, 2u8]);
            assert_eq!(term.as_slice().len(), 4 + 8);
            assert_eq!(term.as_slice()[4], 0u8);
            assert_eq!(term.as_slice()[5], 0u8);
            assert_eq!(term.as_slice()[6], 0u8);
            assert_eq!(term.as_slice()[7], 0u8);
            assert_eq!(term.as_slice()[8], 0u8);
            assert_eq!(term.as_slice()[9], 0u8);
            assert_eq!(term.as_slice()[10], (933u64 / 256u64) as u8);
            assert_eq!(term.as_slice()[11], (983u64 % 256u64) as u8);
        }
    }
}
