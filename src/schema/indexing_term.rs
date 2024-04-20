use std::net::Ipv6Addr;

use columnar::{MonotonicallyMappableToU128, MonotonicallyMappableToU64};

use super::date_time_options::DATE_TIME_PRECISION_INDEXED;
use super::Field;
use crate::fastfield::FastValue;
use crate::schema::Type;
use crate::DateTime;

/// Term represents the value that the token can take.
/// It's a serialized representation over different types.
///
/// It actually wraps a `Vec<u8>`. The first 5 bytes are metadata.
/// 4 bytes are the field id, and the last byte is the type.
///
/// The serialized value `ValueBytes` is considered everything after the 4 first bytes (term id).
#[derive(Clone)]
pub struct IndexingTerm<B = Vec<u8>>(B)
where
    B: AsRef<[u8]>;

/// The number of bytes used as metadata by `Term`.
const TERM_METADATA_LENGTH: usize = 5;

impl IndexingTerm {
    /// Create a new Term with a buffer with a given capacity.
    pub fn with_capacity(capacity: usize) -> IndexingTerm {
        let mut data = Vec::with_capacity(TERM_METADATA_LENGTH + capacity);
        data.resize(TERM_METADATA_LENGTH, 0u8);
        IndexingTerm(data)
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

    /// Removes the value_bytes and set the field and type code.
    pub(crate) fn clear_with_field_and_type(&mut self, typ: Type, field: Field) {
        self.truncate_value_bytes(0);
        self.set_field_and_type(field, typ);
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

    /// Sets a `i64` value in the term.
    pub fn set_i64(&mut self, val: i64) {
        self.set_fast_value(val);
    }

    /// Sets a `f64` value in the term.
    pub fn set_f64(&mut self, val: f64) {
        self.set_fast_value(val);
    }

    /// Sets a `bool` value in the term.
    pub fn set_bool(&mut self, val: bool) {
        self.set_fast_value(val);
    }

    fn set_fast_value<T: FastValue>(&mut self, val: T) {
        self.set_bytes(val.to_u64().to_be_bytes().as_ref());
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

    /// Sets a `Ipv6Addr` value in the term.
    pub fn set_ip_addr(&mut self, val: Ipv6Addr) {
        self.set_bytes(val.to_u128().to_be_bytes().as_ref());
    }

    /// Sets the value of a `Bytes` field.
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.truncate_value_bytes(0);
        self.0.extend(bytes);
    }

    /// Truncates the value bytes of the term. Value and field type stays the same.
    pub fn truncate_value_bytes(&mut self, len: usize) {
        self.0.truncate(len + TERM_METADATA_LENGTH);
    }

    /// The length of the bytes.
    pub fn len_bytes(&self) -> usize {
        self.0.len() - TERM_METADATA_LENGTH
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
}

impl<B> IndexingTerm<B>
where
    B: AsRef<[u8]>,
{
    /// Wraps a object holding bytes
    pub fn wrap(data: B) -> IndexingTerm<B> {
        IndexingTerm(data)
    }

    /// Returns the field.
    pub fn field(&self) -> Field {
        let field_id_bytes: [u8; 4] = (&self.0.as_ref()[..4]).try_into().unwrap();
        Field::from_field_id(u32::from_be_bytes(field_id_bytes))
    }

    /// Returns the serialized representation of Term.
    /// This includes field_id, value type and value.
    ///
    /// Do NOT rely on this byte representation in the index.
    /// This value is likely to change in the future.
    #[inline]
    pub fn serialized_term(&self) -> &[u8] {
        self.0.as_ref()
    }
}
