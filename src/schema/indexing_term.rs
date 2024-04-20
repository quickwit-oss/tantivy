use std::net::Ipv6Addr;

use columnar::{MonotonicallyMappableToU128, MonotonicallyMappableToU64};

use super::date_time_options::DATE_TIME_PRECISION_INDEXED;
use super::Field;
use crate::fastfield::FastValue;
use crate::schema::Type;
use crate::DateTime;

/// IndexingTerm represents is the serialized information of a term during indexing.
/// It's a serialized representation over different types.
///
/// It actually wraps a `Vec<u8>`.
///
/// The format is as follow:
/// `[field id: u32][serialized value]`
///
/// For JSON it equals to:
/// `[field id: u32][path id: u32][type code: u8][serialized value]`
///
/// The format is chosen to easily partition the terms by field during serialization, as all terms
/// are stored in one hashmap.
#[derive(Clone)]
pub(crate) struct IndexingTerm(Vec<u8>);

/// The number of bytes used as for the field id by `Term`.
const FIELD_ID_LENGTH: usize = 4;

impl IndexingTerm {
    /// Create a new IndexingTerm.
    pub fn new() -> IndexingTerm {
        let mut data = Vec::with_capacity(FIELD_ID_LENGTH + 32);
        data.resize(FIELD_ID_LENGTH, 0u8);
        IndexingTerm(data)
    }

    /// Is empty if there are no value bytes.
    pub fn is_empty(&self) -> bool {
        self.0.len() == FIELD_ID_LENGTH
    }

    /// Removes the value_bytes and set the field
    pub(crate) fn clear_with_field(&mut self, field: Field) {
        self.truncate_value_bytes(0);
        self.0[0..4].clone_from_slice(field.field_id().to_be_bytes().as_ref());
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

    /// Sets a `DateTime` value in the term.
    pub fn set_date(&mut self, val: DateTime) {
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
        self.truncate_value_bytes(0);
        self.append_fast_value(val);
    }

    /// Sets a `Ipv6Addr` value in the term.
    pub fn set_ip_addr(&mut self, val: Ipv6Addr) {
        self.set_value_bytes(val.to_u128().to_be_bytes().as_ref());
    }

    /// Sets the value bytes of the term.
    pub fn set_value_bytes(&mut self, bytes: &[u8]) {
        self.truncate_value_bytes(0);
        self.0.extend(bytes);
    }

    /// Append a type marker + fast value to a term.
    /// This is used in JSON type to append a fast value after the path.
    ///
    /// It will not clear existing bytes.
    pub(crate) fn append_type_and_fast_value<T: FastValue>(&mut self, val: T) {
        self.0.push(T::to_type().to_code());
        self.append_fast_value(val)
    }

    /// Append a fast value to a term.
    ///
    /// It will not clear existing bytes.
    pub fn append_fast_value<T: FastValue>(&mut self, val: T) {
        let value = if T::to_type() == Type::Date {
            DateTime::from_u64(val.to_u64())
                .truncate(DATE_TIME_PRECISION_INDEXED)
                .to_u64()
        } else {
            val.to_u64()
        };
        self.0.extend(value.to_be_bytes().as_ref());
    }

    /// Truncates the value bytes of the term. Value and field type stays the same.
    pub fn truncate_value_bytes(&mut self, len: usize) {
        self.0.truncate(len + FIELD_ID_LENGTH);
    }

    /// The length of the bytes.
    pub fn len_bytes(&self) -> usize {
        self.0.len() - FIELD_ID_LENGTH
    }

    /// Appends bytes to the Term.
    ///
    /// This function returns the segment that has just been added.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    /// Returns the serialized representation of Term.
    /// This includes field_id, value bytes
    #[inline]
    pub fn serialized_for_hashmap(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub fn get_field_from_indexing_term(bytes: &[u8]) -> Field {
    let field_id_bytes: [u8; 4] = bytes[..4].try_into().unwrap();
    Field::from_field_id(u32::from_be_bytes(field_id_bytes))
}
