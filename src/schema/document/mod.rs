mod core;
mod de;
mod se;
mod serde_compat;

use std::fmt::Debug;
use std::io::{Read, Write};
use std::mem;
use std::net::Ipv6Addr;

use super::*;
use crate::schema::document::de::ValueType;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// The core trait representing a document within the index.
pub trait DocumentAccess: Send + Sync + 'static {
    /// The value of the field.
    type Value<'a>: DocValue<'a> + Clone
    where Self: 'a;

    /// The owned version of a value type.
    ///
    /// It's possible that this is the same type as the borrowed
    /// variant simply by using things like a `Cow`, but it may
    /// be beneficial to implement them as separate types for
    /// some use cases.
    type OwnedValue: ValueDeserialize + Debug;
    /// The iterator over all of the fields and values within the doc.
    type FieldsValuesIter<'a>: Iterator<Item = (Field, Self::Value<'a>)>
    where Self: 'a;

    /// Returns the number of fields within the document.
    fn len(&self) -> usize;

    /// Returns true if the document contains no fields.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get an iterator iterating over all fields and values in a document.
    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_>;

    /// Create a new document from a given stream of fields.
    fn from_fields(fields: Vec<(Field, Self::OwnedValue)>) -> Self;

    /// Sort and groups the field_values by field.
    ///
    /// The result of this method is not cached and is
    /// computed on the fly when this method is called.
    fn get_sorted_field_values(&self) -> Vec<(Field, Vec<Self::Value<'_>>)> {
        let mut field_values: Vec<(Field, Self::Value<'_>)> =
            self.iter_fields_and_values().collect();
        field_values.sort_by_key(|(field, _)| *field);

        let mut field_values_it = field_values.into_iter();

        let first_field_value = if let Some(first_field_value) = field_values_it.next() {
            first_field_value
        } else {
            return Vec::new();
        };

        let mut grouped_field_values = vec![];
        let mut current_field = first_field_value.0;
        let mut current_group = vec![first_field_value.1];

        for (field, value) in field_values_it {
            if field == current_field {
                current_group.push(value);
            } else {
                grouped_field_values
                    .push((current_field, mem::replace(&mut current_group, vec![value])));
                current_field = field;
            }
        }

        grouped_field_values.push((current_field, current_group));
        grouped_field_values
    }
}

/// A single field value.
pub trait DocValue<'a>: Send + Sync + Debug {
    /// The iterator for walking through the element within the array.
    type ArrayIter: Iterator<Item = ReferenceValue<'a, Self>>;
    /// The visitor for walking through the key-value pairs within
    /// the object.
    type ObjectIter: Iterator<Item = (&'a str, ReferenceValue<'a, Self>)>;

    /// Returns the field value represented by a enum which borrows it's data.
    fn as_value(&self) -> ReferenceValue<'a, Self>;

    #[inline]
    /// Returns if the value is `null` or not.
    fn is_null(&self) -> bool {
        matches!(self.as_value(), ReferenceValue::Null)
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    fn as_str(&self) -> Option<&'a str> {
        if let ReferenceValue::Str(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    fn as_u64(&self) -> Option<u64> {
        if let ReferenceValue::U64(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    fn as_i64(&self) -> Option<i64> {
        if let ReferenceValue::I64(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    fn as_f64(&self) -> Option<f64> {
        if let ReferenceValue::F64(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    fn as_datetime(&self) -> Option<DateTime> {
        if let ReferenceValue::Date(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        if let ReferenceValue::IpAddr(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    fn as_bool(&self) -> Option<bool> {
        if let ReferenceValue::Bool(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a pre-tokenized string, returns the associated string. Returns None
    /// otherwise.
    fn as_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
        if let ReferenceValue::PreTokStr(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    fn as_bytes(&self) -> Option<&'a [u8]> {
        if let ReferenceValue::Bytes(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    fn as_facet(&self) -> Option<&'a Facet> {
        if let ReferenceValue::Facet(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// Returns true if the Value is an array.
    fn is_array(&self) -> bool {
        matches!(self.as_value(), ReferenceValue::Object(_))
    }

    #[inline]
    /// Returns true if the Value is an object.
    fn is_object(&self) -> bool {
        matches!(self.as_value(), ReferenceValue::Object(_))
    }
}

/// A enum representing a value for tantivy to index.
pub enum ReferenceValue<'a, V>
where V: DocValue<'a> + ?Sized
{
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(&'a str),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Facet
    Facet(&'a Facet),
    /// Arbitrarily sized byte array
    Bytes(&'a [u8]),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// Bool value
    Bool(bool),
    /// Pre-tokenized str type,
    PreTokStr(&'a PreTokenizedString),
    /// A an array containing multiple values.
    Array(V::ArrayIter),
    /// A nested / dynamic object.
    Object(V::ObjectIter),
}

impl<'a, V> ReferenceValue<'a, V>
where V: DocValue<'a> + ?Sized
{
    #[inline]
    /// Returns if the value is `null` or not.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    pub fn as_str(&self) -> Option<&'a str> {
        if let Self::Str(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    pub fn as_u64(&self) -> Option<u64> {
        if let Self::U64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        if let Self::I64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    pub fn as_f64(&self) -> Option<f64> {
        if let Self::F64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    pub fn as_datetime(&self) -> Option<DateTime> {
        if let Self::Date(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    pub fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        if let Self::IpAddr(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a pre-tokenized string, returns the associated string. Returns None
    /// otherwise.
    pub fn as_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
        if let Self::PreTokStr(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    pub fn as_facet(&self) -> Option<&'a Facet> {
        if let Self::Facet(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// Returns true if the Value is an array.
    pub fn is_array(&self) -> bool {
        matches!(self, Self::Object(_))
    }

    #[inline]
    /// Returns true if the Value is an object.
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }
}

pub(crate) mod type_codes {
    pub const TEXT_CODE: u8 = 0;
    pub const U64_CODE: u8 = 1;
    pub const I64_CODE: u8 = 2;
    pub const HIERARCHICAL_FACET_CODE: u8 = 3;
    pub const BYTES_CODE: u8 = 4;
    pub const DATE_CODE: u8 = 5;
    pub const F64_CODE: u8 = 6;
    pub const EXT_CODE: u8 = 7;
    // Depreciated: Replaced by the `OBJECT_CODE`.
    // pub const JSON_OBJ_CODE: u8 = 8;
    pub const BOOL_CODE: u8 = 9;
    pub const IP_CODE: u8 = 10;
    pub const NULL_CODE: u8 = 11;
    pub const ARRAY_CODE: u8 = 12;
    pub const OBJECT_CODE: u8 = 13;

    // Extended type codes
    pub const TOK_STR_EXT_CODE: u8 = 0;
}
