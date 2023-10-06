use std::fmt::Debug;
use std::net::Ipv6Addr;

use common::DateTime;

use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;

/// A single field value.
pub trait Value<'a>: Send + Sync + Debug {
    /// The child value type returned by this doc value.
    /// The iterator for walking through the elements within the array.
    type ArrayIter: Iterator<Item = Self>;
    /// The visitor walking through the key-value pairs within
    /// the object.
    type ObjectIter: Iterator<Item = (&'a str, Self)>;

    /// Returns the field value represented by an enum which borrows it's data.
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
    fn as_pre_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
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
    /// Returns the iterator over the array if the Value is an array.
    fn as_array(&self) -> Option<Self::ArrayIter> {
        if let ReferenceValue::Array(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// Returns the iterator over the object if the Value is an object.
    fn as_object(&self) -> Option<Self::ObjectIter> {
        if let ReferenceValue::Object(val) = self.as_value() {
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
#[derive(Clone, Debug, PartialEq)]
pub enum ReferenceValue<'a, V>
where V: Value<'a> + ?Sized
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
where V: Value<'a>
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
    pub fn as_pre_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
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
