use std::net::{IpAddr, Ipv6Addr};

use crate::schema::{Cardinality, FieldType, Type};
use crate::DateTime;

pub fn ip_to_u128(ip_addr: IpAddr) -> u128 {
    let ip_addr_v6: Ipv6Addr = match ip_addr {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    u128::from_be_bytes(ip_addr_v6.octets())
}

/// Trait for large types that are allowed for fast fields: u128, IpAddr
pub trait FastValueU128: Clone + Copy + Send + Sync + PartialOrd + 'static {
    /// Converts a value from u128
    ///
    /// Internally all fast field values are encoded as u128.
    /// **Note: To be used for converting encoded Term, Posting values.**
    fn from_u128(val: u128) -> Self;

    /// Converts a value to u128.
    ///
    /// Internally all fast field values are encoded as u128.
    fn to_u128(&self) -> u128;

    /// Cast value to `u128`.
    /// The value is just reinterpreted in memory.
    fn as_u128(&self) -> u128;

    /// Returns the `schema::Type` for this FastValue.
    fn to_type() -> Type;

    /// Build a default value. This default value is never used, so the value does not
    /// really matter.
    fn make_zero() -> Self {
        Self::from_u128(0u128)
    }
}

impl FastValueU128 for u128 {
    fn from_u128(val: u128) -> Self {
        val
    }

    fn to_u128(&self) -> u128 {
        *self
    }

    fn as_u128(&self) -> u128 {
        *self
    }

    fn to_type() -> Type {
        Type::U128
    }
}

impl FastValueU128 for IpAddr {
    fn from_u128(val: u128) -> Self {
        IpAddr::from(val.to_be_bytes())
    }

    fn to_u128(&self) -> u128 {
        ip_to_u128(*self)
    }

    fn as_u128(&self) -> u128 {
        ip_to_u128(*self)
    }

    fn to_type() -> Type {
        Type::Ip
    }
}

/// Trait for types that are allowed for fast fields:
/// (u64, i64 and f64, bool, DateTime).
pub trait FastValue: Clone + Copy + Send + Sync + PartialOrd + 'static {
    /// Converts a value from u64
    ///
    /// Internally all fast field values are encoded as u64.
    /// **Note: To be used for converting encoded Term, Posting values.**
    fn from_u64(val: u64) -> Self;

    /// Converts a value to u64.
    ///
    /// Internally all fast field values are encoded as u64.
    fn to_u64(&self) -> u64;

    /// Returns the fast field cardinality that can be extracted from the given
    /// `FieldType`.
    ///
    /// If the type is not a fast field, `None` is returned.
    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality>;

    /// Cast value to `u64`.
    /// The value is just reinterpreted in memory.
    fn as_u64(&self) -> u64;

    /// Build a default value. This default value is never used, so the value does not
    /// really matter.
    fn make_zero() -> Self {
        Self::from_u64(0i64.to_u64())
    }

    /// Returns the `schema::Type` for this FastValue.
    fn to_type() -> Type;
}

impl FastValue for u64 {
    fn from_u64(val: u64) -> Self {
        val
    }

    fn to_u64(&self) -> u64 {
        *self
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::U64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            FieldType::Facet(_) => Some(Cardinality::MultiValues),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        *self
    }

    fn to_type() -> Type {
        Type::U64
    }
}

impl FastValue for i64 {
    fn from_u64(val: u64) -> Self {
        common::u64_to_i64(val)
    }

    fn to_u64(&self) -> u64 {
        common::i64_to_u64(*self)
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::I64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        *self as u64
    }

    fn to_type() -> Type {
        Type::I64
    }
}

impl FastValue for f64 {
    fn from_u64(val: u64) -> Self {
        common::u64_to_f64(val)
    }

    fn to_u64(&self) -> u64 {
        common::f64_to_u64(*self)
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::F64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        self.to_bits()
    }

    fn to_type() -> Type {
        Type::F64
    }
}

impl FastValue for bool {
    fn from_u64(val: u64) -> Self {
        val != 0u64
    }

    fn to_u64(&self) -> u64 {
        match self {
            false => 0,
            true => 1,
        }
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::Bool(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        *self as u64
    }

    fn to_type() -> Type {
        Type::Bool
    }
}

impl FastValue for DateTime {
    /// Converts a timestamp microseconds into DateTime.
    ///
    /// **Note the timestamps is expected to be in microseconds.**
    fn from_u64(timestamp_micros_u64: u64) -> Self {
        let timestamp_micros = i64::from_u64(timestamp_micros_u64);
        Self::from_timestamp_micros(timestamp_micros)
    }

    fn to_u64(&self) -> u64 {
        common::i64_to_u64(self.into_timestamp_micros())
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::Date(ref options) => options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        self.into_timestamp_micros().as_u64()
    }

    fn to_type() -> Type {
        Type::Date
    }
}
