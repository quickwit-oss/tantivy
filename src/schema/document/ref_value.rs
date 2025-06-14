use std::net::Ipv6Addr;

use common::DateTime;

use super::de::{BinaryArrayDeserializer, BinaryObjectDeserializer};
use crate::tokenizer::PreTokenizedString;

pub enum RefValue<'a> {
    /// A null value.
    Null,
    /// Bool value
    Bool(bool),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// Arbitrarily sized byte array
    Bytes(&'a [u8]),
    /// The str type is used for any text information.
    Str(&'a str),
    /// Facet string needs to match the format of
    /// [Facet::encoded_str](crate::schema::Facet::encoded_str).
    Facet(&'a str),
    /// Pre-tokenized str type,
    PreTokStr(PreTokenizedString),
    /// An iterator over a list of values.
    Array(BinaryArrayDeserializer<'a>),
    /// An iterator over a list of key-value pairs.
    Object(BinaryObjectDeserializer<'a>),
}
