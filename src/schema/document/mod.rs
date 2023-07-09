//! Document definition for Tantivy to index and store.
//!
//! A document and its values are defined by a couple core traits:
//! - [DocumentAccess] which describes your top-level document and it's fields.
//! - [DocValue] which provides tantivy with a way to access the document's values in a common way
//!   without performing any additional allocations.
//! - [DocumentDeserialize] which implements the necessary code to deserialize the document from the
//!   doc store.
//!
//! Tantivy provides a few out-of-box implementations of these core traits to provide
//! some simple usage if you don't want to implement these traits on a custom type yourself.
//!
//! # Out-of-box document implementations
//! - [Document] the old document type used by Tantivy before the trait based approach was
//!   implemented. This type is still valid and provides all of the original behaviour you might
//!   expect.
//! - `BTreeMap<Field, Value>` a mapping of field_ids to their relevant schema value using a
//!   BTreeMap.
//! - `HashMap<Field, Value>` a mapping of field_ids to their relevant schema value using a HashMap.
//!
//! # Implementing your custom documents
//! Often in larger projects or higher performance applications you want to avoid the extra overhead
//! of converting your own types to the Tantivy [Document] type, this can often save you a
//! significant amount of time when indexing by avoiding the additional allocations.
//!
//! ### Important Note
//! The implementor of the `DocumentAccess` trait must be `'static` and safe to send across
//! thread boundaries.
//!
//! ## Reusing existing types
//! The API design of the document traits allow you to reuse as much of as little of the
//! existing trait implementations as you like, this can save quite a bit of boilerplate
//! as shown by the following example.
//!
//! ## A basic custom document
//! ```
//! use std::collections::{btree_map, BTreeMap};
//! use tantivy::schema::{document, DocumentAccess, Field};
//! use tantivy::schema::document::{DeserializeError, DocumentDeserialize, DocumentDeserializer};
//!
//! /// Our custom document to let us use a map of `serde_json::Values`.
//! pub struct MyCustomDocument {
//!     // Tantivy provides trait implementations for common `serde_json` types.
//!     fields: BTreeMap<Field, serde_json::Value>
//! }
//!
//! impl DocumentAccess for MyCustomDocument {
//!     // The value type produced by the `iter_fields_and_values` iterator.
//!     type Value<'a> = &'a serde_json::Value;
//!     // The iterator which is produced by `iter_fields_and_values`.
//!     // Often this is a simple new-type wrapper unless you like super long generics.
//!     type FieldsValuesIter<'a> = MyCustomIter<'a>;
//!
//!     /// The length of the document.
//!     fn len(&self) -> usize {
//!         self.fields.len()
//!     }
//!
//!     /// Produces an iterator over the document fields and values.
//!     /// This method will be called multiple times, it's important
//!     /// to not do anything too heavy in this step, any heavy operations
//!     /// should be done before and effectively cached.
//!     fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
//!         MyCustomIter(self.fields.iter())
//!     }
//! }
//!
//! // Our document must also provide a way to get the original doc
//! // back when it's deserialized from the doc store.
//! // The API for this is very similar to serde but a little bit
//! // more specialised, giving you access to types like IP addresses, datetime, etc...
//! impl DocumentDeserialize for MyCustomDocument {
//!     fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
//!     where D: DocumentDeserializer<'de>
//!     {
//!         // We're not going to implement the necessary logic for this example
//!         // see the `Deserialization` section of implementing a custom document
//!         // for more information on how this works.
//!         unimplemented!()
//!     }
//! }
//!
//! /// Our custom iterator just helps us to avoid some messy generics.
//! pub struct MyCustomIter<'a>(btree_map::Iter<'a, Field, serde_json::Value>);//!
//! impl<'a> Iterator for MyCustomIter<'a> {
//!     // Here we can see our field-value pairs being produced by the iterator.
//!     // The value returned alongside the field is the same type as `DocumentAccess::Value<'_>`.
//!     type Item = (Field, &'a serde_json::Value);
//!
//!     fn next(&mut self) -> Option<Self::Item> {
//!         let (field, value) = self.0.next()?;
//!         Some((*field, value))
//!     }
//! }
//! ```
//!
//! You may have noticed in this example that we haven't needed to implement any custom value types,
//! instead we've just used a [serde_json::Value] type which tantivy provides an existing
//! implementation for.
//!
//! ## Implementing custom values
//! Internally, Tantivy only works with `ReferenceValue` which is an enum that tries to borrow
//! as much data as it can, in order to allow documents to return custom types, they must implement
//! the `DocValue` trait which provides a way for Tantivy to get a `ReferenceValue` that it can then
//! index and store.
//!
//! Values can just as easily be customised as documents by implementing the `DocValue` trait.
//!
//! The implementor of this type should not own the data it's returning, instead it should just
//! hold references of the data held by the parent [DocumentAccess] which can then be passed
//! on to the [ReferenceValue].
//!
//! This is why `DocValue` is implemented for `&'a serde_json::Value` and `&'a
//! tantivy::schema::Value` but not for their owned counterparts, as we cannot satisfy the lifetime
//! bounds necessary when indexing the documents.
//!
//! ### A note about returning values
//! The custom value type does not have to be the type stored by the document, instead the
//! implementor of a `DocValue` can just be used as a way to convert between the owned type
//! kept in the parent document, and the value passed into Tantivy.
//!
//! ```
//! use tantivy::schema::document::ReferenceValue;
//! use tantivy::schema::{DocValue, Field};
//!
//! #[derive(Debug)]
//! /// Our custom value type which has 3 types, a string, float and bool.
//! pub enum MyCustomValue<'a> {
//!     // Our string data is owned by the parent document, instead we just
//!     // hold onto a reference of this data.
//!     String(&'a str),
//!     Float(f64),
//!     Bool(bool),
//! }
//!
//! impl<'a> DocValue<'a> for MyCustomValue<'a> {
//!     // We don't need to worry about these types here as we're not
//!     // working with nested types, but if we wanted to we would
//!     // define our two iterator types, a sequence of ReferenceValues
//!     // for the array iterator and a sequence of key-value pairs for objects.
//!     type ArrayIter = std::iter::Empty<ReferenceValue<'a, Self>>;
//!     type ObjectIter = std::iter::Empty<(&'a str, ReferenceValue<'a, Self>)>;
//!
//!     // The ReferenceValue which Tantivy can use.
//!     fn as_value(&self) -> ReferenceValue<'a, Self> {
//!         // We can support any type that Tantivy itself supports.
//!         match self {
//!             MyCustomValue::String(val) => ReferenceValue::Str(val),
//!             MyCustomValue::Float(val) => ReferenceValue::F64(*val),
//!             MyCustomValue::Bool(val) => ReferenceValue::Bool(*val),
//!         }
//!     }
//!
//! }
//! ```
//!
//! TODO: Complete this section...

mod core;
mod de;
mod helpers;
mod se;

use std::fmt::Debug;
use std::mem;
use std::net::Ipv6Addr;

pub use self::core::{DocParsingError, Document};
pub(crate) use self::de::GenericDocumentDeserializer;
pub use self::de::{
    ArrayAccess, DeserializeError, DocumentDeserialize, DocumentDeserializer, ObjectAccess,
    ValueDeserialize, ValueDeserializer, ValueType, ValueVisitor,
};
pub(crate) use self::se::DocumentSerializer;
use super::*;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// The core trait representing a document within the index.
pub trait DocumentAccess: DocumentDeserialize + Send + Sync + 'static {
    /// The value of the field.
    type Value<'a>: DocValue<'a> + Clone
    where Self: 'a;

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
    // Replaced by the `OBJECT_CODE`.
    // -- pub const JSON_OBJ_CODE: u8 = 8;
    pub const BOOL_CODE: u8 = 9;
    pub const IP_CODE: u8 = 10;
    pub const NULL_CODE: u8 = 11;
    pub const ARRAY_CODE: u8 = 12;
    pub const OBJECT_CODE: u8 = 13;

    // Extended type codes
    pub const TOK_STR_EXT_CODE: u8 = 0;
}
