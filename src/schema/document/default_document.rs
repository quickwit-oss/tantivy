use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::Ipv6Addr;

use columnar::MonotonicallyMappableToU128;
use common::{serialize_vint_u32, BinarySerializable, DateTime, VInt};
use serde_json::Map;
pub use CompactDoc as TantivyDocument;

use super::{ReferenceValue, ReferenceValueLeaf, Value};
use crate::schema::document::{
    DeserializeError, Document, DocumentDeserialize, DocumentDeserializer,
};
use crate::schema::field_type::ValueParsingError;
use crate::schema::{Facet, Field, NamedFieldDocument, OwnedValue, Schema};
use crate::tokenizer::PreTokenizedString;

#[repr(C, packed)]
#[derive(Debug, Clone)]
/// A field value pair in the compact tantivy document
struct FieldValueAddr {
    pub field: u16,
    pub value_addr: ValueAddr,
}

#[derive(Debug, Clone)]
/// The default document in tantivy. It encodes data in a compact form.
pub struct CompactDoc {
    /// `node_data` is a vec of bytes, where each value is serialized into bytes and stored. It
    /// includes all the data of the document and also metadata like where the nodes are located
    /// in an object or array.
    pub node_data: Vec<u8>,
    /// The root (Field, Value) pairs
    field_values: Vec<FieldValueAddr>,
}

impl Default for CompactDoc {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactDoc {
    /// Creates a new, empty document object
    /// The reserved capacity is for the total serialized data
    pub fn with_capacity(bytes: usize) -> CompactDoc {
        CompactDoc {
            node_data: Vec::with_capacity(bytes),
            field_values: Vec::with_capacity(4),
        }
    }

    /// Creates a new, empty document object
    pub fn new() -> CompactDoc {
        CompactDoc::with_capacity(1024)
    }

    /// Skrinks the capacity of the document to fit the data
    pub fn shrink_to_fit(&mut self) {
        self.node_data.shrink_to_fit();
        self.field_values.shrink_to_fit();
    }

    /// Returns the length of the document.
    pub fn len(&self) -> usize {
        self.field_values.len()
    }

    /// Adding a facet to the document.
    pub fn add_facet<F>(&mut self, field: Field, path: F)
    where Facet: From<F> {
        let facet = Facet::from(path);
        self.add_leaf_field_value(field, ReferenceValueLeaf::Facet(facet.encoded_str()));
    }

    /// Add a text field.
    pub fn add_text<S: AsRef<str>>(&mut self, field: Field, text: S) {
        self.add_leaf_field_value(field, ReferenceValueLeaf::Str(text.as_ref()));
    }

    /// Add a pre-tokenized text field.
    pub fn add_pre_tokenized_text(&mut self, field: Field, pre_tokenized_text: PreTokenizedString) {
        self.add_leaf_field_value(field, pre_tokenized_text);
    }

    /// Add a u64 field
    pub fn add_u64(&mut self, field: Field, value: u64) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a IP address field. Internally only Ipv6Addr is used.
    pub fn add_ip_addr(&mut self, field: Field, value: Ipv6Addr) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a i64 field
    pub fn add_i64(&mut self, field: Field, value: i64) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a f64 field
    pub fn add_f64(&mut self, field: Field, value: f64) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a bool field
    pub fn add_bool(&mut self, field: Field, value: bool) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a date field with unspecified time zone offset
    pub fn add_date(&mut self, field: Field, value: DateTime) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a bytes field
    pub fn add_bytes(&mut self, field: Field, value: &[u8]) {
        self.add_leaf_field_value(field, value);
    }

    /// Add a plugin-defined custom field value.
    ///
    /// The bytes are an opaque payload owned by the plugin that consumes this field's custom
    /// type; tantivy never inspects, indexes, or stores them.
    pub fn add_custom(&mut self, field: Field, value: &[u8]) {
        self.add_leaf_field_value(field, ReferenceValueLeaf::Custom(value));
    }

    /// Add a dynamic object field
    pub fn add_object(&mut self, field: Field, object: BTreeMap<String, OwnedValue>) {
        self.add_field_value(field, &OwnedValue::from(object));
    }

    /// Add a (field, value) to the document.
    ///
    /// `OwnedValue` implements Value, which should be easiest to use, but is not the most
    /// performant.
    pub fn add_field_value<'a, V: Value<'a>>(&mut self, field: Field, value: V) {
        let field_value = FieldValueAddr {
            field: field
                .field_id()
                .try_into()
                .expect("support only up to u16::MAX field ids"),
            value_addr: self.add_value(value),
        };
        self.field_values.push(field_value);
    }

    /// Add a (field, leaf value) to the document.
    /// Leaf values don't have nested values.
    pub fn add_leaf_field_value<'a, T: Into<ReferenceValueLeaf<'a>>>(
        &mut self,
        field: Field,
        typed_val: T,
    ) {
        let value = typed_val.into();
        let field_value = FieldValueAddr {
            field: field
                .field_id()
                .try_into()
                .expect("support only up to u16::MAX field ids"),
            value_addr: self.add_value_leaf(value),
        };
        self.field_values.push(field_value);
    }

    /// field_values accessor
    pub fn field_values(&self) -> impl Iterator<Item = (Field, CompactDocValue<'_>)> {
        self.field_values.iter().map(|field_val| {
            let field = Field::from_field_id(field_val.field as u32);
            let val = self.get_compact_doc_value(field_val.value_addr);
            (field, val)
        })
    }

    /// Returns all of the `ReferenceValue`s associated the given field
    pub fn get_all(&self, field: Field) -> impl Iterator<Item = CompactDocValue<'_>> + '_ {
        self.field_values
            .iter()
            .filter(move |field_value| Field::from_field_id(field_value.field as u32) == field)
            .map(|val| self.get_compact_doc_value(val.value_addr))
    }

    /// Returns the first `ReferenceValue` associated the given field
    pub fn get_first(&self, field: Field) -> Option<CompactDocValue<'_>> {
        self.get_all(field).next()
    }

    /// Create document from a named doc.
    pub fn convert_named_doc(
        schema: &Schema,
        named_doc: NamedFieldDocument,
    ) -> Result<Self, DocParsingError> {
        let mut document = Self::new();
        for (field_name, values) in named_doc.0 {
            if let Ok(field) = schema.get_field(&field_name) {
                for value in values {
                    document.add_field_value(field, &value);
                }
            }
        }
        Ok(document)
    }

    /// Build a document object from a json-object.
    pub fn parse_json(schema: &Schema, doc_json: &str) -> Result<Self, DocParsingError> {
        let json_obj: Map<String, serde_json::Value> =
            serde_json::from_str(doc_json).map_err(|_| DocParsingError::invalid_json(doc_json))?;
        Self::from_json_object(schema, json_obj)
    }

    /// Build a document object from a json-object.
    pub fn from_json_object(
        schema: &Schema,
        json_obj: Map<String, serde_json::Value>,
    ) -> Result<Self, DocParsingError> {
        let mut doc = Self::default();
        for (field_name, json_value) in json_obj {
            if let Ok(field) = schema.get_field(&field_name) {
                let field_entry = schema.get_field_entry(field);
                let field_type = field_entry.field_type();
                match json_value {
                    serde_json::Value::Array(json_items) => {
                        for json_item in json_items {
                            let value = field_type
                                .value_from_json(json_item)
                                .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                            doc.add_field_value(field, &value);
                        }
                    }
                    _ => {
                        let value = field_type
                            .value_from_json(json_value)
                            .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                        doc.add_field_value(field, &value);
                    }
                }
            }
        }
        Ok(doc)
    }

    fn add_value_leaf(&mut self, leaf: ReferenceValueLeaf) -> ValueAddr {
        let type_id = ValueType::from(&leaf);
        // Write into `node_data` and return u32 position as its address
        // Null and bool are inlined into the address
        let val_addr = match leaf {
            ReferenceValueLeaf::Null => 0,
            ReferenceValueLeaf::Str(bytes) => {
                write_bytes_into(&mut self.node_data, bytes.as_bytes())
            }
            ReferenceValueLeaf::Facet(bytes) => {
                write_bytes_into(&mut self.node_data, bytes.as_bytes())
            }
            ReferenceValueLeaf::Bytes(bytes) => write_bytes_into(&mut self.node_data, bytes),
            ReferenceValueLeaf::Custom(bytes) => write_bytes_into(&mut self.node_data, bytes),
            ReferenceValueLeaf::U64(num) => write_into(&mut self.node_data, num),
            ReferenceValueLeaf::I64(num) => write_into(&mut self.node_data, num),
            ReferenceValueLeaf::F64(num) => write_into(&mut self.node_data, num),
            ReferenceValueLeaf::Bool(b) => b as u32,
            ReferenceValueLeaf::Date(date) => {
                write_into(&mut self.node_data, date.into_timestamp_nanos())
            }
            ReferenceValueLeaf::IpAddr(num) => write_into(&mut self.node_data, num.to_u128()),
            ReferenceValueLeaf::PreTokStr(pre_tok) => write_into(&mut self.node_data, *pre_tok),
        };
        ValueAddr { type_id, val_addr }
    }
    /// Adds a value and returns in address into the
    fn add_value<'a, V: Value<'a>>(&mut self, value: V) -> ValueAddr {
        // Leaves (most values) need no scratch space.
        if let ReferenceValue::Leaf(leaf) = value.as_value() {
            return self.add_value_leaf(leaf);
        }
        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        SCRATCH.with(|scratch| match scratch.try_borrow_mut() {
            Ok(mut scratch) => {
                scratch.clear();
                self.add_value_with_scratch(value, &mut scratch)
            }
            // Re-entrant call (a `Value` impl adding to a document while being added).
            Err(_) => self.add_value_with_scratch(value, &mut Vec::new()),
        })
    }

    /// `scratch` is a stack of child addresses shared by all nesting levels: each array or
    /// object appends its children's addresses after the current end, copies them into
    /// `node_data`, then truncates back. One allocation per document instead of one per object.
    fn add_value_with_scratch<'a, V: Value<'a>>(
        &mut self,
        value: V,
        scratch: &mut Vec<u8>,
    ) -> ValueAddr {
        let value = value.as_value();
        let type_id = ValueType::from(&value);
        match value {
            ReferenceValue::Leaf(leaf) => self.add_value_leaf(leaf),
            ReferenceValue::Array(elements) => {
                let start = scratch.len();
                for elem in elements {
                    let value_addr = self.add_value_with_scratch(elem, scratch);
                    value_addr.write_to_vec(scratch);
                }
                let val_addr = write_bytes_into(&mut self.node_data, &scratch[start..]);
                scratch.truncate(start);
                ValueAddr { type_id, val_addr }
            }
            ReferenceValue::Object(entries) => {
                let start = scratch.len();
                for (key, value) in entries {
                    let key_addr = self.add_value_leaf(ReferenceValueLeaf::Str(key));
                    let value_addr = self.add_value_with_scratch(value, scratch);
                    key_addr.write_to_vec(scratch);
                    value_addr.write_to_vec(scratch);
                }
                let val_addr = write_bytes_into(&mut self.node_data, &scratch[start..]);
                scratch.truncate(start);
                ValueAddr { type_id, val_addr }
            }
        }
    }

    /// Get CompactDocValue for address
    fn get_compact_doc_value(&self, value_addr: ValueAddr) -> CompactDocValue<'_> {
        CompactDocValue {
            container: self,
            value_addr,
        }
    }

    /// get &[u8] reference from node_data
    fn extract_bytes(&self, addr: Addr) -> &[u8] {
        binary_deserialize_bytes(self.get_slice(addr))
    }

    /// get &str reference from node_data
    fn extract_str(&self, addr: Addr) -> &str {
        let data = self.extract_bytes(addr);
        // Utf-8 checks would have a noticeable performance overhead here
        unsafe { std::str::from_utf8_unchecked(data) }
    }

    /// deserialized owned value from node_data
    fn read_from<T: BinarySerializable>(&self, addr: Addr) -> io::Result<T> {
        let data_slice = &self.node_data[addr as usize..];
        let mut cursor = std::io::Cursor::new(data_slice);
        T::deserialize(&mut cursor)
    }

    /// get slice from address. The returned slice is open ended
    fn get_slice(&self, addr: Addr) -> &[u8] {
        &self.node_data[addr as usize..]
    }
}

/// BinarySerializable alternative to read references
fn binary_deserialize_bytes(data: &[u8]) -> &[u8] {
    let (len, rest) = read_vint_u32(data).expect("corrupted compact doc");
    &rest[..len as usize]
}

/// Write bytes and return the position of the written data.
///
/// BinarySerializable alternative to write references
fn write_bytes_into(vec: &mut Vec<u8>, data: &[u8]) -> u32 {
    let pos = vec.len() as u32;
    let mut buf = [0u8; 8];
    let len_vint_bytes = serialize_vint_u32(data.len() as u32, &mut buf);
    vec.extend_from_slice(len_vint_bytes);
    vec.extend_from_slice(data);
    pos
}

/// Serialize and return the position
fn write_into<T: BinarySerializable>(vec: &mut Vec<u8>, value: T) -> u32 {
    let pos = vec.len() as u32;
    value.serialize(vec).unwrap();
    pos
}

impl PartialEq for CompactDoc {
    fn eq(&self, other: &Self) -> bool {
        // super slow, but only here for tests
        let convert_to_comparable_map = |doc: &CompactDoc| {
            let mut field_value_set: HashMap<Field, HashSet<String>> = Default::default();
            for field_value in doc.field_values.iter() {
                let value: OwnedValue = doc.get_compact_doc_value(field_value.value_addr).into();
                let value = serde_json::to_string(&value).unwrap();
                field_value_set
                    .entry(Field::from_field_id(field_value.field as u32))
                    .or_default()
                    .insert(value);
            }
            field_value_set
        };
        let self_field_values: HashMap<Field, HashSet<String>> = convert_to_comparable_map(self);
        let other_field_values: HashMap<Field, HashSet<String>> = convert_to_comparable_map(other);
        self_field_values.eq(&other_field_values)
    }
}

impl Eq for CompactDoc {}

impl DocumentDeserialize for CompactDoc {
    fn deserialize<'de, D>(mut deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de> {
        let mut doc = CompactDoc::default();
        // TODO: Deserializing into OwnedValue is wasteful. The deserializer should be able to work
        // on slices and referenced data.
        while let Some((field, value)) = deserializer.next_field::<OwnedValue>()? {
            doc.add_field_value(field, &value);
        }
        Ok(doc)
    }
}

/// A value of Compact Doc needs a reference to the container to extract its payload
#[derive(Debug, Clone, Copy)]
pub struct CompactDocValue<'a> {
    container: &'a CompactDoc,
    value_addr: ValueAddr,
}
impl PartialEq for CompactDocValue<'_> {
    fn eq(&self, other: &Self) -> bool {
        let value1: OwnedValue = (*self).into();
        let value2: OwnedValue = (*other).into();
        value1 == value2
    }
}
impl From<CompactDocValue<'_>> for OwnedValue {
    fn from(value: CompactDocValue) -> Self {
        value.as_value().into()
    }
}
impl<'a> Value<'a> for CompactDocValue<'a> {
    type ArrayIter = CompactDocArrayIter<'a>;

    type ObjectIter = CompactDocObjectIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        self.get_ref_value().unwrap()
    }
}
impl<'a> CompactDocValue<'a> {
    fn get_ref_value(&self) -> io::Result<ReferenceValue<'a, CompactDocValue<'a>>> {
        let addr = self.value_addr.val_addr;
        match self.value_addr.type_id {
            ValueType::Null => Ok(ReferenceValueLeaf::Null.into()),
            ValueType::Str => {
                let str_ref = self.container.extract_str(addr);
                Ok(ReferenceValueLeaf::Str(str_ref).into())
            }
            ValueType::Facet => {
                let str_ref = self.container.extract_str(addr);
                Ok(ReferenceValueLeaf::Facet(str_ref).into())
            }
            ValueType::Bytes => {
                let data = self.container.extract_bytes(addr);
                Ok(ReferenceValueLeaf::Bytes(data).into())
            }
            ValueType::Custom => {
                let data = self.container.extract_bytes(addr);
                Ok(ReferenceValueLeaf::Custom(data).into())
            }
            ValueType::U64 => self
                .container
                .read_from::<u64>(addr)
                .map(ReferenceValueLeaf::U64)
                .map(Into::into),
            ValueType::I64 => self
                .container
                .read_from::<i64>(addr)
                .map(ReferenceValueLeaf::I64)
                .map(Into::into),
            ValueType::F64 => self
                .container
                .read_from::<f64>(addr)
                .map(ReferenceValueLeaf::F64)
                .map(Into::into),
            ValueType::Bool => Ok(ReferenceValueLeaf::Bool(addr != 0).into()),
            ValueType::Date => self
                .container
                .read_from::<i64>(addr)
                .map(|ts| ReferenceValueLeaf::Date(DateTime::from_timestamp_nanos(ts)))
                .map(Into::into),
            ValueType::IpAddr => self
                .container
                .read_from::<u128>(addr)
                .map(|num| ReferenceValueLeaf::IpAddr(Ipv6Addr::from_u128(num)))
                .map(Into::into),
            ValueType::PreTokStr => self
                .container
                .read_from::<PreTokenizedString>(addr)
                .map(Into::into)
                .map(ReferenceValueLeaf::PreTokStr)
                .map(Into::into),
            ValueType::Object => Ok(ReferenceValue::Object(CompactDocObjectIter::new(
                self.container,
                addr,
            )?)),
            ValueType::Array => Ok(ReferenceValue::Array(CompactDocArrayIter::new(
                self.container,
                addr,
            )?)),
        }
    }
}

/// The address in the vec
type Addr = u32;

#[derive(Clone, Copy, Default)]
#[repr(C, packed)]
/// The value type and the address to its payload in the container.
struct ValueAddr {
    type_id: ValueType,
    /// This is the address to the value in the vec, except for bool and null, which are inlined
    val_addr: Addr,
}
impl ValueAddr {
    /// Appends the `BinarySerializable` encoding (type byte + VInt address).
    #[inline]
    fn write_to_vec(self, output: &mut Vec<u8>) {
        output.push(self.type_id as u8);
        let mut buf = [0u8; 8];
        output.extend_from_slice(serialize_vint_u32(self.val_addr, &mut buf));
    }

    /// Decodes a `ValueAddr` from the front of `data` and advances it (same encoding as
    /// `BinarySerializable`, without going through `io::Read`).
    #[inline]
    fn read_from_slice(data: &mut &[u8]) -> Option<Self> {
        let (&type_byte, rest) = data.split_first()?;
        if type_byte > 13 {
            return None;
        }
        // SAFETY: `ValueType` is `repr(u8)` with discriminants 0..=13.
        let type_id = unsafe { std::mem::transmute::<u8, ValueType>(type_byte) };
        let (val_addr, rest) = read_vint_u32(rest)?;
        *data = rest;
        Some(ValueAddr { type_id, val_addr })
    }
}

/// Reads a VInt `u32` (tantivy's VInt: 7 bits per byte, the last byte has its high bit set).
#[inline]
fn read_vint_u32(data: &[u8]) -> Option<(u32, &[u8])> {
    let first = *data.first()?;
    if first >= 128 {
        return Some(((first & 127) as u32, &data[1..]));
    }
    let mut result = first as u32;
    let mut shift = 7;
    for (idx, &byte) in data.iter().enumerate().skip(1).take(4) {
        result |= ((byte & 127) as u32) << shift;
        if byte >= 128 {
            return Some((result, &data[idx + 1..]));
        }
        shift += 7;
    }
    None
}

impl BinarySerializable for ValueAddr {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.type_id.serialize(writer)?;
        VInt(self.val_addr as u64).serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let type_id = ValueType::deserialize(reader)?;
        let val_addr = VInt::deserialize(reader)?.0 as u32;
        Ok(ValueAddr { type_id, val_addr })
    }
}
impl std::fmt::Debug for ValueAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val_addr = self.val_addr;
        f.write_fmt(format_args!("{:?} at {:?}", self.type_id, val_addr))
    }
}

/// A enum representing a value for tantivy to index.
///
/// ** Any changes need to be reflected in `BinarySerializable` for `ValueType` **
///
/// We can't use [schema::Type] or [columnar::ColumnType] here, because they are missing
/// some items like Array and PreTokStr.
#[derive(Default, Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum ValueType {
    /// A null value.
    #[default]
    Null = 0,
    /// The str type is used for any text information.
    Str = 1,
    /// Unsigned 64-bits Integer `u64`
    U64 = 2,
    /// Signed 64-bits Integer `i64`
    I64 = 3,
    /// 64-bits Float `f64`
    F64 = 4,
    /// Date/time with nanoseconds precision
    Date = 5,
    /// Facet
    Facet = 6,
    /// Arbitrarily sized byte array
    Bytes = 7,
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr = 8,
    /// Bool value
    Bool = 9,
    /// Pre-tokenized str type,
    PreTokStr = 10,
    /// Object
    Object = 11,
    /// Pre-tokenized str type,
    Array = 12,
    /// Opaque payload of a plugin-defined custom field.
    Custom = 13,
}

impl BinarySerializable for ValueType {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        (*self as u8).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let num = u8::deserialize(reader)?;
        let type_id = if (0..=13).contains(&num) {
            unsafe { std::mem::transmute::<u8, ValueType>(num) }
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid value type id: {num}"),
            ));
        };
        Ok(type_id)
    }
}

impl<'a, V: Value<'a>> From<&ReferenceValue<'a, V>> for ValueType {
    fn from(value: &ReferenceValue<'a, V>) -> Self {
        match value {
            ReferenceValue::Leaf(leaf) => leaf.into(),
            ReferenceValue::Array(_) => ValueType::Array,
            ReferenceValue::Object(_) => ValueType::Object,
        }
    }
}
impl<'a> From<&ReferenceValueLeaf<'a>> for ValueType {
    fn from(value: &ReferenceValueLeaf<'a>) -> Self {
        match value {
            ReferenceValueLeaf::Null => ValueType::Null,
            ReferenceValueLeaf::Str(_) => ValueType::Str,
            ReferenceValueLeaf::U64(_) => ValueType::U64,
            ReferenceValueLeaf::I64(_) => ValueType::I64,
            ReferenceValueLeaf::F64(_) => ValueType::F64,
            ReferenceValueLeaf::Bool(_) => ValueType::Bool,
            ReferenceValueLeaf::Date(_) => ValueType::Date,
            ReferenceValueLeaf::IpAddr(_) => ValueType::IpAddr,
            ReferenceValueLeaf::PreTokStr(_) => ValueType::PreTokStr,
            ReferenceValueLeaf::Facet(_) => ValueType::Facet,
            ReferenceValueLeaf::Bytes(_) => ValueType::Bytes,
            ReferenceValueLeaf::Custom(_) => ValueType::Custom,
        }
    }
}

#[derive(Debug, Clone)]
/// The Iterator for the object values in the compact document
pub struct CompactDocObjectIter<'a> {
    container: &'a CompactDoc,
    node_addresses_slice: &'a [u8],
}

impl<'a> CompactDocObjectIter<'a> {
    fn new(container: &'a CompactDoc, addr: Addr) -> io::Result<Self> {
        // Objects are `&[ValueAddr]` serialized into bytes
        let node_addresses_slice = container.extract_bytes(addr);
        Ok(Self {
            container,
            node_addresses_slice,
        })
    }
}

/// Number of `ValueAddr`s in a serialized address list: each one is a type byte (< 128)
/// followed by a VInt whose last byte, and only that one, has its high bit set.
#[inline]
fn num_value_addrs(addresses: &[u8]) -> usize {
    addresses.iter().filter(|&&byte| byte >= 128).count()
}

impl<'a> Iterator for CompactDocObjectIter<'a> {
    type Item = (&'a str, CompactDocValue<'a>);

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_entries = num_value_addrs(self.node_addresses_slice) / 2;
        (num_entries, Some(num_entries))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.node_addresses_slice.is_empty() {
            return None;
        }
        let key_addr = ValueAddr::read_from_slice(&mut self.node_addresses_slice)?;
        let key = self.container.extract_str(key_addr.val_addr);
        let value = ValueAddr::read_from_slice(&mut self.node_addresses_slice)?;
        let value = CompactDocValue {
            container: self.container,
            value_addr: value,
        };
        Some((key, value))
    }
}

#[derive(Debug, Clone)]
/// The Iterator for the array values in the compact document
pub struct CompactDocArrayIter<'a> {
    container: &'a CompactDoc,
    node_addresses_slice: &'a [u8],
}

impl<'a> CompactDocArrayIter<'a> {
    fn new(container: &'a CompactDoc, addr: Addr) -> io::Result<Self> {
        // Arrays are &[ValueAddr] serialized into bytes
        let node_addresses_slice = container.extract_bytes(addr);
        Ok(Self {
            container,
            node_addresses_slice,
        })
    }
}

impl<'a> Iterator for CompactDocArrayIter<'a> {
    type Item = CompactDocValue<'a>;

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_values = num_value_addrs(self.node_addresses_slice);
        (num_values, Some(num_values))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.node_addresses_slice.is_empty() {
            return None;
        }
        let value = ValueAddr::read_from_slice(&mut self.node_addresses_slice)?;
        let value = CompactDocValue {
            container: self.container,
            value_addr: value,
        };
        Some(value)
    }
}

impl CompactDoc {
    /// Writes one value in the doc store encoding (`BinaryValueSerializer`), reading
    /// `node_data` directly. Returns `false` for value types left to the generic serializer.
    fn write_stored_value(&self, value_addr: ValueAddr, output: &mut Vec<u8>) -> bool {
        use super::type_codes;
        let addr = value_addr.val_addr as usize;
        // Strings and bytes are stored in `node_data` as VInt(len) + bytes, which is also their
        // doc store encoding after the type code.
        let copy_len_prefixed = |code: u8, output: &mut Vec<u8>| {
            let data = &self.node_data[addr..];
            let (len, rest) = read_vint_u32(data).expect("corrupted compact doc");
            let vint_len = data.len() - rest.len();
            output.push(code);
            output.extend_from_slice(&data[..vint_len + len as usize]);
        };
        let fixed_8 = |code: u8, transform: fn(u64) -> u64, output: &mut Vec<u8>| {
            // `write_into` serialized these with `BinarySerializable` (8 bytes, little endian).
            let bytes: [u8; 8] = self.node_data[addr..addr + 8].try_into().unwrap();
            output.push(code);
            output.extend_from_slice(&transform(u64::from_le_bytes(bytes)).to_le_bytes());
        };
        match value_addr.type_id {
            ValueType::Null => output.push(type_codes::NULL_CODE),
            ValueType::Str => copy_len_prefixed(type_codes::TEXT_CODE, output),
            ValueType::Facet => copy_len_prefixed(type_codes::HIERARCHICAL_FACET_CODE, output),
            ValueType::Bytes => copy_len_prefixed(type_codes::BYTES_CODE, output),
            ValueType::U64 => fixed_8(type_codes::U64_CODE, |v| v, output),
            ValueType::I64 => fixed_8(type_codes::I64_CODE, |v| v, output),
            ValueType::F64 => fixed_8(
                type_codes::F64_CODE,
                |bits| common::f64_to_u64(f64::from_bits(bits)),
                output,
            ),
            ValueType::Date => fixed_8(type_codes::DATE_CODE, |v| v, output),
            ValueType::Bool => {
                output.push(type_codes::BOOL_CODE);
                output.push((value_addr.val_addr != 0) as u8);
            }
            ValueType::Array | ValueType::Object => {
                let is_object = value_addr.type_id == ValueType::Object;
                let mut addresses = self.extract_bytes(value_addr.val_addr);
                // Objects are stored as [key, value, key, value, ...] in both encodings.
                let num_values = num_value_addrs(addresses);
                output.push(if is_object {
                    type_codes::OBJECT_CODE
                } else {
                    type_codes::ARRAY_CODE
                });
                let mut buf = [0u8; 8];
                output.extend_from_slice(serialize_vint_u32(num_values as u32, &mut buf));
                while let Some(child) = ValueAddr::read_from_slice(&mut addresses) {
                    if !self.write_stored_value(child, output) {
                        return false;
                    }
                }
            }
            // Rare: keep the generic serializer as the single source of truth.
            ValueType::IpAddr | ValueType::PreTokStr | ValueType::Custom => return false,
        }
        true
    }
}

impl Document for CompactDoc {
    type Value<'a> = CompactDocValue<'a>;
    type FieldsValuesIter<'a> = FieldValueIterRef<'a>;

    fn serialize_stored_fields(&self, schema: &Schema, output: &mut Vec<u8>) -> bool {
        let start = output.len();
        let num_stored = self
            .field_values
            .iter()
            .filter(|field_value| {
                schema
                    .get_field_entry(Field::from_field_id(field_value.field as u32))
                    .is_stored()
            })
            .count();
        let mut buf = [0u8; 8];
        output.extend_from_slice(serialize_vint_u32(num_stored as u32, &mut buf));
        for field_value in &self.field_values {
            let field = Field::from_field_id(field_value.field as u32);
            if !schema.get_field_entry(field).is_stored() {
                continue;
            }
            output.extend_from_slice(&(field_value.field as u32).to_le_bytes());
            if !self.write_stored_value(field_value.value_addr, output) {
                output.truncate(start);
                return false;
            }
        }
        true
    }

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldValueIterRef {
            slice: self.field_values.iter(),
            container: self,
        }
    }

    fn into_tantivy_document(self) -> TantivyDocument {
        self
    }
}

/// A helper wrapper for creating an iterator over the field values
pub struct FieldValueIterRef<'a> {
    slice: std::slice::Iter<'a, FieldValueAddr>,
    container: &'a CompactDoc,
}

impl<'a> Iterator for FieldValueIterRef<'a> {
    type Item = (Field, CompactDocValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|field_value| {
            (
                Field::from_field_id(field_value.field as u32),
                CompactDocValue::<'a> {
                    container: self.container,
                    value_addr: field_value.value_addr,
                },
            )
        })
    }
}

/// Error that may happen when deserializing
/// a document from JSON.
#[derive(Debug, Error, PartialEq)]
pub enum DocParsingError {
    /// The payload given is not valid JSON.
    #[error("The provided string is not valid JSON")]
    InvalidJson(String),
    /// One of the value node could not be parsed.
    #[error("The field '{0:?}' could not be parsed: {1:?}")]
    ValueError(String, ValueParsingError),
}

impl DocParsingError {
    /// Builds a NotJson DocParsingError
    fn invalid_json(invalid_json: &str) -> Self {
        let sample = invalid_json.chars().take(20).collect();
        DocParsingError::InvalidJson(sample)
    }
}

#[cfg(test)]
mod fast_decode_tests {
    use std::net::Ipv6Addr;

    use common::{serialize_vint_u32, BinarySerializable, DateTime};
    use proptest::prelude::*;

    use super::{read_vint_u32, ValueAddr, ValueType};
    use crate::schema::document::{BinaryDocumentSerializer, Document};
    use crate::schema::{Facet, OwnedValue, Schema, FAST, STORED, TEXT};
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::TantivyDocument;

    fn leaf_value() -> impl Strategy<Value = OwnedValue> {
        prop_oneof![
            Just(OwnedValue::Null),
            ".{0,40}".prop_map(OwnedValue::Str),
            "[a-z]{0,300}".prop_map(OwnedValue::Str),
            any::<u64>().prop_map(OwnedValue::U64),
            any::<i64>().prop_map(OwnedValue::I64),
            any::<f64>().prop_map(OwnedValue::F64),
            any::<bool>().prop_map(OwnedValue::Bool),
            any::<i64>().prop_map(|ts| OwnedValue::Date(DateTime::from_timestamp_nanos(ts))),
            proptest::collection::vec(any::<u8>(), 0..20).prop_map(OwnedValue::Bytes),
            "[a-z]{1,5}".prop_map(|s| OwnedValue::Facet(Facet::from_path([s.as_str()]))),
        ]
    }

    fn value() -> impl Strategy<Value = OwnedValue> {
        leaf_value().prop_recursive(3, 40, 6, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..6).prop_map(OwnedValue::Array),
                proptest::collection::vec(("[a-z.]{0,8}", inner), 0..6)
                    .prop_map(OwnedValue::Object),
            ]
        })
    }

    fn generic_bytes(doc: &TantivyDocument, schema: &Schema) -> Vec<u8> {
        let mut out = Vec::new();
        BinaryDocumentSerializer::new(&mut out, schema)
            .serialize_doc(doc)
            .unwrap();
        out
    }

    proptest! {
        #[test]
        fn test_fast_stored_serialization_matches_generic(
            values in proptest::collection::vec((0u32..4, value()), 0..10),
        ) {
            // Fields 0 and 2 are stored, 1 and 3 are not.
            let mut schema_builder = Schema::builder();
            schema_builder.add_json_field("a", STORED);
            schema_builder.add_json_field("b", TEXT);
            schema_builder.add_json_field("c", STORED | FAST);
            schema_builder.add_text_field("d", TEXT);
            let schema = schema_builder.build();
            let mut doc = TantivyDocument::default();
            for (field_id, value) in &values {
                doc.add_field_value(crate::schema::Field::from_field_id(*field_id), value);
            }
            let mut fast = Vec::new();
            prop_assert!(doc.serialize_stored_fields(&schema, &mut fast));
            prop_assert_eq!(fast, generic_bytes(&doc, &schema));
        }
    }

    #[test]
    fn test_fast_stored_serialization_falls_back_for_rare_types() {
        let mut schema_builder = Schema::builder();
        let ip = schema_builder.add_ip_addr_field("ip", STORED);
        let text = schema_builder.add_text_field("text", STORED);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        doc.add_text(text, "kept");
        doc.add_ip_addr(ip, Ipv6Addr::LOCALHOST);
        let mut out = vec![42u8];
        assert!(!doc.serialize_stored_fields(&schema, &mut out));
        assert_eq!(out, vec![42u8], "output is left untouched on fallback");

        let mut doc = TantivyDocument::default();
        doc.add_pre_tokenized_text(
            text,
            PreTokenizedString {
                text: "pre tokenized".to_string(),
                tokens: vec![Token::default()],
            },
        );
        assert!(!doc.serialize_stored_fields(&schema, &mut Vec::new()));
    }

    proptest! {
        #[test]
        fn test_read_vint_u32_matches_serialize(val in any::<u32>(), tail in proptest::collection::vec(any::<u8>(), 0..4)) {
            let mut buf = [0u8; 8];
            let mut bytes = serialize_vint_u32(val, &mut buf).to_vec();
            let encoded_len = bytes.len();
            bytes.extend_from_slice(&tail);
            let (decoded, rest) = read_vint_u32(&bytes).unwrap();
            prop_assert_eq!(decoded, val);
            prop_assert_eq!(rest, &bytes[encoded_len..]);
        }

        #[test]
        fn test_compact_doc_round_trips_nested_values(
            values in proptest::collection::vec(value(), 0..8),
        ) {
            let mut doc = TantivyDocument::default();
            for (idx, value) in values.iter().enumerate() {
                doc.add_field_value(crate::schema::Field::from_field_id(idx as u32 % 3), value);
            }
            let read_back: Vec<OwnedValue> = doc
                .iter_fields_and_values()
                .map(|(_, value)| OwnedValue::from(value))
                .collect();
            // NaN != NaN: compare debug strings.
            prop_assert_eq!(format!("{read_back:?}"), format!("{values:?}"));
        }

        #[test]
        fn test_value_addr_write_to_vec_matches_serialize(type_byte in 0u8..=13, val_addr in any::<u32>()) {
            let type_id: ValueType = unsafe { std::mem::transmute::<u8, ValueType>(type_byte) };
            let addr = ValueAddr { type_id, val_addr };
            let mut expected = Vec::new();
            addr.serialize(&mut expected).unwrap();
            let mut actual = Vec::new();
            addr.write_to_vec(&mut actual);
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn test_value_addr_read_from_slice_matches_deserialize(type_byte in 0u8..=13, val_addr in any::<u32>()) {
            let type_id: ValueType = unsafe { std::mem::transmute::<u8, ValueType>(type_byte) };
            let addr = ValueAddr { type_id, val_addr };
            let mut bytes = Vec::new();
            addr.serialize(&mut bytes).unwrap();
            let mut slice = &bytes[..];
            let decoded = ValueAddr::read_from_slice(&mut slice).unwrap();
            prop_assert!(slice.is_empty());
            prop_assert_eq!(decoded.type_id, type_id);
            prop_assert_eq!(decoded.val_addr, val_addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::*;

    #[test]
    fn test_doc() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = TantivyDocument::default();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.field_values().count(), 1);

        let schema = schema_builder.build();
        let _val = doc.get_first(text_field).unwrap();
        let _json = doc.to_named_doc(&schema);
    }

    #[test]
    fn test_json_value() {
        let json_str = r#"{
            "toto": "titi",
            "float": -0.2,
            "bool": true,
            "unsigned": 1,
            "signed": -2,
            "complexobject": {
                "field.with.dot": 1
            },
            "date": "1985-04-12T23:20:50.52Z",
            "my_arr": [2, 3, {"my_key": "two tokens"}, 4, {"nested_array": [2, 5, 6, [7, 8, {"a": [{"d": {"e":[99]}}, 9000]}, 9, 10], [5, 5]]}]
        }"#;
        let json_val: std::collections::BTreeMap<String, OwnedValue> =
            serde_json::from_str(json_str).unwrap();

        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let mut doc = TantivyDocument::default();
        doc.add_object(json_field, json_val);

        let schema = schema_builder.build();
        let json = doc.to_json(&schema);
        let actual_json: serde_json::Value = serde_json::from_str(&json).unwrap();
        let expected_json: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(actual_json["json"][0], expected_json);
    }

    // TODO: Should this be re-added with the serialize method
    //       technically this is no longer useful since the doc types
    //       do not implement BinarySerializable due to orphan rules.
    // #[test]
    // fn test_doc_serialization_issue() {
    //     let mut doc = Document::default();
    //     doc.add_json_object(
    //         Field::from_field_id(0),
    //         serde_json::json!({"key": 2u64})
    //             .as_object()
    //             .unwrap()
    //             .clone(),
    //     );
    //     doc.add_text(Field::from_field_id(1), "hello");
    //     assert_eq!(doc.field_values().len(), 2);
    //     let mut payload: Vec<u8> = Vec::new();
    //     doc_binary_wrappers::serialize(&doc, &mut payload).unwrap();
    //     assert_eq!(payload.len(), 26);
    //     doc_binary_wrappers::deserialize::<Document, _>(&mut &payload[..]).unwrap();
    // }
}
