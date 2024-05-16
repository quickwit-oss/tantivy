use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::Ipv6Addr;

use columnar::MonotonicallyMappableToU128;
use common::{read_u32_vint_no_advance, serialize_vint_u32, BinarySerializable, DateTime};
use serde_json::Map;
pub use CompactDoc as TantivyDocument;

use super::{ReferenceValue, ReferenceValueLeaf, Value};
use crate::schema::document::{
    DeserializeError, Document, DocumentDeserialize, DocumentDeserializer,
};
use crate::schema::field_type::ValueParsingError;
use crate::schema::{Facet, Field, NamedFieldDocument, OwnedValue, Schema};
use crate::tokenizer::PreTokenizedString;

#[repr(packed)]
#[derive(Debug, Clone)]
/// A field value pair in the compact tantivy document
pub struct FieldValueAddr {
    pub field: u16,
    pub value: ValueAddr,
}

#[derive(Debug, Clone)]
/// The default document in tantivy. It encodes data in a compact form.
pub struct CompactDoc {
    /// Container to encode data from values
    container: CompactDocContainer,
    /// The root (Field, Value) pairs
    field_values: mediumvec::Vec32<FieldValueAddr>,
}
impl Default for CompactDoc {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactDoc {
    /// Creates a new, empty document object
    pub fn new() -> CompactDoc {
        CompactDoc {
            container: Default::default(),
            field_values: mediumvec::Vec32::with_capacity(4),
        }
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
            value: self.container.add_value(value),
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
            value: self.container.add_value_leaf(value),
        };
        self.field_values.push(field_value);
    }

    /// field_values accessor
    pub fn field_values(
        &self,
    ) -> impl Iterator<Item = (Field, ReferenceValue<'_, CompactDocValue<'_>>)> {
        self.field_values.iter().map(|field_val| {
            let field = Field::from_field_id(field_val.field as u32);
            let val = self.container.extract_ref_value(field_val.value).unwrap();
            (field, val)
        })
    }

    /// Returns all of the `ReferenceValue`s associated the given field
    pub fn get_all(
        &self,
        field: Field,
    ) -> impl Iterator<Item = ReferenceValue<'_, CompactDocValue<'_>>> + '_ {
        self.field_values
            .iter()
            .filter(move |field_value| Field::from_field_id(field_value.field as u32) == field)
            .map(|val| self.container.extract_ref_value(val.value).unwrap())
    }

    /// Returns the first `ReferenceValue` associated the given field
    pub fn get_first(&self, field: Field) -> Option<ReferenceValue<'_, CompactDocValue<'_>>> {
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
}

impl PartialEq for CompactDoc {
    fn eq(&self, other: &Self) -> bool {
        // super slow, but only here for tests
        let convert_to_comparable_map = |doc: &CompactDoc| {
            let mut field_value_set: HashMap<Field, HashSet<String>> = Default::default();
            for field_value in doc.field_values.iter() {
                let value: OwnedValue = doc
                    .container
                    .extract_ref_value(field_value.value)
                    .unwrap()
                    .into();
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
    container: &'a CompactDocContainer,
    value: ValueAddr,
}
impl<'a> Value<'a> for CompactDocValue<'a> {
    type ArrayIter = CompactDocArrayIter<'a>;

    type ObjectIter = CompactDocObjectIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        self.container.extract_ref_value(self.value).unwrap()
    }
}

#[derive(Debug, Clone)]
/// A container to store tantivy Value
struct CompactDocContainer {
    /// A list of nodes, that are used to store the values of the document.
    ///
    /// ## Note on the design
    /// We could use just a single vec `node_data` to store the nodes, but this would have a
    /// downside. node_data has flexible sized elements compared to `nodes`. So when creating
    /// a vec or document, we can reserve space for all od the direct child nodes upfront, and then
    /// write into the nodes array without resizing. This is not possible with `node_data`. So
    /// we use `nodes` to store the references to the actual data in `node_data`.
    /// There would be 2 ways to use node_data instead of nodes:
    /// - Instead of storing start and len for arrays and objects, we could a list of node pointers
    /// to the bytes in node_data. This would require more memory.
    /// - A two layer approach, where we when receiving an array/object, we would process the first
    ///   level
    /// subnodes and store the pos and len in and then handle then deeper levels. I don't like the
    /// added complexity of this approach.
    nodes: mediumvec::Vec32<ValueAddr>,
    /// The `node_data` is a vec of bytes, where each value is serialized into bytes and stored. It
    /// includes all the data of the document.
    node_data: mediumvec::Vec32<u8>,
}
impl Default for CompactDocContainer {
    fn default() -> Self {
        Self {
            nodes: mediumvec::Vec32::with_capacity(4),
            // This should be at the lower end of the payload of a document
            // 512 byte is pretty small
            node_data: mediumvec::Vec32::with_capacity(512),
        }
    }
}

#[derive(Clone, Copy, Default)]
/// The value type and the address to its payload in the container.
/// Since Addr is only 3 bytes, the struct size is only 4bytes
pub struct ValueAddr {
    type_id: ValueType,
    val: Addr, // this is the address, except for bool and null, which are inlined
}
impl std::fmt::Debug for ValueAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.type_id))
    }
}
/// Addr in 3 bytes, can be converted from u32 by dropping the high byte.
/// This means that we can address at most 16MB data in a Document.
#[derive(Clone, Default, Eq, PartialEq, Debug, Copy)]
struct Addr([u8; 3]);
// TODO: use fallible conversion
impl From<u32> for Addr {
    fn from(val: u32) -> Self {
        let bytes = val.to_be_bytes();
        Addr([bytes[1], bytes[2], bytes[3]])
    }
}
impl From<Addr> for u32 {
    fn from(val: Addr) -> Self {
        let mut bytes = [0; 4];
        bytes[0] = 0;
        bytes[1..].copy_from_slice(&val.0);
        u32::from_be_bytes(bytes)
    }
}

impl ValueAddr {
    pub fn new(type_id: ValueType, val: u32) -> Self {
        Self {
            type_id,
            val: val.into(),
        }
    }
}

/// A enum representing a leaf value for tantivy to index.
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeAddress {
    /// position in the node array
    pos: u32,
    /// num elements in the node array
    num_nodes: u32,
}
impl BinarySerializable for NodeAddress {
    fn serialize<W: std::io::Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        self.pos.serialize(writer)?;
        self.num_nodes.serialize(writer)
    }
    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let pos = u32::deserialize(reader)?;
        let num_nodes = u32::deserialize(reader)?;
        Ok(NodeAddress { pos, num_nodes })
    }
}

impl CompactDocContainer {
    pub fn add_value_leaf(&mut self, leaf: ReferenceValueLeaf) -> ValueAddr {
        let type_id = ValueType::from(&leaf);
        match leaf {
            ReferenceValueLeaf::Null => ValueAddr::new(type_id, 0),
            ReferenceValueLeaf::Str(bytes) => ValueAddr::new(
                type_id,
                write_bytes_into(&mut self.node_data, bytes.as_bytes()),
            ),
            ReferenceValueLeaf::Facet(bytes) => ValueAddr::new(
                type_id,
                write_bytes_into(&mut self.node_data, bytes.as_bytes()),
            ),
            ReferenceValueLeaf::Bytes(bytes) => {
                ValueAddr::new(type_id, write_bytes_into(&mut self.node_data, bytes))
            }
            ReferenceValueLeaf::U64(num) => {
                ValueAddr::new(type_id, write_into(&mut self.node_data, num))
            }
            ReferenceValueLeaf::I64(num) => {
                ValueAddr::new(type_id, write_into(&mut self.node_data, num))
            }
            ReferenceValueLeaf::F64(num) => {
                ValueAddr::new(type_id, write_into(&mut self.node_data, num))
            }
            ReferenceValueLeaf::Bool(b) => ValueAddr::new(type_id, b as u32),
            ReferenceValueLeaf::Date(date) => ValueAddr::new(
                type_id,
                write_into(&mut self.node_data, date.into_timestamp_nanos()),
            ),
            ReferenceValueLeaf::IpAddr(num) => {
                ValueAddr::new(type_id, write_into(&mut self.node_data, num.to_u128()))
            }
            ReferenceValueLeaf::PreTokStr(pre_tok) => {
                ValueAddr::new(type_id, write_into(&mut self.node_data, *pre_tok))
            }
        }
    }
    pub fn add_value<'a, V: Value<'a>>(&mut self, value: V) -> ValueAddr {
        let value = value.as_value();
        let type_id = ValueType::from(&value);
        match value {
            ReferenceValue::Leaf(leaf) => self.add_value_leaf(leaf),
            ReferenceValue::Array(elements) => {
                let elements: Vec<_> = elements.collect(); // TODO: Use ExactSizeIterator?
                let pos = self.nodes.len() as u32;
                let len = elements.len() as u32;
                // We reserve space upfront, so that we can write into the nodes array and just
                // reference it with pos + len
                self.nodes
                    .as_vec(|vec| vec.resize(pos as usize + len as usize, ValueAddr::default()));
                for (idx, elem) in elements.into_iter().enumerate() {
                    let ref_elem = self.add_value(elem);
                    let node_idx = idx + pos as usize;
                    self.nodes[node_idx] = ref_elem;
                }
                ValueAddr::new(
                    type_id,
                    write_into(
                        &mut self.node_data,
                        NodeAddress {
                            pos,
                            num_nodes: len,
                        },
                    ),
                )
            }
            ReferenceValue::Object(entries) => {
                let entries: Vec<_> = entries.collect(); // TODO: Use ExactSizeIterator?
                let pos = self.nodes.len() as u32;
                let len = entries.len() as u32;
                // We reserve space upfront, so that we can write into the nodes array and just
                // reference it with pos + len
                self.nodes.as_vec(|vec| {
                    vec.resize(pos as usize + (len * 2) as usize, ValueAddr::default())
                });
                for (idx, (key, value)) in entries.into_iter().enumerate() {
                    let node_idx = (idx * 2) + pos as usize;
                    let ref_key = self.add_value_leaf(ReferenceValueLeaf::Str(key));
                    let ref_value = self.add_value(value);
                    self.nodes[node_idx] = ref_key;
                    self.nodes[node_idx + 1] = ref_value;
                }
                ValueAddr::new(
                    type_id,
                    write_into(
                        &mut self.node_data,
                        NodeAddress {
                            pos,
                            num_nodes: len,
                        },
                    ),
                )
            }
        }
    }
}

/// BinarySerializable for &str
/// Specialized version since BinarySerializable doesn't have lifetimes.
fn binary_deserialize_str(data: &[u8]) -> &str {
    let data = binary_deserialize_bytes(data);
    unsafe { std::str::from_utf8_unchecked(data) }
}

/// BinarySerializable for &[u8]
/// Specialized version since BinarySerializable doesn't have lifetimes.
fn binary_deserialize_bytes(data: &[u8]) -> &[u8] {
    let (len, bytes_read) = read_u32_vint_no_advance(data);
    &data[bytes_read..bytes_read + len as usize]
}

/// BinarySerializable alternative for borrowed data
fn write_bytes_into(data: &mut mediumvec::Vec32<u8>, bytes: &[u8]) -> u32 {
    let pos = data.len() as u32;
    let mut buf = [0u8; 8];
    let vint_bytes = serialize_vint_u32(bytes.len() as u32, &mut buf);
    data.as_vec(|vec| vec.extend_from_slice(vint_bytes));
    data.as_vec(|vec| vec.extend_from_slice(bytes));
    pos
}

/// Serialize and return the position
fn write_into<T: BinarySerializable>(data: &mut mediumvec::Vec32<u8>, value: T) -> u32 {
    let pos = data.len() as u32;
    data.as_vec(|vec| value.serialize(vec).unwrap());
    pos
}

impl CompactDocContainer {
    pub fn extract_ref_value(
        &self,
        ref_value: ValueAddr,
    ) -> io::Result<ReferenceValue<'_, CompactDocValue<'_>>> {
        match ref_value.type_id {
            ValueType::Null => Ok(ReferenceValueLeaf::Null.into()),
            ValueType::Str => {
                let data = binary_deserialize_bytes(self.get_slice(ref_value.val));
                let str_ref = unsafe { std::str::from_utf8_unchecked(data) };
                Ok(ReferenceValueLeaf::Str(str_ref).into())
            }
            ValueType::Facet => {
                let data = binary_deserialize_bytes(self.get_slice(ref_value.val));
                let str_ref = unsafe { std::str::from_utf8_unchecked(data) };
                Ok(ReferenceValueLeaf::Facet(str_ref).into())
            }
            ValueType::Bytes => {
                let data = binary_deserialize_bytes(self.get_slice(ref_value.val));
                Ok(ReferenceValueLeaf::Bytes(data).into())
            }
            ValueType::U64 => self
                .read_from::<u64>(ref_value.val)
                .map(ReferenceValueLeaf::U64)
                .map(Into::into),
            ValueType::I64 => self
                .read_from::<i64>(ref_value.val)
                .map(ReferenceValueLeaf::I64)
                .map(Into::into),
            ValueType::F64 => self
                .read_from::<f64>(ref_value.val)
                .map(ReferenceValueLeaf::F64)
                .map(Into::into),
            ValueType::Bool => Ok(ReferenceValueLeaf::Bool(u32::from(ref_value.val) != 0).into()),
            ValueType::Date => self
                .read_from::<i64>(ref_value.val)
                .map(|ts| ReferenceValueLeaf::Date(DateTime::from_timestamp_nanos(ts)))
                .map(Into::into),
            ValueType::IpAddr => self
                .read_from::<u128>(ref_value.val)
                .map(|num| ReferenceValueLeaf::IpAddr(Ipv6Addr::from_u128(num)))
                .map(Into::into),
            ValueType::PreTokStr => self
                .read_from::<PreTokenizedString>(ref_value.val)
                .map(Into::into)
                .map(ReferenceValueLeaf::PreTokStr)
                .map(Into::into),
            ValueType::Object => Ok(ReferenceValue::Object(CompactDocObjectIter::new(
                self,
                ref_value.val,
            )?)),
            ValueType::Array => Ok(ReferenceValue::Array(CompactDocArrayIter::new(
                self,
                ref_value.val,
            )?)),
        }
    }

    pub fn extract_str(&self, ref_value: ValueAddr) -> &str {
        binary_deserialize_str(self.get_slice(ref_value.val))
    }

    fn read_from<T: BinarySerializable>(&self, addr: Addr) -> io::Result<T> {
        let start = u32::from(addr) as usize;
        let data_slice = &self.node_data[start..];
        let mut cursor = std::io::Cursor::new(data_slice);
        T::deserialize(&mut cursor)
    }

    fn get_slice(&self, addr: Addr) -> &[u8] {
        let start = u32::from(addr) as usize;
        &self.node_data[start..]
    }
}

#[derive(Debug, Clone)]
/// The Iterator for the object values in the compact document
pub struct CompactDocObjectIter<'a> {
    container: &'a CompactDocContainer,
    index: usize,
    end: usize,
}

impl<'a> CompactDocObjectIter<'a> {
    fn new(container: &'a CompactDocContainer, addr: Addr) -> io::Result<Self> {
        let node_address = container.read_from::<NodeAddress>(addr)?;
        let start = node_address.pos as usize;
        let end = start + node_address.num_nodes as usize * 2;
        Ok(Self {
            container,
            index: start,
            end,
        })
    }
}

impl<'a> Iterator for CompactDocObjectIter<'a> {
    type Item = (&'a str, CompactDocValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let key_index = self.index;
            self.index += 2;
            let key = self.container.extract_str(self.container.nodes[key_index]);
            let value = CompactDocValue {
                container: self.container,
                value: self.container.nodes[key_index + 1],
            };
            return Some((key, value));
        }
        None
    }
}

#[derive(Debug, Clone)]
/// The Iterator for the array values in the compact document
pub struct CompactDocArrayIter<'a> {
    container: &'a CompactDocContainer,
    index: usize,
    end: usize,
}

impl<'a> CompactDocArrayIter<'a> {
    fn new(structure: &'a CompactDocContainer, addr: Addr) -> io::Result<Self> {
        let node_address = structure.read_from::<NodeAddress>(addr)?;
        let start = node_address.pos as usize;
        let end = start + node_address.num_nodes as usize;
        Ok(Self {
            container: structure,
            index: start,
            end,
        })
    }
}

impl<'a> Iterator for CompactDocArrayIter<'a> {
    type Item = CompactDocValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let key_index = self.index;
            let value = CompactDocValue {
                container: self.container,
                value: self.container.nodes[key_index],
            };
            self.index += 1;
            return Some(value);
        }
        None
    }
}

impl Document for CompactDoc {
    type Value<'a> = CompactDocValue<'a>;
    type FieldsValuesIter<'a> = FieldValueIterRef<'a>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldValueIterRef {
            slice: self.field_values.iter(),
            container: &self.container,
        }
    }
}

/// A helper wrapper for creating standard iterators
/// out of the fields iterator trait.
pub struct FieldValueIterRef<'a> {
    slice: std::slice::Iter<'a, FieldValueAddr>,
    container: &'a CompactDocContainer,
}

impl<'a> Iterator for FieldValueIterRef<'a> {
    type Item = (Field, CompactDocValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|field_value| {
            (
                Field::from_field_id(field_value.field as u32),
                CompactDocValue::<'a> {
                    container: self.container,
                    value: field_value.value,
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
            "my_arr": [2, 3, {"my_key": "two tokens"}, 4, {"nested_array": [2, 5, 6]}]
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
