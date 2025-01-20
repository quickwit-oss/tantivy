// src/schema/document/default_document.rs
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::Ipv6Addr;

use columnar::MonotonicallyMappableToU128;
use common::{read_u32_vint_no_advance, serialize_vint_u32, BinarySerializable, DateTime, VInt};
use serde_json::Map;
pub use CompactDoc as TantivyDocument;

use super::{FieldType, ReferenceValue, ReferenceValueLeaf, Value};
use crate::schema::document::{
    DeserializeError, Document, DocumentDeserialize, DocumentDeserializer,
};
use crate::schema::field_type::ValueParsingError;
use crate::schema::{Facet, Field, NamedFieldDocument, OwnedValue, Schema};
use crate::tokenizer::PreTokenizedString;

#[repr(packed)]
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
    where
        Facet: From<F>,
    {
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

    /// Parse a JSON object, expanding any `FieldType::Nested` fields
    /// into multiple child docs, returning `(child_docs + 1 parent_doc)`.
    ///
    /// The parent doc is appended last (like Elasticsearch),
    /// child docs come first, in a single block.
    /// Expands a single JSON object that may contain nested arrays.
    /// Returns one or more `TantivyDocument`: all child docs first, then the parent doc last.
    pub fn parse_json_for_nested(
        schema: &Schema,
        top_level_json_str: &str,
    ) -> Result<Vec<TantivyDocument>, DocParsingError> {
        let value: serde_json::Value = serde_json::from_str(top_level_json_str)
            .map_err(|e| DocParsingError::InvalidJson(e.to_string()))?;

        if !value.is_object() {
            return Err(DocParsingError::InvalidJson(
                "Top-level JSON must be an object".to_string(),
            ));
        }

        // We'll keep a Vec of child docs, plus a "parent doc."
        let mut child_docs: Vec<TantivyDocument> = Vec::new();
        let mut parent_doc = TantivyDocument::default();

        // For each key => json_value in the top-level object
        let obj = value.as_object().unwrap();
        for (field_name, json_value) in obj {
            let field_opt = schema.get_field(field_name).ok();
            if field_opt.is_none() {
                // unknown field => skip or handle
                continue;
            }
            let field = field_opt.unwrap();
            let field_entry = schema.get_field_entry(field);

            match field_entry.field_type() {
                FieldType::Nested(nested_opts) => {
                    // We expect an array of objects
                    if let Some(arr) = json_value.as_array() {
                        for child_object in arr {
                            if !child_object.is_object() {
                                // skip or handle error
                                continue;
                            }

                            // Build a child doc
                            let mut child_doc = TantivyDocument::default();

                            // parse each subfield from `child_object` into `child_doc`
                            // also flatten into parent if needed
                            let child_obj_map = child_object.as_object().unwrap();
                            for (child_key, child_val) in child_obj_map {
                                if let Ok(child_field) = schema.get_field(child_key) {
                                    // parse the child's field normally
                                    Self::parse_one_field(
                                        schema,
                                        child_field,
                                        child_val,
                                        &mut child_doc,
                                    )?;

                                    // If include_in_parent => also copy to the parent
                                    if nested_opts.include_in_parent {
                                        Self::parse_one_field(
                                            schema,
                                            child_field,
                                            child_val,
                                            &mut parent_doc,
                                        )?;
                                    }
                                } else {
                                    // unknown field => skip or handle
                                }
                            }
                            child_docs.push(child_doc);
                        }
                    }
                    // else skip or handle if user gave non-array
                }
                // normal field => parse directly into the parent doc
                _ => {
                    Self::parse_one_field(schema, field, json_value, &mut parent_doc)?;
                }
            }
        }

        // Finally, add the parent doc last
        child_docs.push(parent_doc);

        Ok(child_docs)
    }

    /// Helper to parse a single field's JSON value & insert into doc
    fn parse_one_field(
        schema: &Schema,
        field: Field,
        val: &serde_json::Value,
        doc: &mut TantivyDocument,
    ) -> Result<(), DocParsingError> {
        let field_entry = schema.get_field_entry(field);

        // We skip if field is `Nested(...)`, because top-level array expansions
        // are handled above. If user incorrectly gave e.g. { "myNested": { ... } }, we can error:
        if field_entry.field_type().is_nested() {
            return Err(DocParsingError::ValueError(
                schema.get_field_name(field).to_string(),
                ValueParsingError::TypeError {
                    expected: "array-of-objects for nested fields",
                    json: val.clone(),
                },
            ));
        }

        // If the field is normal, parse it using your usual approach (like `field_type.value_from_json_non_nested(val.clone())`)
        let typed_val = field_entry
            .field_type()
            .value_from_json_non_nested(val.clone()) // clone because we need ownership
            .map_err(|err| {
                DocParsingError::ValueError(schema.get_field_name(field).to_string(), err)
            })?;

        doc.add_field_value(field, &typed_val);
        Ok(())
    }

    /// A small helper that takes a single field + JSON value + schema,
    /// parses it, and adds it to self. This is what your code calls
    /// `doc.parse_and_add_json(field, json_val, schema)?;`
    pub fn parse_and_add_json(
        &mut self,
        field: Field,
        json_val: &serde_json::Value,
        schema: &Schema,
    ) -> Result<(), DocParsingError> {
        let field_entry = schema.get_field_entry(field);
        // If it's nested, error or skip
        if field_entry.field_type().is_nested() {
            return Err(DocParsingError::ValueError(
                schema.get_field_name(field).to_string(),
                ValueParsingError::TypeError {
                    expected: "array-of-objects for nested fields",
                    json: json_val.clone(),
                },
            ));
        }

        // Otherwise parse normally:
        let typed_val = field_entry
            .field_type()
            .value_from_json_non_nested(json_val.clone())
            .map_err(|err| {
                DocParsingError::ValueError(schema.get_field_name(field).to_string(), err)
            })?;

        // Insert into doc
        self.add_field_value(field, &typed_val);
        Ok(())
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
        let value = value.as_value();
        let type_id = ValueType::from(&value);
        match value {
            ReferenceValue::Leaf(leaf) => self.add_value_leaf(leaf),
            ReferenceValue::Array(elements) => {
                // addresses of the elements in node_data
                // Reusing a vec would be nicer, but it's not easy because of the recursion
                // A global vec would work if every writer get it's discriminator
                let mut addresses = Vec::new();
                for elem in elements {
                    let value_addr = self.add_value(elem);
                    write_into(&mut addresses, value_addr);
                }
                ValueAddr {
                    type_id,
                    val_addr: write_bytes_into(&mut self.node_data, &addresses),
                }
            }
            ReferenceValue::Object(entries) => {
                // addresses of the elements in node_data
                let mut addresses = Vec::new();
                for (key, value) in entries {
                    let key_addr = self.add_value_leaf(ReferenceValueLeaf::Str(key));
                    let value_addr = self.add_value(value);
                    write_into(&mut addresses, key_addr);
                    write_into(&mut addresses, value_addr);
                }
                ValueAddr {
                    type_id,
                    val_addr: write_bytes_into(&mut self.node_data, &addresses),
                }
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
    let (len, bytes_read) = read_u32_vint_no_advance(data);
    &data[bytes_read..bytes_read + len as usize]
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

/// Internal function that returns a list of docs: all children plus one parent doc.
fn parse_nested_inner(
    schema: &Schema,
    val: &serde_json::Value,
) -> Result<Vec<TantivyDocument>, DocParsingError> {
    println!("\nENTER parse_nested_inner()");
    println!("Input value: {}", val);

    let mut child_docs = Vec::new();
    let mut parent_doc = TantivyDocument::default();

    let obj = val.as_object().ok_or_else(|| {
        println!("ERROR: Input was not a JSON object");
        DocParsingError::InvalidJson("Expected top-level object for nested parse".into())
    })?;

    println!("Processing top-level object with {} fields", obj.len());

    // We'll iterate over top-level fields. If the field is nested, we expand it.
    for (key, val_json) in obj {
        println!("\nProcessing field '{}' with value: {}", key, val_json);
        
        let field_opt = schema.get_field(key).ok(); // if unknown, skip or handle
        if let Some(field) = field_opt {
            println!("Found schema field id={}", field.field_id());
            let fentry = schema.get_field_entry(field);
            match fentry.field_type() {
                FieldType::Nested(nested_opts) => {
                    println!("Field is NESTED type with include_in_parent={}", nested_opts.include_in_parent);
                    // Expect array-of-objects.
                    if let Some(arr) = val_json.as_array() {
                        println!("Processing nested array with {} elements", arr.len());
                        for (i, child_object) in arr.iter().enumerate() {
                            println!("\nProcessing array element {} of {}", i+1, arr.len());
                            if !child_object.is_object() {
                                println!("WARNING: Skipping non-object element: {}", child_object);
                                continue;
                            }
                            println!("Child object: {}", child_object);
                            
                            // parse child doc
                            let mut child_doc = TantivyDocument::default();
                            println!("Created new child doc");
                            
                            // flatten fields from that object into child_doc
                            parse_into_doc(schema, child_object, &mut child_doc)?;
                            println!("Parsed fields into child doc");

                            // If include_in_parent is set, also add these child fields to parent doc
                            if nested_opts.include_in_parent {
                                println!("Copying child fields to parent (include_in_parent=true)");
                                parse_into_doc(schema, child_object, &mut parent_doc)?;
                            }
                            
                            println!("Adding child doc to collection (now {} children)", child_docs.len() + 1);
                            child_docs.push(child_doc);
                        }
                    } else {
                        println!("WARNING: Expected array for nested field '{}' but got: {}", key, val_json);
                        // The user might supply a single object or invalid data
                        // For simplicity, skip or handle error
                    }
                }
                // Otherwise, parse as normal
                _ => {
                    println!("Processing regular (non-nested) field");
                    parse_single_field(schema, field, val_json, &mut parent_doc)?;
                }
            }
        } else {
            println!("WARNING: Unknown field '{}' - skipping", key);
            // unknown field => your policy. We can skip or parse with heuristics
        }
    }

    // The parent doc goes last.
    println!("\nAdding parent doc (after {} children)", child_docs.len());
    child_docs.push(parent_doc);

    println!("EXIT parse_nested_inner(): returning {} total docs", child_docs.len());
    Ok(child_docs)
}

/// Helper: parse an entire object into a single doc (non-nested parse).
fn parse_into_doc(
    schema: &Schema,
    val: &serde_json::Value,
    doc: &mut TantivyDocument,
) -> Result<(), DocParsingError> {
    println!("\nENTER parse_into_doc()");
    println!("Input value: {}", val);
    
    if let Some(m) = val.as_object() {
        println!("Processing object with {} fields", m.len());
        for (k, v) in m {
            println!("Processing field '{}' with value: {}", k, v);
            let field_opt = schema.get_field(k).ok();
            if let Some(field) = field_opt {
                println!("Found schema field id={}", field.field_id());
                parse_single_field(schema, field, v, doc)?;
            } else {
                println!("WARNING: Unknown field '{}' - skipping", k);
                // unknown field => skip or handle
            }
        }
    } else {
        println!("WARNING: Input was not a JSON object: {}", val);
    }
    
    println!("EXIT parse_into_doc()");
    Ok(())
}

/// Parse a single field's JSON value and add it to `doc`.
/// This calls the normal `TantivyDocument::parse_json_field(...)`.
fn parse_single_field(
    schema: &Schema,
    field: Field,
    json_val: &serde_json::Value,
    doc: &mut TantivyDocument,
) -> Result<(), DocParsingError> {
    println!("\nENTER parse_single_field()");
    println!("Field id={}, value={}", field.field_id(), json_val);
    
    // If you have a standard parse function (like `parse_json_field` in Tantivy),
    // call it here. We'll do a naive approach:
    match doc.parse_and_add_json(field, json_val, schema) {
        Ok(_) => {
            println!("Successfully parsed and added field");
            println!("EXIT parse_single_field()");
            Ok(())
        }
        Err(e) => {
            println!("ERROR parsing field: {:?}", e);
            println!("EXIT parse_single_field() with error");
            Err(e)
        }
    }
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
    where
        D: DocumentDeserializer<'de>,
    {
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
#[repr(packed)]
/// The value type and the address to its payload in the container.
struct ValueAddr {
    type_id: ValueType,
    /// This is the address to the value in the vec, except for bool and null, which are inlined
    val_addr: Addr,
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
}

impl BinarySerializable for ValueType {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        (*self as u8).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let num = u8::deserialize(reader)?;
        let type_id = if (0..=12).contains(&num) {
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

impl<'a> Iterator for CompactDocObjectIter<'a> {
    type Item = (&'a str, CompactDocValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.node_addresses_slice.is_empty() {
            return None;
        }
        let key_addr = ValueAddr::deserialize(&mut self.node_addresses_slice).ok()?;
        let key = self.container.extract_str(key_addr.val_addr);
        let value = ValueAddr::deserialize(&mut self.node_addresses_slice).ok()?;
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

    fn next(&mut self) -> Option<Self::Item> {
        if self.node_addresses_slice.is_empty() {
            return None;
        }
        let value = ValueAddr::deserialize(&mut self.node_addresses_slice).ok()?;
        let value = CompactDocValue {
            container: self.container,
            value_addr: value,
        };
        Some(value)
    }
}

impl Document for CompactDoc {
    type Value<'a> = CompactDocValue<'a>;
    type FieldsValuesIter<'a> = FieldValueIterRef<'a>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldValueIterRef {
            slice: self.field_values.iter(),
            container: self,
        }
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

#[cfg(test)]
mod nested_tests {
    use super::*;
    use crate::schema::{
        nested_options::NestedOptions, Schema, TantivyDocument, TextOptions, STRING,
    };

    #[test]
    fn test_nested_expansion() -> Result<(), DocParsingError> {
        // 1) define a schema with normal fields + a nested field
        let mut builder = Schema::builder();
        let doc_type_field = builder.add_text_field("docType", STRING);

        let nested_opts = NestedOptions::new().set_include_in_parent(true);
        let user_nested_field = builder.add_nested_field("user", nested_opts);

        // Also define the fields "first", "last", "group"
        let first_field = builder.add_text_field("first", STRING);
        let last_field = builder.add_text_field("last", STRING);
        let group_field = builder.add_text_field("group", STRING);

        let schema = builder.build();

        // 2) Our input JSON
        let input = r#"
        {
           "group": "fans",
           "user": [
              { "first": "John", "last": "Smith" },
              { "first": "Alice", "last": "White" }
           ]
        }
        "#;

        // 3) parse
        let docs = TantivyDocument::parse_json_for_nested(&schema, input)?;
        assert_eq!(docs.len(), 3, "Should produce 2 child docs + 1 parent doc");

        // child doc #1
        let child1 = &docs[0];
        let dt_vals = child1.get_all(doc_type_field).collect::<Vec<_>>();
        // we have not set docType = child in the snippet above, but you can do so if you prefer.
        // For now, we expect docType is absent:
        assert_eq!(dt_vals.len(), 0, "No docType in children by default?");

        let first_vals = child1.get_all(first_field).collect::<Vec<_>>();
        assert_eq!(first_vals[0].as_str(), Some("John"));
        let last_vals = child1.get_all(last_field).collect::<Vec<_>>();
        assert_eq!(last_vals[0].as_str(), Some("Smith"));

        // child doc #2
        let child2 = &docs[1];
        let afirst = child2.get_all(first_field).collect::<Vec<_>>();
        let alast = child2.get_all(last_field).collect::<Vec<_>>();
        assert_eq!(afirst[0].as_str(), Some("Alice"));
        assert_eq!(alast[0].as_str(), Some("White"));

        // parent doc #3
        let parent = &docs[2];
        // Because include_in_parent=true, the parent's doc also has "first" & "last" from the nested child docs
        let pf = parent
            .get_all(first_field)
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(pf, vec!["John", "Alice"]);
        let pl = parent
            .get_all(last_field)
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(pl, vec!["Smith", "White"]);

        // The parent also has group="fans"
        let groupv = parent.get_first(group_field).unwrap().as_str().unwrap();
        assert_eq!(groupv, "fans");

        Ok(())
    }

    fn test_default_document_parse_nested() -> Result<(), DocParsingError> {
        let mut builder = Schema::builder();
        let user_opts = NestedOptions::new().set_include_in_parent(true);
        let nested_field = builder.add_nested_field("user", user_opts);
        let normal_field = builder.add_text_field("title", STRING);
        let schema = builder.build();

        // Provide an example JSON with user as array-of-objects:
        let json_str = r#"{
            "title": "My doc",
            "user": [
              { "name": "John" },
              { "name": "Alice" }
            ]
        }"#;

        let docs = TantivyDocument::parse_json_for_nested(&schema, json_str)?;
        // We expect 2 child docs + 1 parent doc
        assert_eq!(docs.len(), 3);

        let child0 = &docs[0];
        // There's no "title" => let's confirm
        assert_eq!(child0.get_first(normal_field), None);
        // If you want, confirm the name "John" is present
        //   => but we haven't added that field to the schema, so let's skip.

        let parent = &docs[2];
        let title_val = parent.get_first(normal_field).unwrap().as_str().unwrap();
        assert_eq!(title_val, "My doc");

        // Now test the single object for a nested field => we skip it per the logic
        // in parse_json_for_nested:
        let single_obj_json = r#"{
            "user": { "name": "just one object" }
        }"#;
        let docs2 = TantivyDocument::parse_json_for_nested(&schema, single_obj_json)?;
        // It won't error or panic. By default we "skip or handle error".
        // We'll get 1 doc => the parent
        assert_eq!(docs2.len(), 1);
        Ok(())
    }

    #[test]
    fn test_default_document_parse_one_field_nested_err() {
        let mut builder = Schema::builder();
        let nested_opts = NestedOptions::new().set_include_in_parent(true);
        let nested_field = builder.add_nested_field("my_nested", nested_opts);
        let schema = builder.build();

        let val = serde_json::json!({"key":"value"});
        let mut doc = TantivyDocument::default();
        let result = crate::schema::document::default_document::parse_single_field(
            &schema,
            nested_field,
            &val,
            &mut doc,
        );
        match result {
            Err(DocParsingError::ValueError(
                fieldname,
                ValueParsingError::TypeError { expected, .. },
            )) => {
                assert_eq!(fieldname, "my_nested");
                assert_eq!(expected, "array-of-objects for nested fields");
            }
            other => panic!("Expected ValueError for nested fields, got: {:?}", other),
        }
    }

    fn test_default_document_parse_and_add_json_nested_err() {
        let mut builder = Schema::builder();
        let nested_opts = NestedOptions::new().set_include_in_parent(true);
        let nested_field = builder.add_nested_field("my_nested", nested_opts);
        let schema = builder.build();

        let val = serde_json::json!({"k":"v"});
        let mut doc = TantivyDocument::default();
        let result = doc.parse_and_add_json(nested_field, &val, &schema);
        match result {
            Err(DocParsingError::ValueError(
                fieldname,
                ValueParsingError::TypeError { expected, .. },
            )) => {
                assert_eq!(fieldname, "my_nested");
                assert!(expected.contains("array-of-objects for nested fields"));
            }
            other => panic!("Expected ValueError for nested fields, got: {:?}", other),
        }
    }

    fn test_field_type_nested_value_from_json() {
        let nested_opts = NestedOptions::new().set_include_in_parent(true);
        let ft = FieldType::Nested(nested_opts);
        let val = serde_json::json!({"test":123});
        let res = ft.value_from_json(val.clone());
        match res {
            Err(ValueParsingError::TypeError { expected, json }) => {
                assert_eq!(expected, "nested field must be expanded via custom logic");
                assert_eq!(json, val);
            }
            other => panic!("Expected TypeError for nested field, got: {:?}", other),
        }
    }
}
