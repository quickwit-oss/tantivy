#![allow(unused_imports)]
#![allow(dead_code)]
use std::alloc::System;
use std::env::args;
use std::net::Ipv6Addr;

use columnar::{MonotonicallyMappableToU128, MonotonicallyMappableToU64};
use common::{BinarySerializable, CountingWriter, DateTime, FixedSize};
use peakmem_alloc::*;
use tantivy::schema::{Field, FieldValue, OwnedValue, FAST, INDEXED, STRING, TEXT};
use tantivy::tokenizer::PreTokenizedString;
use tantivy::{doc, TantivyDocument};

const GH_LOGS: &str = include_str!("../benches/gh.json");
const HDFS_LOGS: &str = include_str!("../benches/hdfs.json");

#[global_allocator]
static GLOBAL: &PeakMemAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    dbg!(std::mem::size_of::<TantivyDocument>());
    dbg!(std::mem::size_of::<DocContainerRef>());
    dbg!(std::mem::size_of::<OwnedValue>());
    dbg!(std::mem::size_of::<OwnedValueMedVec>());
    dbg!(std::mem::size_of::<ValueContainerRef>());
    dbg!(std::mem::size_of::<mediumvec::vec32::Vec32::<u8>>());

    let filter = args().nth(1);
    measure_fn(
        test_hdfs::<TantivyDocument>,
        "hdfs TantivyDocument",
        &filter,
    );
    measure_fn(
        test_hdfs::<TantivyDocumentMedVec>,
        "hdfs TantivyDocumentMedVec",
        &filter,
    );
    measure_fn(
        test_hdfs::<DocContainerRef>,
        "hdfs DocContainerRef",
        &filter,
    );
    measure_fn(test_gh::<TantivyDocument>, "gh TantivyDocument", &filter);
    measure_fn(
        test_gh::<TantivyDocumentMedVec>,
        "gh TantivyDocumentMedVec",
        &filter,
    );
    measure_fn(test_gh::<DocContainerRef>, "gh DocContainerRef", &filter);
}
fn measure_fn<F: FnOnce()>(f: F, name: &str, filter: &Option<std::string::String>) {
    if let Some(filter) = filter {
        if !name.contains(filter) {
            return;
        }
    }
    GLOBAL.reset_peak_memory();
    f();
    println!("Peak Memory {} : {:#?}", GLOBAL.get_peak_memory(), name);
}
fn test_hdfs<T: From<TantivyDocument>>() {
    let schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_u64_field("timestamp", INDEXED);
        schema_builder.add_text_field("body", TEXT);
        schema_builder.add_text_field("severity", STRING);
        schema_builder.build()
    };
    let mut docs: Vec<T> = Vec::with_capacity(HDFS_LOGS.lines().count());
    for doc_json in HDFS_LOGS.lines() {
        let doc = TantivyDocument::parse_json(&schema, doc_json)
            .unwrap()
            .into();
        docs.push(doc);
    }
}

fn test_gh<T: From<TantivyDocument>>() {
    let schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", FAST);
        schema_builder.build()
    };
    let mut docs: Vec<T> = Vec::with_capacity(GH_LOGS.lines().count());
    for doc_json in GH_LOGS.lines() {
        let json_field = schema.get_field("json").unwrap();

        let json_val: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(doc_json).unwrap();
        let doc = tantivy::doc!(json_field=>json_val).into();
        docs.push(doc);
    }
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct TantivyDocumentMedVec {
    field_values: mediumvec::Vec32<FieldValueMedVec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldValueMedVec {
    pub field: Field,
    pub value: OwnedValueMedVec,
}

/// This is a owned variant of `Value`, that can be passed around without lifetimes.
/// Represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValueMedVec {
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(mediumvec::vec32::Vec32<u8>),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Bool value
    Bool(bool),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    Array(mediumvec::vec32::Vec32<Self>),
    /// Dynamic object value.
    Object(mediumvec::vec32::Vec32<(String, Self)>),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// Pre-tokenized str type,
    PreTokStr(Box<PreTokenizedString>),
    /// Arbitrarily sized byte array
    Bytes(mediumvec::vec32::Vec32<u8>),
}

impl From<TantivyDocument> for TantivyDocumentMedVec {
    fn from(doc: TantivyDocument) -> Self {
        let field_values = doc
            .into_iter()
            .map(|fv| FieldValueMedVec {
                field: fv.field,
                value: fv.value.into(),
            })
            .collect();
        TantivyDocumentMedVec { field_values }
    }
}
impl From<OwnedValue> for OwnedValueMedVec {
    fn from(value: OwnedValue) -> Self {
        match value {
            OwnedValue::Null => OwnedValueMedVec::Null,
            OwnedValue::Str(s) => {
                let bytes = s.into_bytes();
                let vec = mediumvec::vec32::Vec32::from_vec(bytes);
                OwnedValueMedVec::Str(vec)
            }
            OwnedValue::U64(u) => OwnedValueMedVec::U64(u),
            OwnedValue::I64(i) => OwnedValueMedVec::I64(i),
            OwnedValue::F64(f) => OwnedValueMedVec::F64(f),
            OwnedValue::Bool(b) => OwnedValueMedVec::Bool(b),
            OwnedValue::Date(d) => OwnedValueMedVec::Date(d),
            OwnedValue::Array(arr) => {
                let arr = arr.into_iter().map(|v| v.into()).collect();
                OwnedValueMedVec::Array(arr)
            }
            OwnedValue::Object(obj) => {
                let obj = obj.into_iter().map(|(k, v)| (k, v.into())).collect();
                OwnedValueMedVec::Object(obj)
            }
            OwnedValue::IpAddr(ip) => OwnedValueMedVec::IpAddr(ip),
            _ => panic!("Unsupported value type {:?}", value),
        }
    }
}

#[repr(packed)]
pub struct FieldValueContainerRef {
    pub field: u16,
    pub value: ValueContainerRef,
}

#[repr(packed)]
struct DocContainerRef {
    container: OwnedValueRefContainer,
    field_values: mediumvec::Vec32<FieldValueContainerRef>,
}

#[derive(Default)]
struct OwnedValueRefContainer {
    nodes: mediumvec::Vec32<ValueContainerRef>,
    node_data: mediumvec::Vec32<u8>,
}
impl OwnedValueRefContainer {
    fn shrink_to_fit(&mut self) {
        self.nodes.shrink_to_fit();
        self.node_data.shrink_to_fit();
    }
}

impl From<TantivyDocument> for DocContainerRef {
    fn from(doc: TantivyDocument) -> Self {
        let mut container = OwnedValueRefContainer::default();
        let field_values = doc
            .into_iter()
            .map(|fv| FieldValueContainerRef {
                field: fv.field.field_id().try_into().unwrap(),
                value: container.add_value(fv.value),
            })
            .collect();
        container.shrink_to_fit();
        Self {
            field_values,
            container,
        }
    }
}

// References to positions in two array, one for the OwnedValueRef and the other for the encoded
// bytes
#[derive(Debug, Clone, PartialEq)]
pub enum ValueContainerRef {
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(u32),
    /// Unsigned 64-bits Integer `u64`
    U64(u32), // position of the serialized 8 bytes in the data array
    /// Signed 64-bits Integer `i64`
    I64(u32), // position of the serialized 8 bytes in the data array
    /// 64-bits Float `f64`
    F64(u32), // position of the serialized 8 bytes in the data array
    /// Bool value
    Bool(bool), // inlined bool
    /// Date/time with nanoseconds precision
    Date(u32), // position of the serialized 8 byte in the data array
    Array(NodeAddress),
    /// Dynamic object value.
    Object(NodeAddress),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(u32), // position of the serialized 16 bytes in the data array
    /// Arbitrarily sized byte array
    Bytes(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeAddress {
    pos: u32,
    num_nodes: u32,
}

impl OwnedValueRefContainer {
    pub fn add_value(&mut self, value: OwnedValue) -> ValueContainerRef {
        match value {
            OwnedValue::Null => ValueContainerRef::Null,
            OwnedValue::U64(num) => ValueContainerRef::U64(write_into(&mut self.node_data, num)),
            OwnedValue::I64(num) => ValueContainerRef::I64(write_into(&mut self.node_data, num)),
            OwnedValue::F64(num) => ValueContainerRef::F64(write_into(&mut self.node_data, num)),
            OwnedValue::Bool(b) => ValueContainerRef::Bool(b),
            OwnedValue::Date(date) => ValueContainerRef::Date(write_into(
                &mut self.node_data,
                date.into_timestamp_nanos(),
            )),
            OwnedValue::Str(bytes) => {
                ValueContainerRef::Str(write_into(&mut self.node_data, bytes))
            }
            OwnedValue::Bytes(bytes) => {
                ValueContainerRef::Bytes(write_into(&mut self.node_data, bytes))
            }
            OwnedValue::Array(elements) => {
                let pos = self.nodes.len() as u32;
                let len = elements.len() as u32;
                for elem in elements {
                    let ref_elem = self.add_value(elem);
                    self.nodes.push(ref_elem);
                }
                ValueContainerRef::Array(NodeAddress {
                    pos,
                    num_nodes: len,
                })
            }
            OwnedValue::Object(entries) => {
                let pos = self.nodes.len() as u32;
                let len = entries.len() as u32;
                for (key, value) in entries {
                    let ref_key = self.add_value(OwnedValue::Str(key));
                    let ref_value = self.add_value(value);
                    self.nodes.push(ref_key);
                    self.nodes.push(ref_value);
                }
                ValueContainerRef::Object(NodeAddress {
                    pos,
                    num_nodes: len,
                })
            }
            OwnedValue::IpAddr(num) => {
                ValueContainerRef::IpAddr(write_into(&mut self.node_data, num.to_u128()))
            }
            OwnedValue::PreTokStr(_) => todo!(),
            OwnedValue::Facet(_) => todo!(),
        }
    }
}

fn write_into<T: BinarySerializable>(data: &mut mediumvec::Vec32<u8>, value: T) -> u32 {
    let pos = data.len() as u32;
    data.as_vec(|vec| value.serialize(vec).unwrap());
    pos
}

fn write_into_2<T: BinarySerializable>(data: &mut mediumvec::Vec32<u8>, value: T) -> NodeAddress {
    let pos = data.len() as u32;
    let mut len = 0;
    data.as_vec(|vec| {
        let mut wrt = CountingWriter::wrap(vec);
        value.serialize(&mut wrt).unwrap();
        len = wrt.written_bytes() as u32;
    });
    NodeAddress {
        pos,
        num_nodes: len,
    }
}

// impl From<ContainerDocRef> for TantivyDocument {
// fn from(doc: ContainerDocRef) -> Self {
// let mut doc2 = TantivyDocument::new();
// for fv in doc.field_values {
// let field = Field::from_field_id(fv.field as u32);
// let value = doc.container.get_value(fv.value);
// doc2.add(FieldValue::new(field, value));
//}
// doc2
//}
