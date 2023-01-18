use std::collections::HashMap;
use std::io;

use columnar::{ColumnarWriter, NumericalType, NumericalValue};
use common;
use fastfield_codecs::{Column, MonotonicallyMappableToU128, MonotonicallyMappableToU64};
use rustc_hash::FxHashMap;
use tantivy_bitpacker::BlockedBitpacker;

use super::FastFieldType;
use crate::fastfield::CompositeFastFieldSerializer;
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::UnorderedTermId;
use crate::schema::{Document, Field, FieldEntry, FieldType, Schema, Value};
use crate::termdict::TermOrdinal;
use crate::{DatePrecision, DocId};

/// The `FastFieldsWriter` groups all of the fast field writers.
pub struct FastFieldsWriter {
    columnar_writer: ColumnarWriter,
    fast_fields: Vec<Option<String>>, //< TODO see if we can cash the field name hash too.
    num_docs: DocId,
    // term_id_writers: Vec<MultiValuedFastFieldWriter>,
    // single_value_writers: Vec<IntFastFieldWriter>,
    // u128_value_writers: Vec<U128FastFieldWriter>,
    // u128_multi_value_writers: Vec<MultiValueU128FastFieldWriter>,
    // multi_values_writers: Vec<MultiValuedFastFieldWriter>,
    // bytes_value_writers: Vec<BytesFastFieldWriter>,
}

pub(crate) fn unexpected_value(expected: &str, actual: &Value) -> crate::TantivyError {
    crate::TantivyError::SchemaError(format!(
        "Expected a {:?} in fast field, but got {:?}",
        expected, actual
    ))
}

fn fast_field_default_value(field_entry: &FieldEntry) -> u64 {
    match *field_entry.field_type() {
        FieldType::I64(_) | FieldType::Date(_) => common::i64_to_u64(0i64),
        FieldType::F64(_) => common::f64_to_u64(0.0f64),
        _ => 0u64,
    }
}

enum FastFieldTyp {
    Numerical(NumericalType),
    Other,
}

fn fast_numerical_type(field_type: &FieldType) -> Option<FastFieldTyp> {
    // TODO
    match field_type {
        FieldType::U64(numerical_option) => {
            if numerical_option.is_fast() {
                Some(FastFieldTyp::Numerical(NumericalType::U64))
            } else {
                None
            }
        }
        FieldType::I64(numerical_option) => {
            if numerical_option.is_fast() {
                Some(FastFieldTyp::Numerical(NumericalType::I64))
            } else {
                None
            }
        }
        FieldType::F64(numerical_option) => {
            if numerical_option.is_fast() {
                Some(FastFieldTyp::Numerical(NumericalType::F64))
            } else {
                None
            }
        }
        FieldType::Str(str_option) => {
            if str_option.is_fast() {
                Some(FastFieldTyp::Other)
            } else {
                None
            }
        }
        FieldType::Bool(int_options) => {
            if int_options.is_fast() {
                Some(FastFieldTyp::Other)
            } else {
                None
            }
        }
        FieldType::Date(date_options) => {
            if date_options.is_fast() {
                Some(FastFieldTyp::Other)
            } else {
                None
            }
        }
        FieldType::Facet(_) => todo!(),
        FieldType::Bytes(_) => todo!(),
        FieldType::JsonObject(_) => todo!(),
        FieldType::IpAddr(_) => todo!(),
    }
}

impl FastFieldsWriter {
    /// Create all `FastFieldWriter` required by the schema.
    pub fn from_schema(schema: &Schema) -> FastFieldsWriter {
        let mut columnar_writer = ColumnarWriter::default();
        let mut fast_fields = vec![None; schema.num_fields()];
        // TODO see other types
        for (field, field_entry) in schema.fields() {
            if let Some(fast_field_typ) = fast_numerical_type(field_entry.field_type()) {
                match fast_field_typ {
                    FastFieldTyp::Numerical(numerical_type) => {
                        columnar_writer.force_numerical_type(field_entry.name(), numerical_type);
                    }
                    FastFieldTyp::Other => {}
                }
                fast_fields[field.field_id() as usize] = Some(field_entry.name().to_string());
            }
        }
        FastFieldsWriter {
            columnar_writer,
            fast_fields,
            num_docs: 0u32,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.columnar_writer.mem_usage()
    }

    /// Indexes all of the fastfields of a new document.
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        let doc_id = self.num_docs;
        for field_value in doc.field_values() {
            if let Some(field_name) =
                self.fast_fields[field_value.field().field_id() as usize].as_ref()
            {
                match &field_value.value {
                    Value::U64(u64_val) => {
                        self.columnar_writer.record_numerical(
                            doc_id,
                            field_name.as_str(),
                            NumericalValue::from(*u64_val),
                        );
                    }
                    Value::I64(i64_val) => {
                        self.columnar_writer.record_numerical(
                            doc_id,
                            field_name.as_str(),
                            NumericalValue::from(*i64_val),
                        );
                    }
                    Value::F64(f64_val) => {
                        self.columnar_writer.record_numerical(
                            doc_id,
                            field_name.as_str(),
                            NumericalValue::from(*f64_val),
                        );
                    }
                    Value::Str(_) => todo!(),
                    Value::PreTokStr(_) => todo!(),
                    Value::Bool(_) => todo!(),
                    Value::Date(_) => todo!(),
                    Value::Facet(_) => todo!(),
                    Value::Bytes(_) => todo!(),
                    Value::JsonObject(_) => todo!(),
                    Value::IpAddr(_) => todo!(),
                }
            }
        }
        self.num_docs += 1;
        Ok(())
    }

    /// Serializes all of the `FastFieldWriter`s by pushing them in
    /// order to the fast field serializer.
    pub fn serialize(
        mut self,
        wrt: &mut dyn io::Write,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        assert!(doc_id_map.is_none()); // TODO handle doc id map
        let num_docs = self.num_docs;
        self.columnar_writer.serialize(num_docs, wrt)?;
        Ok(())
    }
}

/// Fast field writer for u128 values.
/// The fast field writer just keeps the values in memory.
///
/// Only when the segment writer can be closed and
/// persisted on disk, the fast field writer is
/// sent to a `FastFieldSerializer` via the `.serialize(...)`
/// method.
///
/// We cannot serialize earlier as the values are
/// compressed to a compact number space and the number of
/// bits required for bitpacking can only been known once
/// we have seen all of the values.
pub struct U128FastFieldWriter {
    field: Field,
    vals: Vec<u128>,
    val_count: u32,
}

impl U128FastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field) -> Self {
        Self {
            field,
            vals: vec![],
            val_count: 0,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.vals.len() * 16
    }

    /// Records a new value.
    ///
    /// The n-th value being recorded is implicitely
    /// associated to the document with the `DocId` n.
    /// (Well, `n-1` actually because of 0-indexing)
    pub fn add_val(&mut self, val: u128) {
        self.vals.push(val);
    }

    /// Extract the fast field value from the document
    /// (or use the default value) and records it.
    ///
    /// Extract the value associated to the fast field for
    /// this document.
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        match doc.get_first(self.field) {
            Some(v) => {
                let ip_addr = v.as_ip_addr().ok_or_else(|| unexpected_value("ip", v))?;
                let value = ip_addr.to_u128();
                self.add_val(value);
            }
            None => {
                self.add_val(0); // TODO fix null handling
            }
        };
        self.val_count += 1;
        Ok(())
    }

    /// Push the fast fields value to the `FastFieldWriter`.
    pub fn serialize(
        &self,
        serializer: &mut CompositeFastFieldSerializer,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        if let Some(doc_id_map) = doc_id_map {
            let iter_gen = || {
                doc_id_map
                    .iter_old_doc_ids()
                    .map(|idx| self.vals[idx as usize])
            };

            serializer.create_u128_fast_field_with_idx(self.field, iter_gen, self.val_count, 0)?;
        } else {
            let iter_gen = || self.vals.iter().cloned();
            serializer.create_u128_fast_field_with_idx(self.field, iter_gen, self.val_count, 0)?;
        }

        Ok(())
    }
}

/// Fast field writer for ints.
/// The fast field writer just keeps the values in memory.
///
/// Only when the segment writer can be closed and
/// persisted on disk, the fast field writer is
/// sent to a `FastFieldSerializer` via the `.serialize(...)`
/// method.
///
/// We cannot serialize earlier as the values are
/// bitpacked and the number of bits required for bitpacking
/// can only been known once we have seen all of the values.
///
/// Both u64, i64 and f64 use the same writer.
/// i64 and f64 are just remapped to the `0..2^64 - 1`
/// using `common::i64_to_u64` and `common::f64_to_u64`.
pub struct IntFastFieldWriter {
    field: Field,
    precision_opt: Option<DatePrecision>,
    vals: BlockedBitpacker,
    val_count: usize,
    val_if_missing: u64,
    val_min: u64,
    val_max: u64,
}

impl IntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field, precision_opt: Option<DatePrecision>) -> IntFastFieldWriter {
        IntFastFieldWriter {
            field,
            precision_opt,
            vals: BlockedBitpacker::new(),
            val_count: 0,
            val_if_missing: 0u64,
            val_min: u64::MAX,
            val_max: 0,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.vals.mem_usage()
    }

    /// Returns the field that this writer is targeting.
    pub fn field(&self) -> Field {
        self.field
    }

    /// Sets the default value.
    ///
    /// This default value is recorded for documents if
    /// a document does not have any value.
    fn set_val_if_missing(&mut self, val_if_missing: u64) {
        self.val_if_missing = val_if_missing;
    }

    /// Records a new value.
    ///
    /// The n-th value being recorded is implicitly
    /// associated with the document with the `DocId` n.
    /// (Well, `n-1` actually because of 0-indexing)
    pub fn add_val(&mut self, val: u64) {
        self.vals.add(val);

        if val > self.val_max {
            self.val_max = val;
        }
        if val < self.val_min {
            self.val_min = val;
        }

        self.val_count += 1;
    }

    /// Extract the fast field value from the document
    /// (or use the default value) and records it.
    ///
    ///
    /// Extract the value associated with the fast field for
    /// this document.
    ///
    /// i64 and f64 are remapped to u64 using the logic
    /// in `common::i64_to_u64` and `common::f64_to_u64`.
    ///
    /// If the value is missing, then the default value is used
    /// instead.
    /// If the document has more than one value for the given field,
    /// only the first one is taken in account.
    ///
    /// Values on text fast fields are skipped.
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        match doc.get_first(self.field) {
            Some(v) => {
                let value = match (self.precision_opt, v) {
                    (Some(precision), Value::Date(date_val)) => {
                        date_val.truncate(precision).to_u64()
                    }
                    _ => super::value_to_u64(v)?,
                };
                self.add_val(value);
            }
            None => {
                self.add_val(self.val_if_missing);
            }
        };
        Ok(())
    }

    /// get iterator over the data
    pub(crate) fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.vals.iter()
    }

    /// Push the fast fields value to the `FastFieldWriter`.
    pub fn serialize(
        &self,
        serializer: &mut CompositeFastFieldSerializer,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        let (min, max) = if self.val_min > self.val_max {
            (0, 0)
        } else {
            (self.val_min, self.val_max)
        };

        let fastfield_accessor = WriterFastFieldAccessProvider {
            doc_id_map,
            vals: &self.vals,
            min_value: min,
            max_value: max,
            num_vals: self.val_count as u32,
        };

        serializer.create_auto_detect_u64_fast_field(self.field, fastfield_accessor)?;

        Ok(())
    }
}

#[derive(Clone)]
struct WriterFastFieldAccessProvider<'map, 'bitp> {
    doc_id_map: Option<&'map DocIdMapping>,
    vals: &'bitp BlockedBitpacker,
    min_value: u64,
    max_value: u64,
    num_vals: u32,
}

impl<'map, 'bitp> Column for WriterFastFieldAccessProvider<'map, 'bitp> {
    /// Return the value associated with the given doc.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance
    /// reasons.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the index.
    fn get_val(&self, _doc: u32) -> u64 {
        unimplemented!()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        if let Some(doc_id_map) = self.doc_id_map {
            Box::new(
                doc_id_map
                    .iter_old_doc_ids()
                    .map(|doc_id| self.vals.get(doc_id as usize)),
            )
        } else {
            Box::new(self.vals.iter())
        }
    }

    fn min_value(&self) -> u64 {
        self.min_value
    }

    fn max_value(&self) -> u64 {
        self.max_value
    }

    fn num_vals(&self) -> u32 {
        self.num_vals
    }
}
