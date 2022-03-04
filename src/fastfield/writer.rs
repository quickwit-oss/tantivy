use std::collections::HashMap;
use std::io;

use common;
use fnv::FnvHashMap;
use tantivy_bitpacker::BlockedBitpacker;

use super::multivalued::MultiValuedFastFieldWriter;
use super::serializer::FastFieldStats;
use super::FastFieldDataAccess;
use crate::fastfield::{BytesFastFieldWriter, CompositeFastFieldSerializer};
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::UnorderedTermId;
use crate::schema::{Cardinality, Document, Field, FieldEntry, FieldType, Schema};
use crate::termdict::TermOrdinal;

/// The `FastFieldsWriter` groups all of the fast field writers.
pub struct FastFieldsWriter {
    single_value_writers: Vec<IntFastFieldWriter>,
    multi_values_writers: Vec<MultiValuedFastFieldWriter>,
    bytes_value_writers: Vec<BytesFastFieldWriter>,
}

fn fast_field_default_value(field_entry: &FieldEntry) -> u64 {
    match *field_entry.field_type() {
        FieldType::I64(_) | FieldType::Date(_) => common::i64_to_u64(0i64),
        FieldType::F64(_) => common::f64_to_u64(0.0f64),
        _ => 0u64,
    }
}

impl FastFieldsWriter {
    /// Create all `FastFieldWriter` required by the schema.
    pub fn from_schema(schema: &Schema) -> FastFieldsWriter {
        let mut single_value_writers = Vec::new();
        let mut multi_values_writers = Vec::new();
        let mut bytes_value_writers = Vec::new();

        for (field, field_entry) in schema.fields() {
            match field_entry.field_type() {
                FieldType::I64(ref int_options)
                | FieldType::U64(ref int_options)
                | FieldType::F64(ref int_options)
                | FieldType::Date(ref int_options) => {
                    match int_options.get_fastfield_cardinality() {
                        Some(Cardinality::SingleValue) => {
                            let mut fast_field_writer = IntFastFieldWriter::new(field);
                            let default_value = fast_field_default_value(field_entry);
                            fast_field_writer.set_val_if_missing(default_value);
                            single_value_writers.push(fast_field_writer);
                        }
                        Some(Cardinality::MultiValues) => {
                            let fast_field_writer = MultiValuedFastFieldWriter::new(field, false);
                            multi_values_writers.push(fast_field_writer);
                        }
                        None => {}
                    }
                }
                FieldType::Facet(_) => {
                    let fast_field_writer = MultiValuedFastFieldWriter::new(field, true);
                    multi_values_writers.push(fast_field_writer);
                }
                FieldType::Bytes(bytes_option) => {
                    if bytes_option.is_fast() {
                        let fast_field_writer = BytesFastFieldWriter::new(field);
                        bytes_value_writers.push(fast_field_writer);
                    }
                }
                _ => {}
            }
        }
        FastFieldsWriter {
            single_value_writers,
            multi_values_writers,
            bytes_value_writers,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.single_value_writers
            .iter()
            .map(|w| w.mem_usage())
            .sum::<usize>()
            + self
                .multi_values_writers
                .iter()
                .map(|w| w.mem_usage())
                .sum::<usize>()
            + self
                .bytes_value_writers
                .iter()
                .map(|w| w.mem_usage())
                .sum::<usize>()
    }

    /// Get the `FastFieldWriter` associated to a field.
    pub fn get_field_writer(&self, field: Field) -> Option<&IntFastFieldWriter> {
        // TODO optimize
        self.single_value_writers
            .iter()
            .find(|field_writer| field_writer.field() == field)
    }

    /// Get the `FastFieldWriter` associated to a field.
    pub fn get_field_writer_mut(&mut self, field: Field) -> Option<&mut IntFastFieldWriter> {
        // TODO optimize
        self.single_value_writers
            .iter_mut()
            .find(|field_writer| field_writer.field() == field)
    }

    /// Returns the fast field multi-value writer for the given field.
    ///
    /// Returns None if the field does not exist, or is not
    /// configured as a multivalued fastfield in the schema.
    pub fn get_multivalue_writer_mut(
        &mut self,
        field: Field,
    ) -> Option<&mut MultiValuedFastFieldWriter> {
        // TODO optimize
        self.multi_values_writers
            .iter_mut()
            .find(|multivalue_writer| multivalue_writer.field() == field)
    }

    /// Returns the bytes fast field writer for the given field.
    ///
    /// Returns None if the field does not exist, or is not
    /// configured as a bytes fastfield in the schema.
    pub fn get_bytes_writer_mut(&mut self, field: Field) -> Option<&mut BytesFastFieldWriter> {
        // TODO optimize
        self.bytes_value_writers
            .iter_mut()
            .find(|field_writer| field_writer.field() == field)
    }

    /// Indexes all of the fastfields of a new document.
    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in &mut self.single_value_writers {
            field_writer.add_document(doc);
        }
        for field_writer in &mut self.multi_values_writers {
            field_writer.add_document(doc);
        }
        for field_writer in &mut self.bytes_value_writers {
            field_writer.add_document(doc);
        }
    }

    /// Serializes all of the `FastFieldWriter`s by pushing them in
    /// order to the fast field serializer.
    pub fn serialize(
        &self,
        serializer: &mut CompositeFastFieldSerializer,
        mapping: &HashMap<Field, FnvHashMap<UnorderedTermId, TermOrdinal>>,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        for field_writer in &self.single_value_writers {
            field_writer.serialize(serializer, doc_id_map)?;
        }

        for field_writer in &self.multi_values_writers {
            let field = field_writer.field();
            field_writer.serialize(serializer, mapping.get(&field), doc_id_map)?;
        }
        for field_writer in &self.bytes_value_writers {
            field_writer.serialize(serializer, doc_id_map)?;
        }
        Ok(())
    }
}

/// Fast field writer for ints.
/// The fast field writer just keeps the values in memory.
///
/// Only when the segment writer can be closed and
/// persisted on disc, the fast field writer is
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
    vals: BlockedBitpacker,
    val_count: usize,
    val_if_missing: u64,
    val_min: u64,
    val_max: u64,
}

impl IntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field) -> IntFastFieldWriter {
        IntFastFieldWriter {
            field,
            vals: BlockedBitpacker::new(),
            val_count: 0,
            val_if_missing: 0u64,
            val_min: u64::max_value(),
            val_max: 0,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.vals.mem_usage()
    }

    /// Returns the field that this writer is targetting.
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
    /// The n-th value being recorded is implicitely
    /// associated to the document with the `DocId` n.
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

    /// Extract the value associated to the fast field for
    /// this document.
    ///
    /// i64 and f64 are remapped to u64 using the logic
    /// in `common::i64_to_u64` and `common::f64_to_u64`.
    ///
    /// If the value is missing, then the default value is used
    /// instead.
    /// If the document has more than one value for the given field,
    /// only the first one is taken in account.
    fn extract_val(&self, doc: &Document) -> u64 {
        match doc.get_first(self.field) {
            Some(v) => super::value_to_u64(v),
            None => self.val_if_missing,
        }
    }

    /// Extract the fast field value from the document
    /// (or use the default value) and records it.
    pub fn add_document(&mut self, doc: &Document) {
        let val = self.extract_val(doc);
        self.add_val(val);
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
        };
        let stats = FastFieldStats {
            min_value: min,
            max_value: max,
            num_vals: self.val_count as u64,
        };

        if let Some(doc_id_map) = doc_id_map {
            let iter = doc_id_map
                .iter_old_doc_ids()
                .map(|doc_id| self.vals.get(doc_id as usize));
            serializer.create_auto_detect_u64_fast_field(
                self.field,
                stats,
                fastfield_accessor,
                iter.clone(),
                iter,
            )?;
        } else {
            serializer.create_auto_detect_u64_fast_field(
                self.field,
                stats,
                fastfield_accessor,
                self.vals.iter(),
                self.vals.iter(),
            )?;
        };
        Ok(())
    }
}

#[derive(Clone)]
struct WriterFastFieldAccessProvider<'map, 'bitp> {
    doc_id_map: Option<&'map DocIdMapping>,
    vals: &'bitp BlockedBitpacker,
}
impl<'map, 'bitp> FastFieldDataAccess for WriterFastFieldAccessProvider<'map, 'bitp> {
    /// Return the value associated to the given doc.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance
    /// reasons.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the index.
    fn get_val(&self, doc: u64) -> u64 {
        if let Some(doc_id_map) = self.doc_id_map {
            self.vals
                .get(doc_id_map.get_old_doc_id(doc as u32) as usize) // consider extra
                                                                     // FastFieldReader wrapper for
                                                                     // non doc_id_map
        } else {
            self.vals.get(doc as usize)
        }
    }
}
