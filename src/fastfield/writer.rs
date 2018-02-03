use schema::{Cardinality, Document, Field, Schema};
use fastfield::FastFieldSerializer;
use std::io;
use schema::Value;
use DocId;
use schema::FieldType;
use common;
use common::VInt;
use std::collections::HashMap;
use postings::UnorderedTermId;
use super::multivalued::MultiValueIntFastFieldWriter;
use common::BinarySerializable;

/// The fastfieldswriter regroup all of the fast field writers.
pub struct FastFieldsWriter {
    single_value_writers: Vec<IntFastFieldWriter>,
    multi_values_writers: Vec<MultiValueIntFastFieldWriter>,
}

impl FastFieldsWriter {
    /// Create all `FastFieldWriter` required by the schema.
    pub fn from_schema(schema: &Schema) -> FastFieldsWriter {
        let mut single_value_writers = Vec::new();
        let mut multi_values_writers = Vec::new();

        for (field_id, field_entry) in schema.fields().iter().enumerate() {
            let field = Field(field_id as u32);
            let default_value = if let FieldType::I64(_) = *field_entry.field_type() {
                common::i64_to_u64(0i64)
            } else {
                0u64
            };
            match *field_entry.field_type() {
                FieldType::I64(ref int_options) | FieldType::U64(ref int_options) => {
                    match int_options.get_fastfield_cardinality() {
                        Some(Cardinality::SingleValue) => {
                            let mut fast_field_writer = IntFastFieldWriter::new(field);
                            fast_field_writer.set_val_if_missing(default_value);
                            single_value_writers.push(fast_field_writer);
                        }
                        Some(Cardinality::MultiValues) => {
                            let fast_field_writer = MultiValueIntFastFieldWriter::new(field);
                            multi_values_writers.push(fast_field_writer);
                        }
                        None => {}
                    }
                }
                FieldType::HierarchicalFacet => {
                    let fast_field_writer = MultiValueIntFastFieldWriter::new(field);
                    multi_values_writers.push(fast_field_writer);
                }
                _ => {}
            }
        }
        FastFieldsWriter {
            single_value_writers: single_value_writers,
            multi_values_writers: multi_values_writers,
        }
    }

    /// Returns a `FastFieldsWriter with a `u64` `IntFastFieldWriter` for each
    /// of the field given in argument.
    pub(crate) fn new(fields: Vec<Field>) -> FastFieldsWriter {
        FastFieldsWriter {
            single_value_writers: fields.into_iter().map(IntFastFieldWriter::new).collect(),
            multi_values_writers: vec![],
        }
    }

    /// Get the `FastFieldWriter` associated to a field.
    pub fn get_field_writer(&mut self, field: Field) -> Option<&mut IntFastFieldWriter> {
        // TODO optimize
        self.single_value_writers
            .iter_mut()
            .find(|field_writer| field_writer.field() == field)
    }

    /// Returns the fast field multi-value writer for the given field.
    ///
    /// Returns None if the field does not exist, or is not
    /// configured as a multivalued fastfield in the schema.
    pub(crate) fn get_multivalue_writer(
        &mut self,
        field: Field,
    ) -> Option<&mut MultiValueIntFastFieldWriter> {
        // TODO optimize
        // TODO expose for users
        self.multi_values_writers
            .iter_mut()
            .find(|multivalue_writer| multivalue_writer.field() == field)
    }

    /// Indexes all of the fastfields of a new document.
    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in &mut self.single_value_writers {
            field_writer.add_document(doc);
        }
        for field_writer in &mut self.multi_values_writers {
            field_writer.next_doc();
        }
    }

    /// Serializes all of the `FastFieldWriter`s by pushing them in
    /// order to the fast field serializer.
    pub fn serialize(
        &self,
        serializer: &mut FastFieldSerializer,
        mapping: &HashMap<Field, HashMap<UnorderedTermId, usize>>,
    ) -> io::Result<()> {
        for field_writer in &self.single_value_writers {
            field_writer.serialize(serializer)?;
        }
        for field_writer in &self.multi_values_writers {
            let field = field_writer.field();
            if let Some(mapping) = mapping.get(&field) {
                field_writer.serialize(serializer, mapping)?;
            } else {
                panic!("Term ordinal mapping missing for {:?}", field);
            }
        }
        Ok(())
    }

    /// Ensures all of the fast field writers have
    /// reached `doc`. (included)
    ///
    /// The missing values will be filled with 0.
    pub fn fill_val_up_to(&mut self, doc: DocId) {
        for field_writer in &mut self.single_value_writers {
            field_writer.fill_val_up_to(doc);
        }
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
/// Both u64, and i64 use the same writer.
/// i64 are just remapped to the `0..2^64 - 1`
/// using `common::i64_to_u64`.
pub struct IntFastFieldWriter {
    field: Field,
    vals: Vec<u8>,
    val_count: usize,
    val_if_missing: u64,
    val_min: u64,
    val_max: u64,
}

impl IntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field) -> IntFastFieldWriter {
        IntFastFieldWriter {
            field: field,
            vals: Vec::new(),
            val_count: 0,
            val_if_missing: 0u64,
            val_min: u64::max_value(),
            val_max: 0,
        }
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

    /// Ensures all of the fast field writer have
    /// reached `doc`. (included)
    ///
    /// The missing values will be filled with 0.
    fn fill_val_up_to(&mut self, doc: DocId) {
        let target = doc as usize + 1;
        debug_assert!(self.val_count <= target);
        let val_if_missing = self.val_if_missing;
        while self.val_count < target {
            self.add_val(val_if_missing);
        }
    }

    /// Records a new value.
    ///
    /// The n-th value being recorded is implicitely
    /// associated to the document with the `DocId` n.
    /// (Well, `n-1` actually because of 0-indexing)
    pub fn add_val(&mut self, val: u64) {
        VInt(val)
            .serialize(&mut self.vals)
            .expect("unable to serialize VInt to Vec");

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
    /// i64 are remapped to u64 using the logic
    /// in `common::i64_to_u64`.
    ///
    /// If the value is missing, then the default value is used
    /// instead.
    /// If the document has more than one value for the given field,
    /// only the first one is taken in account.
    fn extract_val(&self, doc: &Document) -> u64 {
        match doc.get_first(self.field) {
            Some(v) => match *v {
                Value::U64(ref val) => *val,
                Value::I64(ref val) => common::i64_to_u64(*val),
                _ => panic!("Expected a u64field, got {:?} ", v),
            },
            None => self.val_if_missing,
        }
    }

    /// Extract the fast field value from the document
    /// (or use the default value) and records it.
    pub fn add_document(&mut self, doc: &Document) {
        let val = self.extract_val(doc);
        self.add_val(val);
    }

    /// Push the fast fields value to the `FastFieldWriter`.
    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        let (min, max) = if self.val_min > self.val_max {
            (0, 0)
        } else {
            (self.val_min, self.val_max)
        };

        let mut single_field_serializer = serializer.new_u64_fast_field(self.field, min, max)?;

        let mut cursor = self.vals.as_slice();
        while let Ok(VInt(val)) = VInt::deserialize(&mut cursor) {
            single_field_serializer.add_val(val)?;
        }

        single_field_serializer.close_field()
    }
}
