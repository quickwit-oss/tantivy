use schema::{Schema, Field, Document};
use fastfield::FastFieldSerializer;
use std::io;
use schema::Value;
use DocId;
use schema::FieldType;
use common;
use common::VInt;
use common::BinarySerializable;

/// The fastfieldswriter regroup all of the fast field writers.
pub struct FastFieldsWriter {
    field_writers: Vec<IntFastFieldWriter>,
}

impl FastFieldsWriter {
    /// Create all `FastFieldWriter` required by the schema.
    pub fn from_schema(schema: &Schema) -> FastFieldsWriter {
        let field_writers: Vec<IntFastFieldWriter> = schema
            .fields()
            .iter()
            .enumerate()
            .flat_map(|(field_id, field_entry)| {
                let field = Field(field_id as u32);
                match *field_entry.field_type() {
                    FieldType::I64(ref int_options) => {
                        if int_options.is_fast() {
                            let mut fast_field_writer = IntFastFieldWriter::new(field);
                            fast_field_writer.set_val_if_missing(common::i64_to_u64(0i64));
                            Some(fast_field_writer)
                        } else {
                            None
                        }
                    }
                    FieldType::U64(ref int_options) => {
                        if int_options.is_fast() {
                            Some(IntFastFieldWriter::new(field))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .collect();
        FastFieldsWriter { field_writers: field_writers }
    }

    /// Returns a `FastFieldsWriter`
    /// with a `IntFastFieldWriter` for each
    /// of the field given in argument.
    pub fn new(fields: Vec<Field>) -> FastFieldsWriter {
        FastFieldsWriter {
            field_writers: fields.into_iter().map(IntFastFieldWriter::new).collect(),
        }
    }

    /// Get the `FastFieldWriter` associated to a field.
    pub fn get_field_writer(&mut self, field: Field) -> Option<&mut IntFastFieldWriter> {
        // TODO optimize
        self.field_writers
            .iter_mut()
            .find(|field_writer| field_writer.field == field)
    }


    /// Indexes all of the fastfields of a new document.
    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in &mut self.field_writers {
            field_writer.add_document(doc);
        }
    }

    /// Serializes all of the `FastFieldWriter`s by pushing them in
    /// order to the fast field serializer.
    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        for field_writer in &self.field_writers {
            field_writer.serialize(serializer)?;
        }
        Ok(())
    }

    /// Ensures all of the fast field writers have
    /// reached `doc`. (included)
    ///
    /// The missing values will be filled with 0.
    pub fn fill_val_up_to(&mut self, doc: DocId) {
        for field_writer in &mut self.field_writers {
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
            Some(v) => {
                match *v {
                    Value::U64(ref val) => *val,
                    Value::I64(ref val) => common::i64_to_u64(*val),
                    _ => panic!("Expected a u64field, got {:?} ", v),
                }
            }
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

        serializer.new_u64_fast_field(self.field, min, max)?;

        let mut cursor = self.vals.as_slice();
        while let Ok(VInt(val)) = VInt::deserialize(&mut cursor) {
            serializer.add_val(val)?;
        }

        serializer.close_field()
    }
}
