use schema::{Schema, Field, Document};
use fastfield::FastFieldSerializer;
use std::io;
use schema::Value;
use DocId;
use common;
use schema::FieldType;

pub struct FastFieldsWriter {
    field_writers: Vec<IntFastFieldWriter>,
}

impl FastFieldsWriter {

    pub fn from_schema(schema: &Schema) -> FastFieldsWriter {
        let field_writers: Vec<IntFastFieldWriter> = schema
            .fields()
            .iter()
            .enumerate()
            .flat_map(|(field_id, field_entry)| {
                let field = Field(field_id as u32);
                match field_entry.field_type() {
                    &FieldType::I64(ref int_options) => {
                        if int_options.is_fast() {
                            let mut fast_field_writer = IntFastFieldWriter::new(field);
                            fast_field_writer.set_val_if_missing(common::i64_to_u64(0i64));
                            Some(fast_field_writer)
                        }
                        else {
                            None
                        }
                    }
                    &FieldType::U64(ref int_options) => {
                        if int_options.is_fast() {
                            Some(IntFastFieldWriter::new(field))
                        }
                        else {
                            None
                        }
                    }
                    _ => None
                }
            }) 
            .collect();
        FastFieldsWriter {
            field_writers: field_writers,
        }
    }

    pub fn new(fields: Vec<Field>) -> FastFieldsWriter {
        FastFieldsWriter {
            field_writers: fields
                .into_iter()
                .map(IntFastFieldWriter::new)
                .collect(),
        }
    }
    
    pub fn get_field_writer(&mut self, field: Field) -> Option<&mut IntFastFieldWriter> {
        // TODO optimize
        self.field_writers
            .iter_mut()
            .find(|field_writer| field_writer.field == field)
    }
    
    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in &mut self.field_writers {
            field_writer.add_document(doc);
        }
    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        for field_writer in &self.field_writers {
            try!(field_writer.serialize(serializer));
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


pub struct IntFastFieldWriter {
    field: Field,
    vals: Vec<u64>,
    val_if_missing: u64,
}

impl IntFastFieldWriter {
    pub fn new(field: Field) -> IntFastFieldWriter {
        IntFastFieldWriter {
            field: field,
            vals: Vec::new(),
            val_if_missing: 0u64,
        }
    }
    
    fn set_val_if_missing(&mut self, val_if_missing: u64) {
        self.val_if_missing = val_if_missing;
    }

    /// Ensures all of the fast field writer have
    /// reached `doc`. (included)
    /// 
    /// The missing values will be filled with 0.
    fn fill_val_up_to(&mut self, doc: DocId) {
        let target = doc as usize + 1;
        debug_assert!(self.vals.len() <= target);
        let val_if_missing = self.val_if_missing;
        while self.vals.len() < target {
            self.add_val(val_if_missing);
        }
    }
    
    pub fn add_val(&mut self, val: u64) {
        self.vals.push(val);
    }
    
    fn extract_val(&self, doc: &Document) -> u64 {
        match doc.get_first(self.field) {
            Some(v) => {
                match *v {
                    Value::U64(ref val) => { *val },
                    Value::I64(ref val) => common::i64_to_u64(*val),
                    _ => { panic!("Expected a u64field, got {:?} ", v) }
                }
            },
            None => {
                self.val_if_missing
            }            
        }
    }
    
    pub fn add_document(&mut self, doc: &Document) {
        let val = self.extract_val(doc);
        self.add_val(val);
    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        let zero = 0;
        let min = *self.vals.iter().min().unwrap_or(&zero);
        let max = *self.vals.iter().max().unwrap_or(&min);
        serializer.new_u64_fast_field(self.field, min, max)?;
        for &val in &self.vals {
            serializer.add_val(val)?;
        }
        serializer.close_field()
    }
}





