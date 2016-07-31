use schema::{Schema, FieldValue, Field, Document};
use fastfield::FastFieldSerializer;
use std::io;
use DocId;

pub struct U32FastFieldsWriter {
    field_writers: Vec<U32FastFieldWriter>,
}

impl U32FastFieldsWriter {

    pub fn from_schema(schema: &Schema) -> U32FastFieldsWriter {
        let u32_fields: Vec<Field> = schema.fields()
            .iter()
            .enumerate()
            .filter(|&(_, field_entry)| field_entry.is_u32_fast()) 
            .map(|(field_id, _)| Field(field_id as u8))
            .collect();
        U32FastFieldsWriter::new(u32_fields)
    }

    pub fn new(fields: Vec<Field>) -> U32FastFieldsWriter {
        U32FastFieldsWriter {
            field_writers: fields
                .into_iter()
                .map(U32FastFieldWriter::new)
                .collect(),
        }
    }
    
    pub fn get_field_writer(&mut self, field: Field) -> Option<&mut U32FastFieldWriter> {
        self.field_writers 
            .iter_mut()
            .filter(|field_writer| field_writer.field == field)
            .next()
    }
    
    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in self.field_writers.iter_mut() {
            field_writer.add_document(doc);
        }
    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        for field_writer in &self.field_writers {
            try!(field_writer.serialize(serializer));
        }
        Ok(())
    }
}

pub struct U32FastFieldWriter {
    field: Field,
    vals: Vec<u32>,
    cur_doc: DocId,
}

impl U32FastFieldWriter {
    pub fn new(field: Field) -> U32FastFieldWriter {
        U32FastFieldWriter {
            field: field.clone(),
            vals: Vec::new(),
            cur_doc: 0,
        }
    }

    pub fn set_val(&mut self, doc: DocId, val: u32) {
        for _ in self.cur_doc .. doc {
            self.vals.push(0u32);
        }
        self.cur_doc = doc;
        self.add_val(val);
    }
    
    pub fn add_val(&mut self, val: u32) {
        self.vals.push(val);
        self.cur_doc += 1;
    }
    
    fn extract_val(&self, doc: &Document) -> u32 {
        match doc.get_first(self.field) {
            Some(field_value) => {
                match field_value {
                    &FieldValue::U32(_, val) => { return val; }
                    _ => { panic!("Expected a u32field, got {:?} ", field_value) }
                }
            },
            None => {
                // TODO make default value configurable
                return 0u32;
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
        try!(serializer.new_u32_fast_field(self.field, min, max));
        for &val in &self.vals {
            try!(serializer.add_val(val));
        }
        serializer.close_field()
    }
}
