use schema::{Schema, U32Field, Document};
use fastfield::FastFieldSerializer;
use std::io;

pub struct U32FastFieldsWriter {
    field_writers: Vec<U32FastFieldWriter>,
}

impl U32FastFieldsWriter {

    pub fn from_schema(schema: &Schema) -> U32FastFieldsWriter {
        let u32_fields: Vec<U32Field> = schema.get_u32_fields()
            .iter()
            .enumerate()
            .filter(|&(_, field_entry)| field_entry.option.is_fast())
            .map(|(field_id, _)| U32Field(field_id as u8))
            .collect();
        U32FastFieldsWriter::new(u32_fields)
    }

    pub fn new(fields: Vec<U32Field>) -> U32FastFieldsWriter {
        U32FastFieldsWriter {
            field_writers: fields
                .iter()
                .map(|field| U32FastFieldWriter::new(&field))
                .collect(),
        }
    }

    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in self.field_writers.iter_mut() {
            field_writer.add_document(doc);
        }
    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        for field_writer in self.field_writers.iter() {
            try!(field_writer.serialize(serializer));
        }
        Ok(())
    }
}

pub struct U32FastFieldWriter {
    field: U32Field,
    vals: Vec<u32>,
}

impl U32FastFieldWriter {
    pub fn new(field: &U32Field) -> U32FastFieldWriter {
        U32FastFieldWriter {
            field: field.clone(),
            vals: Vec::new(),
        }
    }

    pub fn add_val(&mut self, val: u32) {
        self.vals.push(val);
    }

    pub fn add_document(&mut self, doc: &Document) {
        let val = doc.get_u32(&self.field).unwrap_or(0u32);
        self.add_val(val);
    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        let zero = 0;
        let min = self.vals.iter().min().unwrap_or(&zero).clone();
        let max = self.vals.iter().max().unwrap_or(&min).clone();
        try!(serializer.new_u32_fast_field(self.field.clone(), min, max));
        for val in self.vals.iter() {
            try!(serializer.add_val(val.clone()));
        }
        serializer.close_field()
    }
}
