use core::global::*;
use std::fmt::Write;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct FieldValue {
    pub field: Field,
    pub text: String,
}


#[derive(Clone,PartialEq,PartialOrd,Eq,Hash)]
pub struct Term {
    pub data: Vec<u8>, // avoid copies
    // pub field: Field,
	// pub text: &'a [u8],
}

impl Term {

    // TODO avoid all these copies.

    pub fn from_field_text(field: Field, text: &str) -> Term {
        let mut buffer = Vec::with_capacity(1 + text.len());
        let Field(field_idx) = field;
        buffer.clear();
        buffer.push(field_idx);
        buffer.extend(text.as_bytes());
        Term {
            data: buffer,
        }
    }

    pub fn write_into(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend(&self.data);
    }
}


pub struct Document {
    fields: Vec<FieldValue>,
}


impl Document {

    pub fn new() -> Document {
        Document {
            fields: Vec::new()
        }
    }

    pub fn set(&mut self, field: Field, text: &str) {
        self.add(FieldValue {
            field: field,
            text: String::from(text)
        });
    }

    pub fn add(&mut self, field_value: FieldValue) {
        self.fields.push(field_value);
    }

}

impl IntoIterator for Document {
    type Item = FieldValue;
    type IntoIter = ::std::vec::IntoIter<FieldValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }

}
