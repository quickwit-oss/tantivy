use core::global::*;
use std::fmt::Write;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::string::FromUtf8Error;
use std::str;
use std::fmt;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct FieldValue {
    pub field: Field,
    pub text: String,
}


#[derive(Clone,PartialEq,PartialOrd,Ord,Eq,Hash)]
pub struct Term {
    data: Vec<u8>,
}

impl Term {

    // TODO avoid all these copies in Term.
    // when the term is in the dictionary, there
    // shouldn't be any copy
    pub fn field(&self,) -> Field {
        Field(self.data[0])
    }

    pub fn text(&self,) -> &str {
        str::from_utf8(&self.data[1..]).unwrap()
    }

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

    pub fn from(data: &[u8]) -> Term {
        Term {
            data: Vec::from(data),
        }
    }

    pub fn as_slice(&self,)->&[u8] {
        &self.data
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Term({}: {})", self.data[0], self.text())
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
