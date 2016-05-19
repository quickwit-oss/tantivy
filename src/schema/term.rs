use std::io::Write;
use std::fmt;

use common::BinarySerializable;
use super::U32Field;
use super::TextField;

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Term {
    data: Vec<u8>,
}


impl Term {

    pub fn from_field_u32(field: &U32Field, val: u32) -> Term {
        let mut buffer = Vec::with_capacity(1 + 4);
        let U32Field(field_idx) = *field;
        buffer.clear();
        buffer.push(128 | field_idx);
        val.serialize(&mut buffer).unwrap();
        Term {
            data: buffer,
        }
    }

    fn type_num(&self,) -> u8 {
        self.data[0]
    }

    pub fn is_u32(&self,) -> bool {
        !self.is_text()
    }

    pub fn is_text(&self,) -> bool {
        self.type_num() & 128 == 0
    }

    pub fn get_text_field(&self,) -> Option<TextField> {
        if self.is_text() {
            Some(TextField(self.type_num()))
        }
        else {
            None
        }
    }

    pub fn from_field_text(field: &TextField, text: &str) -> Term {
        let mut buffer = Vec::with_capacity(1 + text.len());
        let TextField(field_idx) = *field;
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
        write!(f, "Term({:?})", &self.data[..])
    }
}
