use std::io::Write;
use std::fmt;

use core::serialize::BinarySerializable;
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
        write!(f, "Term({})", self.data[0])
    }
}
