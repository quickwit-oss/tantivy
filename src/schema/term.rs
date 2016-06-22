use std::io::Write;
use std::fmt;

use common::BinarySerializable;
use super::Field;

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Term {
    data: Vec<u8>,
}


impl Term {

    fn type_num(&self,) -> u8 {
        self.data[0]
    }

    pub fn get_field(&self,) -> Field {
        Field(self.type_num())
    }

    pub fn from_field_u32(field: Field, val: u32) -> Term {
        let mut buffer = Vec::with_capacity(1 + 4);
        buffer.clear();
        field.serialize(&mut buffer).unwrap();
        val.serialize(&mut buffer).unwrap();
        Term {
            data: buffer,
        }
    }

    pub fn from_field_text(field: Field, text: &str) -> Term {
        let mut buffer = Vec::with_capacity(1 + text.len());
        buffer.clear();
        field.serialize(&mut buffer).unwrap();
        buffer.extend(text.as_bytes());
        Term {
            data: buffer,
        }
    }

    pub fn as_slice(&self,)->&[u8] {
        &self.data
    }
}

impl<'a> From<&'a [u8]> for Term {
    fn from(data: &[u8]) -> Term {
        Term {
            data: Vec::from(data),
        }
    }
}

impl AsRef<[u8]> for Term {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Term({:?})", &self.data[..])
    }
}
