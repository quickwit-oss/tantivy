use std::fmt;

use common::BinarySerializable;
use super::Field;

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Term(Vec<u8>);

impl Term {

    fn field_id(&self,) -> u8 {
        self.0[0]
    }

    pub fn get_field(&self,) -> Field {
        Field(self.field_id())
    }

    pub fn from_field_u32(field: Field, val: u32) -> Term {
        let mut buffer = Vec::with_capacity(1 + 4);
        buffer.clear();
        field.serialize(&mut buffer).unwrap();
        val.serialize(&mut buffer).unwrap();
        Term(buffer)
    }

    pub fn from_field_text(field: Field, text: &str) -> Term {
        let mut buffer = Vec::with_capacity(1 + text.len());
        buffer.clear();
        field.serialize(&mut buffer).unwrap();
        buffer.extend(text.as_bytes());
        Term(buffer)
    }

    pub fn as_slice(&self,)->&[u8] {
        &self.0
    }
}

impl<'a> From<&'a [u8]> for Term {
    fn from(data: &[u8]) -> Term {
        Term(Vec::from(data))
    }
}

impl AsRef<[u8]> for Term {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Term({:?})", &self.0[..])
    }
}



#[cfg(test)]
mod tests {
    
    use schema::*;

    #[test]
    pub fn test_term() {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let count_field = schema_builder.add_text_field("count", STRING);
        {
            let term = Term::from_field_text(title_field, "test");
            assert_eq!(term.get_field(), title_field);
            assert_eq!(term.as_slice()[0], 1u8);
            assert_eq!(&term.as_slice()[1..], "test".as_bytes());
        }
        {
            let term = Term::from_field_u32(count_field, 983u32);
            assert_eq!(term.get_field(), count_field);
            assert_eq!(term.as_slice()[0], 2u8);
            assert_eq!(term.as_slice().len(), 5);
            assert_eq!(term.as_slice()[1], (983u32 % 256u32) as u8);            
        }
                
    }
}