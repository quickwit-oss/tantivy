use std::fmt;

use common::BinarySerializable;
use super::Field;
use std::str;


/// Term represents the value that the token can take.
///
/// It actually wraps a `Vec<u8>`.
/// TODO remove pub
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Term(Vec<u8>);


impl Term {
    
    /// Pre-allocate a term buffer. 
    pub fn allocate(field: Field, num_bytes: usize) -> Term {
        let mut term = Term(Vec::with_capacity(num_bytes));
        field.serialize(&mut term.0).expect("Serializing term in a Vec should never fail");
        term
    }

    /// Set the content of the term.
    pub fn set_content(&mut self, content: &[u8]) {
        self.0.resize(content.len(), 0u8);
        (&mut self.0[..]).clone_from_slice(content);
    }
    
    /// Returns the field id.
    fn field_id(&self,) -> u8 {
        self.0[0]
    }

    /// Returns the field.
    pub fn field(&self,) -> Field {
        Field(self.field_id())
    }

    /// Builds a term given a field, and a u32-value
    ///
    /// Assuming the term has a field id of 1, and a u32 value of 3234,
    /// the Term will have 5 bytes.
    /// The first byte is `1`, and the 4 following bytes are that of the u32.
    pub fn from_field_u32(field: Field, val: u32) -> Term {
        let mut buffer = Vec::with_capacity(1 + 4);
        buffer.clear();
        field.serialize(&mut buffer).unwrap();
        val.serialize(&mut buffer).unwrap();
        Term(buffer)
    }
    
    /// Builds a term given a field, and a string value
    ///
    /// Assuming the term has a field id of 2, and a text value of "abc",
    /// the Term will have 4 bytes.
    /// The first byte is 2, and the three following bytes are the utf-8 
    /// representation of "abc".
    pub fn from_field_text(field: Field, text: &str) -> Term {
        let mut buffer = Vec::with_capacity(1 + text.len());
        buffer.clear();
        field.serialize(&mut buffer).unwrap();
        buffer.extend(text.as_bytes());
        Term(buffer)
    }
    
    /// Returns the serialized value of the term.
    /// (this does not include the field.)
    ///
    /// If the term is a string, its value is utf-8 encoded.
    /// If the term is a u32, its value is encoded according
    /// to `byteorder::LittleEndian`. 
    pub fn value(&self) -> &[u8] {
        &self.0[1..]
    }

    /// Returns the text associated with the term.
    ///
    /// # Panics
    /// If the value is not valid utf-8. This may happen
    /// if the index is corrupted or if you try to 
    /// call this method on a non-string type.
    pub unsafe fn text(&self) -> &str {
        str::from_utf8_unchecked(self.value())
    }

    /// Set the texts only, keeping the field untouched. 
    pub fn set_text(&mut self, text: &str) {
        self.0.resize(1, 0u8);
        self.0.extend(text.as_bytes());
    }
    
    /// Returns the underlying `&[u8]` 
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
        let mut schema_builder = SchemaBuilder::default();
        schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let count_field = schema_builder.add_text_field("count", STRING);
        {
            let term = Term::from_field_text(title_field, "test");
            assert_eq!(term.field(), title_field);
            assert_eq!(term.as_slice()[0], 1u8);
            assert_eq!(&term.as_slice()[1..], "test".as_bytes());
        }
        {
            let term = Term::from_field_u32(count_field, 983u32);
            assert_eq!(term.field(), count_field);
            assert_eq!(term.as_slice()[0], 2u8);
            assert_eq!(term.as_slice().len(), 5);
            assert_eq!(term.as_slice()[1], (983u32 % 256u32) as u8);            
        }
                
    }
}