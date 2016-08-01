use std::io;
use common::BinarySerializable;
use std::io::Read;
use std::io::Write;
use schema::Field;
use schema::Term;

const TEXT_CODE: u8 = 0;
const U32_CODE: u8 = 1;

#[derive(Debug)]
pub enum FieldValue {
    Text(Field, String),
    U32(Field, u32),
}

impl FieldValue {
    pub fn field(&self) -> Field {
        match self {
            &FieldValue::Text(field, _) => {
                field
            },
            &FieldValue::U32(field, _) => {
                field
            }
        }
    }
    
    pub fn text(&self) -> &str {
        match self {
            &FieldValue::Text(_, ref text) => {
               text
            }
            _ => {
                panic!("This is not a text field.")
            }
        }
    }
    
    pub fn u32_value(&self) -> u32 {
        match self {
            &FieldValue::U32(_, value) => {
               value
            }
            _ => {
                panic!("This is not a text field.")
            }
        }
    }
}

impl BinarySerializable for FieldValue {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let mut written_size = 0;
        match self {
            &FieldValue::Text(ref field, ref text) => {
                written_size += try!(TEXT_CODE.serialize(writer));
                written_size += try!(field.serialize(writer));
                written_size += try!(text.serialize(writer));
            },
            &FieldValue::U32(ref field, ref val) => {
                written_size += try!(U32_CODE.serialize(writer));
                written_size += try!(field.serialize(writer));
                written_size += try!(val.serialize(writer));
            },            
        }
        Ok(written_size)
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let type_code = try!(u8::deserialize(reader));
        match type_code {
            TEXT_CODE => {
                let field = try!(Field::deserialize(reader));
                let text = try!(String::deserialize(reader));
                Ok(FieldValue::Text(field, text))
            }
            U32_CODE => {
                let field = try!(Field::deserialize(reader));
                let value = try!(u32::deserialize(reader));
                Ok(FieldValue::U32(field, value))
            }
            _ => {
                Err(io::Error::new(io::ErrorKind::InvalidData, format!("No field type is associated with code {:?}", type_code)))
            }
        }      
    }
}

