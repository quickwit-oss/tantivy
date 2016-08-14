
use common::BinarySerializable;
use std::io;
use std::io::Write;
use std::io::Read;

#[derive(Debug, Clone)]
pub enum Value {
    Str(String),
    U32(u32),
}

impl Value {
    pub fn text(&self) -> &str {
        match *self {
            Value::Str(ref text) => {
               text
            }
            _ => {
                panic!("This is not a text field.")
            }
        }
    }
    
    pub fn u32_value(&self) -> u32 {
        match *self {
            Value::U32(ref value) => {
               *value
            }
            _ => {
                panic!("This is not a text field.")
            }
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Str(s)
    }
}


impl From<u32> for Value {
    fn from(v: u32) -> Value {
        Value::U32(v)
    }
}


const TEXT_CODE: u8 = 0;
const U32_CODE: u8 = 1;


impl BinarySerializable for Value {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let mut written_size = 0;
        match *self {
            Value::Str(ref text) => {
                written_size += try!(TEXT_CODE.serialize(writer));
                written_size += try!(text.serialize(writer));
            },
            Value::U32(ref val) => {
                written_size += try!(U32_CODE.serialize(writer));
                written_size += try!(val.serialize(writer));
            },            
        }
        Ok(written_size)
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let type_code = try!(u8::deserialize(reader));
        match type_code {
            TEXT_CODE => {
                let text = try!(String::deserialize(reader));
                Ok(Value::Str(text))
            }
            U32_CODE => {
                let value = try!(u32::deserialize(reader));
                Ok(Value::U32(value))
            }
            _ => {
                Err(io::Error::new(io::ErrorKind::InvalidData, format!("No field type is associated with code {:?}", type_code)))
            }
        }      
    }
}