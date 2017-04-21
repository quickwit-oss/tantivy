
use common::BinarySerializable;
use std::io;
use std::io::Write;
use std::io::Read;

/// Value represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, RustcEncodable, RustcDecodable)]
pub enum Value {
    /// The str type is used for any text information.
    Str(String),
    /// Unsigned 32-bits Integer `u64`
    U64(u64),
}

impl Value {
    /// Returns the text value, provided the value is of the `Str` type.
    ///
    /// # Panics
    /// If the value is not of type `Str` 
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
    
    /// Returns the u64-value, provided the value is of the `U64` type.
    ///
    /// # Panics
    /// If the value is not of type `U64` 
    pub fn u64_value(&self) -> u64 {
        match *self {
            Value::U64(ref value) => {
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


impl From<u64> for Value {
    fn from(v: u64) -> Value {
        Value::U64(v)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value {
        Value::Str(s.to_string())
    }
}

const TEXT_CODE: u8 = 0;
const U64_CODE: u8 = 1;


impl BinarySerializable for Value {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let mut written_size = 0;
        match *self {
            Value::Str(ref text) => {
                written_size += try!(TEXT_CODE.serialize(writer));
                written_size += try!(text.serialize(writer));
            },
            Value::U64(ref val) => {
                written_size += try!(U64_CODE.serialize(writer));
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
            U64_CODE => {
                let value = try!(u64::deserialize(reader));
                Ok(Value::U64(value))
            }
            _ => {
                Err(io::Error::new(io::ErrorKind::InvalidData, format!("No field type is associated with code {:?}", type_code)))
            }
        }      
    }
}
