use std::io;
use common::BinarySerializable;
use std::io::Read;
use std::io::Write;
use schema::Field;
use schema::Value;


/// `FieldValue` holds together a `Field` and its `Value`.
#[derive(Debug, Clone, Ord, PartialEq, Eq, PartialOrd, RustcEncodable, RustcDecodable)]
pub struct FieldValue {
    field: Field,
    value: Value,
}

impl FieldValue {
    
    /// Constructor
    pub fn new(field: Field, value: Value) -> FieldValue {
        FieldValue {
            field: field,
            value: value,
        }
    }
    
    /// Field accessor 
    pub fn field(&self) -> Field {
        self.field
    }

    /// Value accessor
    pub fn value(&self,) -> &Value {
        &self.value
    }
}

impl BinarySerializable for FieldValue {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let mut written_size = 0;
        written_size += try!(self.field.serialize(writer));
        written_size += try!(self.value.serialize(writer));
        Ok(written_size)
    }

    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let field = try!(Field::deserialize(reader));
        let value = try!(Value::deserialize(reader));
        Ok(FieldValue::new(field, value))
    }
}

