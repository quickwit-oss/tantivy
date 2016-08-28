use std::io;
use common::BinarySerializable;
use std::io::Read;
use std::io::Write;
use schema::Field;
use schema::Value;


#[derive(Debug, Clone, Ord, PartialEq, Eq, PartialOrd, RustcEncodable, RustcDecodable)]
pub struct FieldValue {
    pub field: Field,
    pub value: Value,
}

impl FieldValue {
    pub fn field(&self) -> Field {
        self.field
    }

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
        Ok(FieldValue {
            field: field,
            value: value,
        })
    }
}

