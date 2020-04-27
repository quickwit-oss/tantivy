use crate::common::BinarySerializable;
use crate::schema::Field;
use crate::schema::Value;
use std::io::{self, Read, Write};

/// `FieldValue` holds together a `Field` and its `Value`.
#[derive(Debug, Clone, Ord, PartialEq, Eq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct FieldValue {
    field: Field,
    value: Value,
}

impl FieldValue {
    /// Constructor
    pub fn new(field: Field, value: Value) -> FieldValue {
        FieldValue { field, value }
    }

    /// Field accessor
    pub fn field(&self) -> Field {
        self.field
    }

    /// Value accessor
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl BinarySerializable for FieldValue {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.field.serialize(writer)?;
        self.value.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let field = Field::deserialize(reader)?;
        let value = Value::deserialize(reader)?;
        Ok(FieldValue::new(field, value))
    }
}
