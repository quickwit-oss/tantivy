use std::io::{self, Read, Write};

use common::BinarySerializable;

use crate::schema::{Field, Value};

/// `FieldValue` holds together a `Field` and its `Value`.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldValue {
    pub field: Field,
    pub value: Value,
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

impl From<FieldValue> for Value {
    fn from(field_value: FieldValue) -> Self {
        field_value.value
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
        Ok(FieldValue { field, value })
    }
}
