use std::io::{self, Read, Write};

use common::BinarySerializable;

use crate::schema::{Field, Value};

/// `FieldValue` holds together a `Field` and its `Value`.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(bound(deserialize = "'a: 'de, 'de: 'a"))]
pub struct FieldValue<'a> {
    pub field: Field,
    pub value: Value<'a>,
}

impl<'a> FieldValue<'a> {
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

impl<'a> From<FieldValue<'a>> for Value<'a> {
    fn from(field_value: FieldValue<'a>) -> Self {
        field_value.value
    }
}

impl<'a> BinarySerializable for FieldValue<'a> {
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
