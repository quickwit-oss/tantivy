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

/// A helper wrapper for creating standard iterators
/// out of the fields iterator trait.
pub struct FieldValueIter<'a>(pub(crate) std::slice::Iter<'a, FieldValue>);

impl<'a> Iterator for FieldValueIter<'a> {
    type Item = (Field, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|field_value| (field_value.field, &field_value.value))
    }
}
