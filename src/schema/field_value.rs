use crate::schema::{Field, OwnedValue};

/// `FieldValue` holds together a `Field` and its `Value`.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldValue {
    pub field: Field,
    pub value: OwnedValue,
}

impl FieldValue {
    /// Constructor
    pub fn new(field: Field, value: OwnedValue) -> FieldValue {
        FieldValue { field, value }
    }

    /// Field accessor
    pub fn field(&self) -> Field {
        self.field
    }

    /// Value accessor
    pub fn value(&self) -> &OwnedValue {
        &self.value
    }
}

impl From<FieldValue> for OwnedValue {
    fn from(field_value: FieldValue) -> Self {
        field_value.value
    }
}

/// A helper wrapper for creating standard iterators
/// out of the fields iterator trait.
pub struct FieldValueIter<'a>(pub(crate) std::slice::Iter<'a, FieldValue>);

impl<'a> Iterator for FieldValueIter<'a> {
    type Item = (Field, &'a OwnedValue);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|field_value| (field_value.field, &field_value.value))
    }
}
