use pyo3::prelude::*;
use pyo3::types::PyDateTime;

use tantivy::schema;

use crate::facet::Facet;

/// Field is a numeric indentifier that represents an entry in the Schema.
#[pyclass]
#[derive(Clone)]
pub(crate) struct Field {
    pub(crate) inner: schema::Field,
}

/// FieldValue holds together a Field and its Value.
#[pyclass]
pub(crate) struct FieldValue {
    pub(crate) field: Field,
    pub(crate) value: PyObject,
}

#[pymethods]
impl FieldValue {
    #[getter]
    fn field(&self) -> Field {
        self.field.clone()
    }

    #[getter]
    fn value(&self) -> &PyObject {
        &self.value
    }
}

impl FieldValue {
    pub(crate) fn value_to_py(
        py: Python,
        value: &schema::Value,
    ) -> Option<PyObject> {
        match value {
            schema::Value::Str(text) => Some(text.into_object(py)),
            schema::Value::U64(num) => Some(num.into_object(py)),
            schema::Value::I64(num) => Some(num.into_object(py)),
            schema::Value::Bytes(b) => Some(b.to_object(py)),
            schema::Value::Date(d) => {
                let date =
                    PyDateTime::from_timestamp(py, d.timestamp() as f64, None);

                match date {
                    Ok(d) => Some(d.into_object(py)),
                    Err(_e) => None,
                }
            }
            schema::Value::Facet(f) => {
                Some(Facet { inner: f.clone() }.into_object(py))
            }
        }
    }

    pub(crate) fn field_value_to_py(
        py: Python,
        field_value: &schema::FieldValue,
    ) -> FieldValue {
        let value = field_value.value();
        let field = field_value.field();

        FieldValue {
            field: Field { inner: field },
            value: FieldValue::value_to_py(py, value).unwrap(),
        }
    }
}
