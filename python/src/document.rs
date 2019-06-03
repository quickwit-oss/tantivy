use pyo3::prelude::*;
use pyo3::types::PyDateTime;
use pyo3::types::{PyDateAccess, PyTimeAccess};

use chrono::offset::TimeZone;
use chrono::Utc;

use tantivy as tv;

use crate::facet::Facet;
use crate::field::{Field, FieldValue};

/// Tantivy's Document is the object that can be indexed and then searched for.
///
/// Documents are fundamentally a collection of unordered tuples
/// (field, value). In this list, one field may appear more than once.
///
/// Example:
///     >>> doc = tantivy.Document()
///     >>> doc.add_text(title, "The Old Man and the Sea")
///     >>> doc.add_text(body, ("He was an old man who fished alone in a "
///                             "skiff in the Gulf Stream and he had gone "
///                             "eighty-four days now without taking a fish."))
#[pyclass]
pub(crate) struct Document {
    pub(crate) inner: tv::Document,
}

#[pymethods]
impl Document {
    #[new]
    fn new(obj: &PyRawObject) {
        obj.init(Document {
            inner: tv::Document::default(),
        });
    }

    /// Add a text value to the document.
    ///
    /// Args:
    ///     field (Field): The field for which we are adding the text.
    ///     text (str): The text that will be added to the document.
    fn add_text(&mut self, field: &Field, text: &str) {
        self.inner.add_text(field.inner, text);
    }

    /// Add an unsigned integer value to the document.
    ///
    /// Args:
    ///     field (Field): The field for which we are adding the integer.
    ///     value (int): The integer that will be added to the document.
    fn add_unsigned(&mut self, field: &Field, value: u64) {
        self.inner.add_u64(field.inner, value);
    }

    /// Add a signed integer value to the document.
    ///
    /// Args:
    ///     field (Field): The field for which we are adding the integer.
    ///     value (int): The integer that will be added to the document.
    fn add_integer(&mut self, field: &Field, value: i64) {
        self.inner.add_i64(field.inner, value);
    }

    /// Add a date value to the document.
    ///
    /// Args:
    ///     field (Field): The field for which we are adding the integer.
    ///     value (datetime): The date that will be added to the document.
    fn add_date(&mut self, field: &Field, value: &PyDateTime) {
        let datetime = Utc
            .ymd(
                value.get_year().into(),
                value.get_month().into(),
                value.get_day().into(),
            )
            .and_hms_micro(
                value.get_hour().into(),
                value.get_minute().into(),
                value.get_second().into(),
                value.get_microsecond().into(),
            );

        self.inner.add_date(field.inner, &datetime);
    }

    /// Add a facet value to the document.
    /// Args:
    ///     field (Field): The field for which we are adding the facet.
    ///     value (Facet): The Facet that will be added to the document.
    fn add_facet(&mut self, field: &Field, value: &Facet) {
        self.inner.add_facet(field.inner, value.inner.clone());
    }

    /// Add a bytes value to the document.
    ///
    /// Args:
    ///     field (Field): The field for which we are adding the bytes.
    ///     value (bytes): The bytes that will be added to the document.
    fn add_bytes(&mut self, field: &Field, value: Vec<u8>) {
        self.inner.add_bytes(field.inner, value);
    }

    /// Returns the number of added fields that have been added to the document
    #[getter]
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// True if the document is empty, False otherwise.
    #[getter]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get the first value associated with the given field.
    ///
    /// Args:
    ///     field (Field): The field for which we would like to get the value.
    ///
    /// Returns the value if one is found, otherwise None.
    /// The type of the value depends on the field.
    fn get_first(&self, py: Python, field: &Field) -> Option<PyObject> {
        let value = self.inner.get_first(field.inner)?;
        FieldValue::value_to_py(py, value)
    }

    /// Get the all values associated with the given field.
    ///
    /// Args:
    ///     field (Field): The field for which we would like to get the values.
    ///
    /// Returns a list of values.
    /// The type of the value depends on the field.
    fn get_all(&self, py: Python, field: &Field) -> Vec<PyObject> {
        let values = self.inner.get_all(field.inner);
        values
            .iter()
            .map(|&v| FieldValue::value_to_py(py, v))
            .filter_map(|x| x)
            .collect()
    }

    /// Get all the fields and values contained in the document.
    fn field_values(&self, py: Python) -> Vec<FieldValue> {
        let field_values = self.inner.field_values();
        field_values
            .iter()
            .map(|v| FieldValue::field_value_to_py(py, v))
            .collect()
    }
}
