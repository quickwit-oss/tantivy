use pyo3::exceptions;
use pyo3::prelude::*;

use tantivy::schema;

use crate::document::Document;
use crate::field::Field;

/// Tantivy schema.
///
/// The schema is very strict. To build the schema the `SchemaBuilder` class is
/// provided.
#[pyclass]
pub(crate) struct Schema {
    pub(crate) inner: schema::Schema,
}

#[pymethods]
impl Schema {
    /// Build a document object from a json string.
    ///
    /// Args:
    ///     doc_json (str) - A string containing json that should be parsed
    ///         into a `Document`
    ///
    /// Returns the parsed document, raises a ValueError if the parsing failed.
    fn parse_document(&self, doc_json: &str) -> PyResult<Document> {
        let ret = self.inner.parse_document(doc_json);
        match ret {
            Ok(d) => Ok(Document { inner: d }),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Convert a `Document` object into a json string.
    ///
    /// Args:
    ///     doc (Document): The document that will be converted into a json
    ///         string.
    fn to_json(&self, doc: &Document) -> String {
        self.inner.to_json(&doc.inner)
    }

    /// Return the field name for a given `Field`.
    ///
    /// Args:
    ///     field (Field): The field for which the name will be returned.
    fn get_field_name(&self, field: &Field) -> &str {
        self.inner.get_field_name(field.inner)
    }

    /// Returns the field option associated with a given name.
    ///
    /// Args:
    ///     name (str): The name of the field that we want to retrieve.
    ///
    /// Returns the Field if one is found, None otherwise.
    fn get_field(&self, name: &str) -> Option<Field> {
        let f = self.inner.get_field(name);
        match f {
            Some(field) => Some(Field { inner: field }),
            None => None,
        }
    }
}
