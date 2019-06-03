use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::PyType;

use tantivy as tv;

use crate::field::Field;
use crate::index::Index;

/// Tantivy's Query
#[pyclass]
pub(crate) struct Query {
    pub(crate) inner: Box<dyn tv::query::Query>,
}

/// Tantivy's Query parser
#[pyclass]
pub(crate) struct QueryParser {
    inner: tv::query::QueryParser,
}

#[pymethods]
impl QueryParser {
    /// Creates a QueryParser for an Index.
    ///
    /// Args:
    ///     index (Index): The index for which the query will be created.
    ///     default_fields (List[Field]): A list of fields used to search if no
    ///         field is specified in the query.
    ///
    /// Returns the QueryParser.
    #[classmethod]
    fn for_index(
        _cls: &PyType,
        index: &Index,
        default_fields: Vec<&Field>,
    ) -> PyResult<QueryParser> {
        let default_fields: Vec<tv::schema::Field> =
            default_fields.iter().map(|&f| f.inner.clone()).collect();

        let parser =
            tv::query::QueryParser::for_index(&index.inner, default_fields);
        Ok(QueryParser { inner: parser })
    }

    /// Parse a string into a query that can be given to a searcher.
    ///
    /// Args:
    ///     query (str): A query string that should be parsed into a query.
    ///
    /// Returns the parsed Query object. Raises ValueError if there was an
    /// error with the query string.
    fn parse_query(&self, query: &str) -> PyResult<Query> {
        let ret = self.inner.parse_query(query);

        match ret {
            Ok(q) => Ok(Query { inner: q }),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Set the default way to compose queries to a conjunction.
    ///
    /// By default, the query happy tax payer is equivalent to the query happy
    /// OR tax OR payer. After calling .set_conjunction_by_default() happy tax
    /// payer will be interpreted by the parser as happy AND tax AND payer.
    fn set_conjunction_by_default(&mut self) {
        self.inner.set_conjunction_by_default();
    }
}
