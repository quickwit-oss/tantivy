use pyo3::exceptions;
use pyo3::prelude::*;

use tantivy as tv;

use crate::document::Document;
use crate::query::Query;

/// Tantivy's Searcher class
///
/// A Searcher is used to search the index given a prepared Query.
#[pyclass]
pub(crate) struct Searcher {
    pub(crate) inner: tv::LeasedItem<tv::Searcher>,
}

#[pymethods]
impl Searcher {
    /// Search the index with the given query and collect results.
    ///
    /// Args:
    ///     query (Query): The query that will be used for the search.
    ///     collector (Collector): A collector that determines how the search
    ///         results will be collected. Only the TopDocs collector is
    ///         supported for now.
    ///
    /// Returns a list of tuples that contains the scores and DocAddress of the
    /// search results.
    ///
    /// Raises a ValueError if there was an error with the search.
    fn search(
        &self,
        query: &Query,
        collector: &mut TopDocs,
    ) -> PyResult<Vec<(f32, DocAddress)>> {
        let ret = self.inner.search(&query.inner, &collector.inner);
        match ret {
            Ok(r) => {
                let result: Vec<(f32, DocAddress)> = r
                    .iter()
                    .map(|(f, d)| (f.clone(), DocAddress::from(d)))
                    .collect();
                Ok(result)
            }
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Returns the overall number of documents in the index.
    #[getter]
    fn num_docs(&self) -> u64 {
        self.inner.num_docs()
    }

    /// Fetches a document from Tantivy's store given a DocAddress.
    ///
    /// Args:
    ///     doc_address (DocAddress): The DocAddress that is associated with
    ///         the document that we wish to fetch.
    ///
    /// Returns the Document, raises ValueError if the document can't be found.
    fn doc(&self, doc_address: &DocAddress) -> PyResult<Document> {
        let ret = self.inner.doc(doc_address.into());
        match ret {
            Ok(doc) => Ok(Document { inner: doc }),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }
}

/// DocAddress contains all the necessary information to identify a document
/// given a Searcher object.
///
/// It consists in an id identifying its segment, and its segment-local DocId.
/// The id used for the segment is actually an ordinal in the list of segment
/// hold by a Searcher.
#[pyclass]
pub(crate) struct DocAddress {
    pub(crate) segment_ord: tv::SegmentLocalId,
    pub(crate) doc: tv::DocId,
}

#[pymethods]
impl DocAddress {
    /// The segment ordinal is an id identifying the segment hosting the
    /// document. It is only meaningful, in the context of a searcher.
    #[getter]
    fn segment_ord(&self) -> u32 {
        self.segment_ord
    }

    /// The segment local DocId
    #[getter]
    fn doc(&self) -> u32 {
        self.doc
    }
}

impl From<&tv::DocAddress> for DocAddress {
    fn from(doc_address: &tv::DocAddress) -> Self {
        DocAddress {
            segment_ord: doc_address.segment_ord(),
            doc: doc_address.doc(),
        }
    }
}

impl Into<tv::DocAddress> for &DocAddress {
    fn into(self) -> tv::DocAddress {
        tv::DocAddress(self.segment_ord(), self.doc())
    }
}

/// The Top Score Collector keeps track of the K documents sorted by their
/// score.
///
/// Args:
///     limit (int, optional): The number of documents that the top scorer will
///         retrieve. Must be a positive integer larger than 0. Defaults to 10.
#[pyclass]
pub(crate) struct TopDocs {
    inner: tv::collector::TopDocs,
}

#[pymethods]
impl TopDocs {
    #[new]
    #[args(limit = 10)]
    fn new(obj: &PyRawObject, limit: usize) -> PyResult<()> {
        let top = tv::collector::TopDocs::with_limit(limit);
        obj.init(TopDocs { inner: top });
        Ok(())
    }
}
