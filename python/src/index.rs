use pyo3::exceptions;
use pyo3::prelude::*;

use crate::document::Document;
use crate::schema::Schema;
use crate::searcher::Searcher;
use tantivy as tv;
use tantivy::directory::MmapDirectory;

const RELOAD_POLICY: &str = "commit";

/// IndexReader is the entry point to read and search the index.
///
/// IndexReader controls when a new version of the index should be loaded and
/// lends you instances of Searcher for the last loaded version.
///
/// To create an IndexReader first create an Index and call the reader() method
/// on the index object.
#[pyclass]
pub(crate) struct IndexReader {
    inner: tv::IndexReader,
}

#[pymethods]
impl IndexReader {
    /// Update searchers so that they reflect the state of the last .commit().
    ///
    /// If you set up the the reload policy to be on 'commit' (which is the
    /// default) every commit should be rapidly reflected on your IndexReader
    /// and you should not need to call reload() at all.
    fn reload(&self) -> PyResult<()> {
        let ret = self.inner.reload();
        match ret {
            Ok(_) => Ok(()),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Get a Searcher for the index.
    ///
    /// This method should be called every single time a search query is
    /// performed. The searchers are taken from a pool of num_searchers
    /// searchers.
    ///
    /// Returns a Searcher object, if no searcher is available this may block.
    fn searcher(&self) -> Searcher {
        let searcher = self.inner.searcher();
        Searcher { inner: searcher }
    }
}

/// IndexWriter is the user entry-point to add documents to the index.
///
/// To create an IndexWriter first create an Index and call the writer() method
/// on the index object.
#[pyclass]
pub(crate) struct IndexWriter {
    inner: tv::IndexWriter,
}

#[pymethods]
impl IndexWriter {
    /// Add a document to the index.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// Returns an `opstamp`, which is an increasing integer that can be used
    /// by the client to align commits with its own document queue.
    /// The `opstamp` represents the number of documents that have been added
    /// since the creation of the index.
    fn add_document(&mut self, document: &Document) -> PyResult<()> {
        self.inner.add_document(document.inner.clone());
        Ok(())
    }

    /// Commits all of the pending changes
    ///
    /// A call to commit blocks. After it returns, all of the document that
    /// were added since the last commit are published and persisted.
    ///
    /// In case of a crash or an hardware failure (as long as the hard disk is
    /// spared), it will be possible to resume indexing from this point.
    ///
    /// Returns the `opstamp` of the last document that made it in the commit.
    fn commit(&mut self) -> PyResult<()> {
        let ret = self.inner.commit();
        match ret {
            Ok(_) => Ok(()),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Rollback to the last commit
    ///
    /// This cancels all of the update that happened before after the last
    /// commit. After calling rollback, the index is in the same state as it
    /// was after the last commit.
    fn rollback(&mut self) -> PyResult<()> {
        let ret = self.inner.rollback();

        match ret {
            Ok(_) => Ok(()),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Detect and removes the files that are not used by the index anymore.
    fn garbage_collect_files(&mut self) -> PyResult<()> {
        let ret = self.inner.garbage_collect_files();

        match ret {
            Ok(_) => Ok(()),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// The opstamp of the last successful commit.
    ///
    /// This is the opstamp the index will rollback to if there is a failure
    /// like a power surge.
    ///
    /// This is also the opstamp of the commit that is currently available
    /// for searchers.
    #[getter]
    fn commit_opstamp(&self) -> u64 {
        self.inner.commit_opstamp()
    }
}

/// Create a new index object.
///
/// Args:
///     schema (Schema): The schema of the index.
///     path (str, optional): The path where the index should be stored. If
///         no path is provided, the index will be stored in memory.
///     reuse (bool, optional): Should we open an existing index if one exists
///         or always create a new one.
///
/// If an index already exists it will be opened and reused. Raises OSError
/// if there was a problem during the opening or creation of the index.
#[pyclass]
pub(crate) struct Index {
    pub(crate) inner: tv::Index,
}

#[pymethods]
impl Index {
    #[new]
    #[args(reuse = true)]
    fn new(
        obj: &PyRawObject,
        schema: &Schema,
        path: Option<&str>,
        reuse: bool,
    ) -> PyResult<()> {
        let index = match path {
            Some(p) => {
                let directory = MmapDirectory::open(p);

                let dir = match directory {
                    Ok(d) => d,
                    Err(e) => {
                        return Err(exceptions::OSError::py_err(e.to_string()))
                    }
                };

                let i = if reuse {
                    tv::Index::open_or_create(dir, schema.inner.clone())
                } else {
                    tv::Index::create(dir, schema.inner.clone())
                };

                match i {
                    Ok(index) => index,
                    Err(e) => {
                        return Err(exceptions::OSError::py_err(e.to_string()))
                    }
                }
            }
            None => tv::Index::create_in_ram(schema.inner.clone()),
        };

        obj.init(Index { inner: index });
        Ok(())
    }

    /// Create a `IndexWriter` for the index.
    ///
    /// The writer will be multithreaded and the provided heap size will be
    /// split between the given number of threads.
    ///
    /// Args:
    ///     overall_heap_size (int, optional): The total target memory usage of
    ///         the writer, can't be less than 3000000.
    ///     num_threads (int, optional): The number of threads that the writer
    ///         should use. If this value is 0, tantivy will choose
    ///         automatically the number of threads.
    ///
    /// Raises ValueError if there was an error while creating the writer.
    #[args(heap_size = 3000000, num_threads = 0)]
    fn writer(
        &self,
        heap_size: usize,
        num_threads: usize,
    ) -> PyResult<IndexWriter> {
        let writer = match num_threads {
            0 => self.inner.writer(heap_size),
            _ => self.inner.writer_with_num_threads(num_threads, heap_size),
        };

        match writer {
            Ok(w) => Ok(IndexWriter { inner: w }),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Create an IndexReader for the index.
    ///
    /// Args:
    ///     reload_policy (str, optional): The reload policy that the
    ///         IndexReader should use. Can be manual or OnCommit.
    ///     num_searchers (int, optional): The number of searchers that the
    ///         reader should create.
    ///
    /// Returns the IndexReader on success, raises ValueError if a IndexReader
    /// couldn't be created.
    #[args(reload_policy = "RELOAD_POLICY", num_searchers = 0)]
    fn reader(
        &self,
        reload_policy: &str,
        num_searchers: usize,
    ) -> PyResult<IndexReader> {
        let reload_policy = reload_policy.to_lowercase();
        let reload_policy = match reload_policy.as_ref() {
            "commit" => tv::ReloadPolicy::OnCommit,
            "on-commit" => tv::ReloadPolicy::OnCommit,
            "oncommit" => tv::ReloadPolicy::OnCommit,
            "manual" => tv::ReloadPolicy::Manual,
            _ => return Err(exceptions::ValueError::py_err(
                "Invalid reload policy, valid choices are: 'manual' and 'OnCommit'"
            ))
        };

        let builder = self.inner.reader_builder();

        let builder = builder.reload_policy(reload_policy);
        let builder = if num_searchers > 0 {
            builder.num_searchers(num_searchers)
        } else {
            builder
        };

        let reader = builder.try_into();
        match reader {
            Ok(r) => Ok(IndexReader { inner: r }),
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }
    }

    /// Check if the given path contains an existing index.
    /// Args:
    ///     path: The path where tantivy will search for an index.
    ///
    /// Returns True if an index exists at the given path, False otherwise.
    ///
    /// Raises OSError if the directory cannot be opened.
    #[staticmethod]
    fn exists(path: &str) -> PyResult<bool> {
        let directory = MmapDirectory::open(path);
        let dir = match directory {
            Ok(d) => d,
            Err(e) => return Err(exceptions::OSError::py_err(e.to_string())),
        };

        Ok(tv::Index::exists(&dir))
    }

    /// The schema of the current index.
    #[getter]
    fn schema(&self) -> Schema {
        let schema = self.inner.schema();
        Schema { inner: schema }
    }
}
