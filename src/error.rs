//! Definition of Tantivy's errors and results.

use std::path::PathBuf;
use std::sync::PoisonError;
use std::{fmt, io};

use thiserror::Error;

use crate::directory::error::{
    Incompatibility, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use crate::fastfield::FastFieldNotAvailableError;
use crate::{query, schema};

/// Represents a `DataCorruption` error.
///
/// When facing data corruption, tantivy actually panics or returns this error.
pub struct DataCorruption {
    filepath: Option<PathBuf>,
    comment: String,
}

impl DataCorruption {
    /// Creates a `DataCorruption` Error.
    pub fn new(filepath: PathBuf, comment: String) -> DataCorruption {
        DataCorruption {
            filepath: Some(filepath),
            comment,
        }
    }

    /// Creates a `DataCorruption` Error, when the filepath is irrelevant.
    pub fn comment_only<TStr: ToString>(comment: TStr) -> DataCorruption {
        DataCorruption {
            filepath: None,
            comment: comment.to_string(),
        }
    }
}

impl fmt::Debug for DataCorruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Data corruption")?;
        if let Some(ref filepath) = &self.filepath {
            write!(f, " (in file `{:?}`)", filepath)?;
        }
        write!(f, ": {}.", self.comment)?;
        Ok(())
    }
}

/// The library's error enum
#[derive(Debug, Error)]
pub enum TantivyError {
    /// Failed to open the directory.
    #[error("Failed to open the directory: '{0:?}'")]
    OpenDirectoryError(#[from] OpenDirectoryError),
    /// Failed to open a file for read.
    #[error("Failed to open file for read: '{0:?}'")]
    OpenReadError(#[from] OpenReadError),
    /// Failed to open a file for write.
    #[error("Failed to open file for write: '{0:?}'")]
    OpenWriteError(#[from] OpenWriteError),
    /// Index already exists in this directory.
    #[error("Index already exists")]
    IndexAlreadyExists,
    /// Failed to acquire file lock.
    #[error("Failed to acquire Lockfile: {0:?}. {1:?}")]
    LockFailure(LockError, Option<String>),
    /// IO Error.
    #[error("An IO error occurred: '{0}'")]
    IoError(#[from] io::Error),
    /// Data corruption.
    #[error("Data corrupted: '{0:?}'")]
    DataCorruption(DataCorruption),
    /// A thread holding the locked panicked and poisoned the lock.
    #[error("A thread holding the locked panicked and poisoned the lock")]
    Poisoned,
    /// The provided field name does not exist.
    #[error("The field does not exist: '{0}'")]
    FieldNotFound(String),
    /// Invalid argument was passed by the user.
    #[error("An invalid argument was passed: '{0}'")]
    InvalidArgument(String),
    /// An Error occurred in one of the threads.
    #[error("An error occurred in a thread: '{0}'")]
    ErrorInThread(String),
    /// An Error occurred related to opening or creating a index.
    #[error("Missing required index builder argument when open/create index: '{0}'")]
    IndexBuilderMissingArgument(&'static str),
    /// An Error occurred related to the schema.
    #[error("Schema error: '{0}'")]
    SchemaError(String),
    /// System error. (e.g.: We failed spawning a new thread).
    #[error("System error.'{0}'")]
    SystemError(String),
    /// Index incompatible with current version of Tantivy.
    #[error("{0:?}")]
    IncompatibleIndex(Incompatibility),
}

#[cfg(feature = "quickwit")]
#[derive(Error, Debug)]
#[doc(hidden)]
pub enum AsyncIoError {
    #[error("io::Error `{0}`")]
    Io(#[from] io::Error),
    #[error("Asynchronous API is unsupported by this directory")]
    AsyncUnsupported,
}

#[cfg(feature = "quickwit")]
impl From<AsyncIoError> for TantivyError {
    fn from(async_io_err: AsyncIoError) -> Self {
        match async_io_err {
            AsyncIoError::Io(io_err) => TantivyError::from(io_err),
            AsyncIoError::AsyncUnsupported => {
                TantivyError::SystemError(format!("{:?}", async_io_err))
            }
        }
    }
}

impl From<DataCorruption> for TantivyError {
    fn from(data_corruption: DataCorruption) -> TantivyError {
        TantivyError::DataCorruption(data_corruption)
    }
}
impl From<FastFieldNotAvailableError> for TantivyError {
    fn from(fastfield_error: FastFieldNotAvailableError) -> TantivyError {
        TantivyError::SchemaError(format!("{}", fastfield_error))
    }
}
impl From<LockError> for TantivyError {
    fn from(lock_error: LockError) -> TantivyError {
        TantivyError::LockFailure(lock_error, None)
    }
}

impl From<query::QueryParserError> for TantivyError {
    fn from(parsing_error: query::QueryParserError) -> TantivyError {
        TantivyError::InvalidArgument(format!("Query is invalid. {:?}", parsing_error))
    }
}

impl<Guard> From<PoisonError<Guard>> for TantivyError {
    fn from(_: PoisonError<Guard>) -> TantivyError {
        TantivyError::Poisoned
    }
}

impl From<chrono::ParseError> for TantivyError {
    fn from(err: chrono::ParseError) -> TantivyError {
        TantivyError::InvalidArgument(err.to_string())
    }
}

impl From<schema::DocParsingError> for TantivyError {
    fn from(error: schema::DocParsingError) -> TantivyError {
        TantivyError::InvalidArgument(format!("Failed to parse document {:?}", error))
    }
}

impl From<serde_json::Error> for TantivyError {
    fn from(error: serde_json::Error) -> TantivyError {
        TantivyError::IoError(error.into())
    }
}

impl From<rayon::ThreadPoolBuildError> for TantivyError {
    fn from(error: rayon::ThreadPoolBuildError) -> TantivyError {
        TantivyError::SystemError(error.to_string())
    }
}
