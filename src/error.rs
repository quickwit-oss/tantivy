//! Definition of Tantivy's error and result.

use std::io;

use directory::error::{IOError, OpenDirectoryError, OpenReadError, OpenWriteError};
use fastfield::FastFieldNotAvailableError;
use query;
use schema;
use serde_json;
use std::path::PathBuf;
use std::sync::PoisonError;

/// The library's failure based error enum
#[derive(Debug, Fail)]
pub enum TantivyError {
    /// Path does not exist.
    #[fail(display = "path does not exist: '{:?}'", _0)]
    PathDoesNotExist(PathBuf),
    /// File already exists, this is a problem when we try to write into a new file.
    #[fail(display = "file already exists: '{:?}'", _0)]
    FileAlreadyExists(PathBuf),
    /// IO Error.
    #[fail(display = "an IO error occurred: '{}'", _0)]
    IOError(#[cause] IOError),
    /// The data within is corrupted.
    ///
    /// For instance, it contains invalid JSON.
    #[fail(display = "file contains corrupted data: '{:?}'", _0)]
    CorruptedFile(PathBuf),
    /// A thread holding the locked panicked and poisoned the lock.
    #[fail(display = "a thread holding the locked panicked and poisoned the lock")]
    Poisoned,
    /// Invalid argument was passed by the user.
    #[fail(display = "an invalid argument was passed: '{}'", _0)]
    InvalidArgument(String),
    /// An Error happened in one of the thread.
    #[fail(display = "an error occurred in a thread: '{}'", _0)]
    ErrorInThread(String),
    /// An Error appeared related to the schema.
    #[fail(display = "Schema error: '{}'", _0)]
    SchemaError(String),
    /// Tried to access a fastfield reader for a field not configured accordingly.
    #[fail(display = "fast field not available: '{:?}'", _0)]
    FastFieldError(#[cause] FastFieldNotAvailableError),
}

impl From<FastFieldNotAvailableError> for TantivyError {
    fn from(fastfield_error: FastFieldNotAvailableError) -> TantivyError {
        TantivyError::FastFieldError(fastfield_error).into()
    }
}

impl From<IOError> for TantivyError {
    fn from(io_error: IOError) -> TantivyError {
        TantivyError::IOError(io_error).into()
    }
}

impl From<io::Error> for TantivyError {
    fn from(io_error: io::Error) -> TantivyError {
        TantivyError::IOError(io_error.into()).into()
    }
}

impl From<query::QueryParserError> for TantivyError {
    fn from(parsing_error: query::QueryParserError) -> TantivyError {
        TantivyError::InvalidArgument(format!("Query is invalid. {:?}", parsing_error)).into()
    }
}

impl<Guard> From<PoisonError<Guard>> for TantivyError {
    fn from(_: PoisonError<Guard>) -> TantivyError {
        TantivyError::Poisoned.into()
    }
}

impl From<OpenReadError> for TantivyError {
    fn from(error: OpenReadError) -> TantivyError {
        match error {
            OpenReadError::FileDoesNotExist(filepath) => {
                TantivyError::PathDoesNotExist(filepath).into()
            }
            OpenReadError::IOError(io_error) => TantivyError::IOError(io_error).into(),
        }
    }
}

impl From<schema::DocParsingError> for TantivyError {
    fn from(error: schema::DocParsingError) -> TantivyError {
        TantivyError::InvalidArgument(format!("Failed to parse document {:?}", error)).into()
    }
}

impl From<OpenWriteError> for TantivyError {
    fn from(error: OpenWriteError) -> TantivyError {
        match error {
            OpenWriteError::FileAlreadyExists(filepath) => {
                TantivyError::FileAlreadyExists(filepath)
            }
            OpenWriteError::IOError(io_error) => TantivyError::IOError(io_error),
        }.into()
    }
}

impl From<OpenDirectoryError> for TantivyError {
    fn from(error: OpenDirectoryError) -> TantivyError {
        match error {
            OpenDirectoryError::DoesNotExist(directory_path) => {
                TantivyError::PathDoesNotExist(directory_path).into()
            }
            OpenDirectoryError::NotADirectory(directory_path) => TantivyError::InvalidArgument(
                format!("{:?} is not a directory", directory_path),
            ).into(),
        }
    }
}

impl From<serde_json::Error> for TantivyError {
    fn from(error: serde_json::Error) -> TantivyError {
        let io_err = io::Error::from(error);
        TantivyError::IOError(io_err.into()).into()
    }
}
