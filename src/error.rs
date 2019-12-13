//! Definition of Tantivy's error and result.

use std::io;

use crate::directory::error::LockError;
use crate::directory::error::{IOError, OpenDirectoryError, OpenReadError, OpenWriteError};
use crate::fastfield::FastFieldNotAvailableError;
use crate::query;
use crate::schema;
use crate::Version;
use serde_json;
use std::fmt;
use std::path::PathBuf;
use std::sync::PoisonError;

pub struct DataCorruption {
    filepath: Option<PathBuf>,
    comment: String,
}

impl DataCorruption {
    pub fn new(filepath: PathBuf, comment: String) -> DataCorruption {
        DataCorruption {
            filepath: Some(filepath),
            comment,
        }
    }

    pub fn comment_only(comment: String) -> DataCorruption {
        DataCorruption {
            filepath: None,
            comment,
        }
    }
}

impl fmt::Debug for DataCorruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Data corruption: ")?;
        if let Some(ref filepath) = &self.filepath {
            write!(f, "(in file `{:?}`)", filepath)?;
        }
        write!(f, ": {}.", self.comment)?;
        Ok(())
    }
}

// Type of index incompatibility between the library and the index found on disk
// Since we will serialize library version and compression method used to the footer of the index.
// we can use this information to prepare a helpful error message with a hint to the user.
pub enum Incompatibility {
    CompressionMismatch { library: Version, index: Version },
    IndexMismatch { library: Version, index: Version },
    CompressionAndIndexMismatch { library: Version, index: Version },
    UnsupportedIndex,
}

impl fmt::Debug for Incompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Incompatibility::CompressionMismatch { library, index } => {
                let err = format!(
                    "Library was compiled with {:?} compression, index was compressed with {:?}",
                    library.store_compression, index.store_compression
                );
                let advice = format!(
                    "Change the feature flag to {:?} and rebuild the library",
                    index.store_compression
                );
                write!(f, "{}. {}", err, advice)?;
            }
            Incompatibility::IndexMismatch { library, index } => {
                let err = format!(
                    "Library version: {:?}, index version: {:?}",
                    library.index_format_version, index.index_format_version
                );
                // TODO make a more useful error message
                // include the version range that supports this index_format_version
                let advice = format!(
                    "Change tantivy to version {}.{} and rebuild the library",
                    index.major, index.minor
                );
                write!(f, "{}. {}", err, advice)?;
            }
            Incompatibility::CompressionAndIndexMismatch { library, index } => {
                let compression_err = format!(
                    "Library was compiled with {:?} compression, index was compressed with {:?}",
                    library.store_compression, index.store_compression
                );
                let compression_advice = format!(
                    "Change the feature flag to {:?} and rebuild the library",
                    index.store_compression
                );
                let index_err = format!(
                    "Library version: {:?}, index version: {:?}",
                    library.index_format_version, index.index_format_version
                );
                // TODO make a more useful error message
                // include the version range that supports this index_format_version
                let index_advice = format!(
                    "Change tantivy to version {}.{} and rebuild the library",
                    index.major, index.minor
                );
                write!(
                    f,
                    "{}. {}\n{}. {}",
                    compression_err, compression_advice, index_err, index_advice
                )?;
            }
            Incompatibility::UnsupportedIndex => {
                write!(f, "UnknownVersion of the index is not supported by this library. Please reindex or downgrade your tantivy version")?;
            }
        }

        Ok(())
    }
}

/// The library's failure based error enum
#[derive(Debug, Fail)]
pub enum TantivyError {
    /// Path does not exist.
    #[fail(display = "Path does not exist: '{:?}'", _0)]
    PathDoesNotExist(PathBuf),
    /// File already exists, this is a problem when we try to write into a new file.
    #[fail(display = "File already exists: '{:?}'", _0)]
    FileAlreadyExists(PathBuf),
    /// Index already exists in this directory
    #[fail(display = "Index already exists")]
    IndexAlreadyExists,
    /// Failed to acquire file lock
    #[fail(display = "Failed to acquire Lockfile: {:?}. {:?}", _0, _1)]
    LockFailure(LockError, Option<String>),
    /// IO Error.
    #[fail(display = "An IO error occurred: '{}'", _0)]
    IOError(#[cause] IOError),
    /// Data corruption.
    #[fail(display = "{:?}", _0)]
    DataCorruption(DataCorruption),
    /// A thread holding the locked panicked and poisoned the lock.
    #[fail(display = "A thread holding the locked panicked and poisoned the lock")]
    Poisoned,
    /// Invalid argument was passed by the user.
    #[fail(display = "An invalid argument was passed: '{}'", _0)]
    InvalidArgument(String),
    /// An Error happened in one of the thread.
    #[fail(display = "An error occurred in a thread: '{}'", _0)]
    ErrorInThread(String),
    /// An Error appeared related to the schema.
    #[fail(display = "Schema error: '{}'", _0)]
    SchemaError(String),
    /// System error. (e.g.: We failed spawning a new thread)
    #[fail(display = "System error.'{}'", _0)]
    SystemError(String),
    /// Index incompatible with current version of tantivy
    #[fail(display = "{:?}", _0)]
    IncompatibleIndex(Incompatibility),
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

impl From<IOError> for TantivyError {
    fn from(io_error: IOError) -> TantivyError {
        TantivyError::IOError(io_error)
    }
}

impl From<io::Error> for TantivyError {
    fn from(io_error: io::Error) -> TantivyError {
        TantivyError::IOError(io_error.into())
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

impl From<OpenReadError> for TantivyError {
    fn from(error: OpenReadError) -> TantivyError {
        match error {
            OpenReadError::FileDoesNotExist(filepath) => TantivyError::PathDoesNotExist(filepath),
            OpenReadError::IOError(io_error) => TantivyError::IOError(io_error),
        }
    }
}

impl From<schema::DocParsingError> for TantivyError {
    fn from(error: schema::DocParsingError) -> TantivyError {
        TantivyError::InvalidArgument(format!("Failed to parse document {:?}", error))
    }
}

impl From<OpenWriteError> for TantivyError {
    fn from(error: OpenWriteError) -> TantivyError {
        match error {
            OpenWriteError::FileAlreadyExists(filepath) => {
                TantivyError::FileAlreadyExists(filepath)
            }
            OpenWriteError::IOError(io_error) => TantivyError::IOError(io_error),
        }
    }
}

impl From<OpenDirectoryError> for TantivyError {
    fn from(error: OpenDirectoryError) -> TantivyError {
        match error {
            OpenDirectoryError::DoesNotExist(directory_path) => {
                TantivyError::PathDoesNotExist(directory_path)
            }
            OpenDirectoryError::NotADirectory(directory_path) => {
                TantivyError::InvalidArgument(format!("{:?} is not a directory", directory_path))
            }
            OpenDirectoryError::IoError(err) => TantivyError::IOError(IOError::from(err)),
        }
    }
}

impl From<serde_json::Error> for TantivyError {
    fn from(error: serde_json::Error) -> TantivyError {
        let io_err = io::Error::from(error);
        TantivyError::IOError(io_err.into())
    }
}

impl From<rayon::ThreadPoolBuildError> for TantivyError {
    fn from(error: rayon::ThreadPoolBuildError) -> TantivyError {
        TantivyError::SystemError(error.to_string())
    }
}
