/// Definition of Tantivy's error and result.

use std::io;

use std::path::PathBuf;
use std::sync::PoisonError;
use directory::error::{IOError, OpenReadError, OpenWriteError, OpenDirectoryError};
use query;
use schema;
use fastfield::FastFieldNotAvailableError;
use serde_json;

error_chain!(
    errors {
        PathDoesNotExist(buf: PathBuf) {
            description("path does not exist")
            display("path does not exist: '{:?}'", buf)
        }
        FileAlreadyExists(buf: PathBuf) {
            description("file already exists")
            display("file already exists: '{:?}'", buf)
        }
        IOError(err: IOError) {
            description("an IO error occurred")
            display("an IO error occurred: '{}'", err)
        }
        CorruptedFile(buf: PathBuf) {
            description("file contains corrupted data")
            display("file contains corrupted data: '{:?}'", buf)
        }
        Poisoned {
            description("a thread holding the locked panicked and poisoned the lock")
        }
        InvalidArgument(arg: String) {
            description("an invalid argument was passed")
            display("an invalid argument was passed: '{}'", arg)
        }
        ErrorInThread(err: String) {
            description("an error occurred in a thread")
            display("an error occurred in a thread: '{}'", err)
        }
        SchemaError(field: String) {
            description("a schema field is missing")
            display("a schema field is missing: '{}'", field)
        }
        FastFieldError(err: FastFieldNotAvailableError) {
            description("fast field not available")
            display("fast field not available: '{:?}'", err)
        }
    }
);

impl From<FastFieldNotAvailableError> for Error {
    fn from(fastfield_error: FastFieldNotAvailableError) -> Error {
        ErrorKind::FastFieldError(fastfield_error).into()
    }
}

impl From<IOError> for Error {
    fn from(io_error: IOError) -> Error {
        ErrorKind::IOError(io_error).into()
    }
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Error {
        ErrorKind::IOError(io_error.into()).into()
    }
}

impl From<query::QueryParserError> for Error {
    fn from(parsing_error: query::QueryParserError) -> Error {
        ErrorKind::InvalidArgument(format!("Query is invalid. {:?}", parsing_error)).into()
    }
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(_: PoisonError<Guard>) -> Error {
        ErrorKind::Poisoned.into()
    }
}

impl From<OpenReadError> for Error {
    fn from(error: OpenReadError) -> Error {
        match error {
            OpenReadError::FileDoesNotExist(filepath) => {
                ErrorKind::PathDoesNotExist(filepath).into()
            }
            OpenReadError::IOError(io_error) => ErrorKind::IOError(io_error).into(),
        }
    }
}

impl From<schema::DocParsingError> for Error {
    fn from(error: schema::DocParsingError) -> Error {
        ErrorKind::InvalidArgument(format!("Failed to parse document {:?}", error)).into()
    }
}

impl From<OpenWriteError> for Error {
    fn from(error: OpenWriteError) -> Error {
        match error {
                OpenWriteError::FileAlreadyExists(filepath) => {
                    ErrorKind::FileAlreadyExists(filepath)
                }
                OpenWriteError::IOError(io_error) => ErrorKind::IOError(io_error),
            }
            .into()
    }
}

impl From<OpenDirectoryError> for Error {
    fn from(error: OpenDirectoryError) -> Error {
        match error {
            OpenDirectoryError::DoesNotExist(directory_path) => {
                ErrorKind::PathDoesNotExist(directory_path).into()
            }
            OpenDirectoryError::NotADirectory(directory_path) => {
                ErrorKind::InvalidArgument(format!("{:?} is not a directory", directory_path))
                    .into()
            }
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Error {
        let io_err = io::Error::from(error);
        ErrorKind::IOError(io_err.into()).into()
    }
}
