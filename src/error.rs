#![allow(enum_variant_names)]

use std::io;
use std::result;
use std::path::PathBuf;
use std::error;
use std::sync::PoisonError;
use directory::error::{FileError, OpenWriteError, OpenDirectoryError};
use query;
use schema;

#[derive(Debug)]
pub enum Error {
    FileError(FileError),
    OpenWriteError(OpenWriteError),
    IOError(io::Error),
    Poisoned,
    OpenDirectoryError(OpenDirectoryError),
    CorruptedFile(PathBuf, Box<error::Error + Send>),
    InvalidArgument(String),
    ErrorInThread(String), // TODO investigate better solution
    Other(Box<error::Error + Send>), // + Send + Sync + 'static
}

impl Error {
    pub fn make_other<E: error::Error + 'static + Send>(e: E) -> Error {
        Error::Other(Box::new(e))
    }
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Error {
        Error::IOError(io_error)
    }
}

impl From<query::ParsingError> for Error {
    fn from(parsing_error: query::ParsingError) -> Error {
        Error::InvalidArgument(format!("Query is invalid. {:?}", parsing_error))
    }
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(_: PoisonError<Guard>) -> Error {
        Error::Poisoned
    }
}

impl From<FileError> for Error {
    fn from(error: FileError) -> Error {
        Error::FileError(error)
    }
}

impl From<schema::DocParsingError> for Error {
    fn from(error: schema::DocParsingError) -> Error {
        Error::InvalidArgument(format!("Failed to parse document {:?}", error))
    }
}

impl From<OpenWriteError> for Error {
    fn from(error: OpenWriteError) -> Error {
        Error::OpenWriteError(error)
    }
}

impl From<OpenDirectoryError> for Error {
    fn from(error: OpenDirectoryError) -> Error {
        Error::OpenDirectoryError(error)
    }
}

pub type Result<T> = result::Result<T, Error>;
