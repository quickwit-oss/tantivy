use std::io;
use std::result;
use std::path::PathBuf;
use std::error;
use std::sync::PoisonError;
use directory::error::{FileError, OpenWriteError, OpenDirectoryError};

#[derive(Debug)]
pub enum Error {
    FileError(FileError),
    OpenWriteError(OpenWriteError),
    IOError(io::Error),
    Poisoned,
    OpenDirectoryError(OpenDirectoryError),
    CorruptedFile(PathBuf, Box<error::Error>),
    InvalidArgument(String),
    ErrorInThread(String), // TODO investigate better solution
    Other(Box<error::Error>), // + Send + Sync + 'static
}

impl Error {
    pub fn make_other<E: error::Error + 'static>(e: E) -> Error {
        Error::Other(Box::new(e))
    }
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Error {
        Error::IOError(io_error)
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
