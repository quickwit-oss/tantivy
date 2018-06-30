use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::path::PathBuf;

/// General IO error with an optional path to the offending file.
#[derive(Debug)]
pub struct IOError {
    path: Option<PathBuf>,
    err: io::Error,
}

impl fmt::Display for IOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.path {
            Some(ref path) => write!(f, "io error occurred on path '{:?}': '{}'", path, self.err),
            None => write!(f, "io error occurred: '{}'", self.err),
        }
    }
}

impl StdError for IOError {
    fn description(&self) -> &str {
        "io error occurred"
    }

    fn cause(&self) -> Option<&StdError> {
        Some(&self.err)
    }
}

impl IOError {
    pub(crate) fn with_path(path: PathBuf, err: io::Error) -> Self {
        IOError {
            path: Some(path),
            err,
        }
    }
}

impl From<io::Error> for IOError {
    fn from(err: io::Error) -> IOError {
        IOError { path: None, err }
    }
}

/// Error that may occur when opening a directory
#[derive(Debug)]
pub enum OpenDirectoryError {
    /// The underlying directory does not exists.
    DoesNotExist(PathBuf),
    /// The path exists but is not a directory.
    NotADirectory(PathBuf),
}

impl fmt::Display for OpenDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OpenDirectoryError::DoesNotExist(ref path) => {
                write!(f, "the underlying directory '{:?}' does not exist", path)
            }
            OpenDirectoryError::NotADirectory(ref path) => {
                write!(f, "the path '{:?}' exists but is not a directory", path)
            }
        }
    }
}

impl StdError for OpenDirectoryError {
    fn description(&self) -> &str {
        "error occurred while opening a directory"
    }

    fn cause(&self) -> Option<&StdError> {
        None
    }
}

/// Error that may occur when starting to write in a file
#[derive(Debug)]
pub enum OpenWriteError {
    /// Our directory is WORM, writing an existing file is forbidden.
    /// Checkout the `Directory` documentation.
    FileAlreadyExists(PathBuf),
    /// Any kind of IO error that happens when
    /// writing in the underlying IO device.
    IOError(IOError),
}

impl From<IOError> for OpenWriteError {
    fn from(err: IOError) -> OpenWriteError {
        OpenWriteError::IOError(err)
    }
}

impl fmt::Display for OpenWriteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OpenWriteError::FileAlreadyExists(ref path) => {
                write!(f, "the file '{:?}' already exists", path)
            }
            OpenWriteError::IOError(ref err) => write!(
                f,
                "an io error occurred while opening a file for writing: '{}'",
                err
            ),
        }
    }
}

impl StdError for OpenWriteError {
    fn description(&self) -> &str {
        "error occurred while opening a file for writing"
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            OpenWriteError::FileAlreadyExists(_) => None,
            OpenWriteError::IOError(ref err) => Some(err),
        }
    }
}

/// Error that may occur when accessing a file read
#[derive(Debug)]
pub enum OpenReadError {
    /// The file does not exists.
    FileDoesNotExist(PathBuf),
    /// Any kind of IO error that happens when
    /// interacting with the underlying IO device.
    IOError(IOError),
}

impl From<IOError> for OpenReadError {
    fn from(err: IOError) -> OpenReadError {
        OpenReadError::IOError(err)
    }
}

impl fmt::Display for OpenReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OpenReadError::FileDoesNotExist(ref path) => {
                write!(f, "the file '{:?}' does not exist", path)
            }
            OpenReadError::IOError(ref err) => write!(
                f,
                "an io error occurred while opening a file for reading: '{}'",
                err
            ),
        }
    }
}

impl StdError for OpenReadError {
    fn description(&self) -> &str {
        "error occurred while opening a file for reading"
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            OpenReadError::FileDoesNotExist(_) => None,
            OpenReadError::IOError(ref err) => Some(err),
        }
    }
}

/// Error that may occur when trying to delete a file
#[derive(Debug)]
pub enum DeleteError {
    /// The file does not exists.
    FileDoesNotExist(PathBuf),
    /// Any kind of IO error that happens when
    /// interacting with the underlying IO device.
    IOError(IOError),
}

impl From<IOError> for DeleteError {
    fn from(err: IOError) -> DeleteError {
        DeleteError::IOError(err)
    }
}

impl fmt::Display for DeleteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DeleteError::FileDoesNotExist(ref path) => {
                write!(f, "the file '{:?}' does not exist", path)
            }
            DeleteError::IOError(ref err) => {
                write!(f, "an io error occurred while deleting a file: '{}'", err)
            }
        }
    }
}

impl StdError for DeleteError {
    fn description(&self) -> &str {
        "error occurred while deleting a file"
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            DeleteError::FileDoesNotExist(_) => None,
            DeleteError::IOError(ref err) => Some(err),
        }
    }
}
