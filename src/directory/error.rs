use std::path::PathBuf;
use std::io;
use std::fmt;

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

impl IOError {
    pub(crate) fn with_path(
                            path: PathBuf,
                            err: io::Error)
                            -> Self {
        IOError {
            path: Some(path),
            err: err,
        }
    }
}

impl From<io::Error> for IOError {
    fn from(err: io::Error) -> IOError {
        IOError {
            path: None,
            err: err,
        }
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

/// Error that may occur when trying to delete a file
#[derive(Debug)]
pub enum DeleteError {
    /// The file does not exists.
    FileDoesNotExist(PathBuf),
    /// Any kind of IO error that happens when
    /// interacting with the underlying IO device.
    IOError(IOError),
    /// The file may not be deleted because it is
    /// protected.
    FileProtected(PathBuf),
}

impl From<IOError> for DeleteError {
    fn from(err: IOError) -> DeleteError {
        DeleteError::IOError(err)
    }
}
