use std::path::PathBuf;
use std::io;

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
    IOError(io::Error),
}

impl From<io::Error> for OpenWriteError {
    fn from(err: io::Error) -> OpenWriteError {
        OpenWriteError::IOError(err)
    }
}

/// Error that may occur when accessing a file (read, or delete)
#[derive(Debug)]
pub enum FileError {
    /// The file does not exists.
    FileDoesNotExist(PathBuf),
    /// Any kind of IO error that happens when 
    /// interacting with the underlying IO device.
    IOError(io::Error),
}

impl From<io::Error> for FileError {
    fn from(err: io::Error) -> FileError {
        FileError::IOError(err)
    }
}
