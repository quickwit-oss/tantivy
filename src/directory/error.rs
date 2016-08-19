use std::path::PathBuf;
use std::io;

#[derive(Debug)]
pub enum OpenWriteError {
    FileAlreadyExists(PathBuf),
    IOError(io::Error),
}

impl From<io::Error> for OpenWriteError {
    fn from(err: io::Error) -> OpenWriteError {
        OpenWriteError::IOError(err)
    }
}

#[derive(Debug)]
pub enum FileError {
    FileDoesNotExist(PathBuf),
    IOError(io::Error),
}

impl From<io::Error> for FileError {
    fn from(err: io::Error) -> FileError {
        FileError::IOError(err)
    }
}

#[derive(Debug)]
pub enum OpenDirectoryError {
    DoesNotExist,
    NotADirectory,
}