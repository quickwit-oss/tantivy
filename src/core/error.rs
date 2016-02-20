use std::result;
use std::io;
use byteorder;

#[derive(Debug)]
pub enum Error {
    NotImplementedYet,
    WriteError(String),
    ReadError,
    BinaryReadError(byteorder::Error),
    IOError(io::ErrorKind, String),
    FileNotFound(String),
    LockError(String),
    ReadOnly(String),
    CannotAcquireLock(String),
    FSTFormat(String),
}

pub type Result<T> = result::Result<T, Error>;
