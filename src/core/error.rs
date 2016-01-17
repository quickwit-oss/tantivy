use std::result;


pub enum Error {
    IOError(String),
}

pub type Result<T> = result::Result<T, Error>;
