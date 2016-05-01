pub mod writer;
pub mod analyzer;
pub mod reader;
pub mod codec;
pub mod searcher;
pub mod collector;
pub mod index;
pub mod merger;

use std::error;
use std::io;

pub fn convert_to_ioerror<E: 'static + error::Error + Send + Sync>(err: E) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        err
    )
}
