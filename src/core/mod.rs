pub mod postings;
pub mod schema;
pub mod directory;
pub mod writer;
pub mod analyzer;
pub mod serial;
pub mod reader;
pub mod codec;
pub mod searcher;
pub mod collector;
pub mod serialize;
pub mod store;
pub mod simdcompression;
pub mod fstmap;
pub mod index;


use std::error;
use std::io;

pub fn convert_to_ioerror<E: 'static + error::Error + Send + Sync>(err: E) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        err
    )
}
