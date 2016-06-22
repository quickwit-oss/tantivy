pub mod writer;
pub mod codec;
pub mod searcher;
pub mod index;
pub mod merger;

mod segment_writer;
mod segment_reader;

use std::error;
use std::io;

pub use self::segment_reader::SegmentReader;

pub fn convert_to_ioerror<E: 'static + error::Error + Send + Sync>(err: E) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        err
    )
}
