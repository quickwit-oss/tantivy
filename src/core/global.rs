use std::io::{BufWriter, Write};
use std::io;

pub type DocId = usize;

// pub trait SeekableIterator<T>: Iterator<T> {
//     pub fn seek(&mut self, el: &T) -> bool;
// }


pub trait Flushable {
    fn flush<W: Write>(&self, writer: &mut W) -> Result<usize, io::Error>;
}
