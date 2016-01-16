use std::io::{BufWriter, Write};
use std::io;

pub type DocId = usize;
pub type FieldId = u32;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct Field(pub FieldId);

// pub trait SeekableIterator<T>: Iterator<T> {
//     pub fn seek(&mut self, el: &T) -> bool;
// }
