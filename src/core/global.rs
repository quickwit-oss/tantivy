use std::io::{BufWriter, Write};
use std::io;

pub type DocId = usize;
pub type FieldId = u8;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct Field(pub FieldId);
