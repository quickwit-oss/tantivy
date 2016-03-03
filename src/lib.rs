#![feature(test)]
#[allow(unused_imports)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate fst;
extern crate byteorder;
extern crate memmap;
extern crate rand;
extern crate regex;
extern crate tempfile;
extern crate rustc_serialize;
extern crate combine;
extern crate atomicwrites;
extern crate tempdir;
extern crate bincode;
extern crate time;
extern crate serde;
extern crate libc;
extern crate lz4;

#[cfg(test)] extern crate test;

mod core;

pub use core::index::Index;
