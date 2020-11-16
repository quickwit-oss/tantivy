use std::io;
use BlockReader;

pub trait ValueReader: Default {

    type Value;

    fn value(&self) -> &Self::Value;

    fn read(&mut self, reader: &mut BlockReader) -> io::Result<()>;
}

pub trait ValueWriter: Default {

    type Value;

    fn write(&mut self, val: &Self::Value, writer: &mut Vec<u8>);
}


#[derive(Default)]
pub struct VoidReader;

impl ValueReader for VoidReader {
    type Value = ();

    fn value(&self) -> &Self::Value {
        &()
    }

    fn read(&mut self, _reader: &mut BlockReader) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct VoidWriter;

impl ValueWriter for VoidWriter {
    type Value = ();

    fn write(&mut self, _: &Self::Value, _: &mut Vec<u8>) {}
}