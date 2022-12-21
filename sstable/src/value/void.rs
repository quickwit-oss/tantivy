use std::io;

use crate::value::{ValueReader, ValueWriter};

#[derive(Default)]
pub struct VoidReader;

impl ValueReader for VoidReader {
    type Value = ();

    fn value(&self, _idx: usize) -> &() {
        &()
    }

    fn load(&mut self, _data: &[u8]) -> io::Result<usize> {
        Ok(0)
    }
}

#[derive(Default)]
pub struct VoidWriter;

impl ValueWriter for VoidWriter {
    type Value = ();

    fn write(&mut self, _val: &()) {}

    fn serialize_block(&mut self, _output: &mut Vec<u8>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_reader_writer() {
        crate::value::tests::test_value_reader_writer::<_, VoidReader, VoidWriter>(&[]);
        crate::value::tests::test_value_reader_writer::<_, VoidReader, VoidWriter>(&[()]);
        crate::value::tests::test_value_reader_writer::<_, VoidReader, VoidWriter>(&[(), (), ()]);
    }
}
