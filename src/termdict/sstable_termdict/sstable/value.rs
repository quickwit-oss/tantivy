use std::io;

use super::{vint, BlockReader};

pub trait ValueReader: Default {
    type Value;

    fn value(&self, idx: usize) -> &Self::Value;

    fn read(&mut self, reader: &mut BlockReader) -> io::Result<()>;
}

pub trait ValueWriter: Default {
    type Value;

    fn write(&mut self, val: &Self::Value);

    fn write_block(&mut self, writer: &mut Vec<u8>);
}

#[derive(Default)]
pub struct VoidReader;

impl ValueReader for VoidReader {
    type Value = ();

    fn value(&self, _idx: usize) -> &() {
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

    fn write(&mut self, _val: &()) {}

    fn write_block(&mut self, _writer: &mut Vec<u8>) {}
}

#[derive(Default)]
pub struct U64MonotonicWriter {
    vals: Vec<u64>,
}

impl ValueWriter for U64MonotonicWriter {
    type Value = u64;

    fn write(&mut self, val: &Self::Value) {
        self.vals.push(*val);
    }

    fn write_block(&mut self, writer: &mut Vec<u8>) {
        let mut prev_val = 0u64;
        vint::serialize_into_vec(self.vals.len() as u64, writer);
        for &val in &self.vals {
            let delta = val - prev_val;
            vint::serialize_into_vec(delta, writer);
            prev_val = val;
        }
        self.vals.clear();
    }
}

#[derive(Default)]
pub struct U64MonotonicReader {
    vals: Vec<u64>,
}

impl ValueReader for U64MonotonicReader {
    type Value = u64;

    fn value(&self, idx: usize) -> &Self::Value {
        &self.vals[idx]
    }

    fn read(&mut self, reader: &mut BlockReader) -> io::Result<()> {
        let len = reader.deserialize_u64() as usize;
        self.vals.clear();
        let mut prev_val = 0u64;
        for _ in 0..len {
            let delta = reader.deserialize_u64() as u64;
            let val = prev_val + delta;
            self.vals.push(val);
            prev_val = val;
        }
        Ok(())
    }
}
