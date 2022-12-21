use std::io;
use std::ops::Range;

use crate::value::{deserialize_u64, ValueReader, ValueWriter};

#[derive(Default)]
pub struct RangeReader {
    vals: Vec<Range<u64>>,
}

impl ValueReader for RangeReader {
    type Value = Range<u64>;

    fn value(&self, idx: usize) -> &Range<u64> {
        &self.vals[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        self.vals.clear();
        let original_num_bytes = data.len();
        let len = deserialize_u64(&mut data) as usize;
        if len != 0 {
            let mut prev_val = deserialize_u64(&mut data);
            for _ in 1..len {
                let next_val = prev_val + deserialize_u64(&mut data);
                self.vals.push(prev_val..next_val);
                prev_val = next_val;
            }
        }
        Ok(original_num_bytes - data.len())
    }
}

#[derive(Default)]
pub struct RangeWriter {
    vals: Vec<u64>,
}

impl ValueWriter for RangeWriter {
    type Value = Range<u64>;

    fn write(&mut self, val: &Range<u64>) {
        if let Some(previous_offset) = self.vals.last().copied() {
            assert_eq!(previous_offset, val.start);
            self.vals.push(val.end);
        } else {
            self.vals.push(val.start);
            self.vals.push(val.end)
        }
    }

    fn serialize_block(&mut self, writer: &mut Vec<u8>) {
        let mut prev_val = 0u64;
        crate::vint::serialize_into_vec(self.vals.len() as u64, writer);
        for &val in &self.vals {
            let delta = val - prev_val;
            crate::vint::serialize_into_vec(delta, writer);
            prev_val = val;
        }
        self.vals.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_reader_writer() {
        crate::value::tests::test_value_reader_writer::<_, RangeReader, RangeWriter>(&[]);
        crate::value::tests::test_value_reader_writer::<_, RangeReader, RangeWriter>(&[0..3]);
        crate::value::tests::test_value_reader_writer::<_, RangeReader, RangeWriter>(&[
            0..3,
            3..10,
        ]);
        crate::value::tests::test_value_reader_writer::<_, RangeReader, RangeWriter>(&[
            0..0,
            0..10,
        ]);
        crate::value::tests::test_value_reader_writer::<_, RangeReader, RangeWriter>(&[
            100..110,
            110..121,
            121..1250,
        ]);
    }

    #[test]
    #[should_panic]
    fn test_range_reader_writer_panics() {
        crate::value::tests::test_value_reader_writer::<_, RangeReader, RangeWriter>(&[
            1..3,
            4..10,
        ]);
    }
}
