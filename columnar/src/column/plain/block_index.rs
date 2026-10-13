//! The block index of a plain-encoded column: value idx -> block, and block -> byte range.
//!
//! ```text
//! u32           num_blocks (B)
//! [u32; B + 1]  value_starts: first value idx of each block, then num_values
//! [u64; B + 1]  byte_starts: start of each block in the blocks area, then its length
//! ```

use std::io;
use std::ops::Range;

use crate::RowId;

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Maps value idxs to blocks, and blocks to their byte range.
///
/// Invariants:
/// - `value_starts.len() == byte_starts.len() == num_blocks + 1`
/// - `value_starts[0] == 0` and `byte_starts[0] == 0`
/// - both are strictly increasing: blocks are never empty.
#[derive(Clone, Debug)]
pub(crate) struct PlainBlockIndex {
    value_starts: Vec<RowId>,
    byte_starts: Vec<u64>,
}

impl Default for PlainBlockIndex {
    fn default() -> Self {
        PlainBlockIndex {
            value_starts: vec![0],
            byte_starts: vec![0],
        }
    }
}

impl PlainBlockIndex {
    /// Appends a block holding `num_values` values, encoded on `num_bytes` bytes.
    pub(crate) fn push_block(&mut self, num_values: u32, num_bytes: u64) {
        assert!(
            num_values > 0 && num_bytes > 0,
            "plain blocks are never empty"
        );
        let value_start = self.num_values();
        let byte_start = self.blocks_num_bytes();
        self.value_starts.push(
            value_start
                .checked_add(num_values)
                .expect("a column holds at most u32::MAX values"),
        );
        self.byte_starts.push(byte_start + num_bytes);
    }

    pub(crate) fn num_blocks(&self) -> u32 {
        (self.value_starts.len() - 1) as u32
    }

    pub(crate) fn num_values(&self) -> u32 {
        *self.value_starts.last().unwrap()
    }

    /// Length of the blocks area.
    pub(crate) fn blocks_num_bytes(&self) -> u64 {
        *self.byte_starts.last().unwrap()
    }

    /// Returns the value idxs of block `block_id`.
    pub(crate) fn value_range(&self, block_id: u32) -> Range<RowId> {
        let block_id = block_id as usize;
        self.value_starts[block_id]..self.value_starts[block_id + 1]
    }

    /// Returns the byte range of block `block_id` in the blocks area.
    pub(crate) fn byte_range(&self, block_id: u32) -> Range<u64> {
        let block_id = block_id as usize;
        self.byte_starts[block_id]..self.byte_starts[block_id + 1]
    }

    /// Returns the block containing the value `value_idx`.
    ///
    /// Precondition: `value_idx < self.num_values()`.
    pub(crate) fn block_containing(&self, value_idx: RowId) -> u32 {
        debug_assert!(value_idx < self.num_values());
        // `value_starts[0] == 0 <= value_idx`, so the partition point is at least 1.
        let num_blocks_starting_before = self
            .value_starts
            .partition_point(|&value_start| value_start <= value_idx);
        (num_blocks_starting_before - 1) as u32
    }

    pub(crate) fn serialize(&self, output: &mut Vec<u8>) {
        output.reserve(4 + self.value_starts.len() * 12);
        output.extend_from_slice(&self.num_blocks().to_le_bytes());
        for value_start in &self.value_starts {
            output.extend_from_slice(&value_start.to_le_bytes());
        }
        for byte_start in &self.byte_starts {
            output.extend_from_slice(&byte_start.to_le_bytes());
        }
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> io::Result<PlainBlockIndex> {
        if bytes.len() < 4 {
            return Err(invalid_data("truncated plain block index"));
        }
        let num_blocks = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let value_starts_bytes_len = (num_blocks + 1) * 4;
        let byte_starts_bytes_len = (num_blocks + 1) * 8;
        if bytes.len() != 4 + value_starts_bytes_len + byte_starts_bytes_len {
            return Err(invalid_data("invalid plain block index length"));
        }
        let (value_starts_bytes, byte_starts_bytes) = bytes[4..].split_at(value_starts_bytes_len);
        let mut value_starts: Vec<RowId> = Vec::with_capacity(num_blocks + 1);
        for value_start_bytes in value_starts_bytes.as_chunks::<4>().0 {
            value_starts.push(u32::from_le_bytes(*value_start_bytes));
        }
        let mut byte_starts: Vec<u64> = Vec::with_capacity(num_blocks + 1);
        for byte_start_bytes in byte_starts_bytes.as_chunks::<8>().0 {
            byte_starts.push(u64::from_le_bytes(*byte_start_bytes));
        }
        if value_starts[0] != 0 || byte_starts[0] != 0 {
            return Err(invalid_data("plain block index does not start at 0"));
        }
        for block_id in 0..num_blocks {
            if value_starts[block_id] >= value_starts[block_id + 1]
                || byte_starts[block_id] >= byte_starts[block_id + 1]
            {
                return Err(invalid_data("plain block index is not strictly increasing"));
            }
        }
        Ok(PlainBlockIndex {
            value_starts,
            byte_starts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_index_for_test(block_num_values: &[u32]) -> PlainBlockIndex {
        let mut block_index = PlainBlockIndex::default();
        for (block_id, &num_values) in block_num_values.iter().enumerate() {
            block_index.push_block(num_values, 100 + block_id as u64);
        }
        block_index
    }

    #[test]
    fn test_empty_block_index() {
        let block_index = PlainBlockIndex::default();
        assert_eq!(block_index.num_blocks(), 0);
        assert_eq!(block_index.num_values(), 0);
        assert_eq!(block_index.blocks_num_bytes(), 0);
        let mut buffer = Vec::new();
        block_index.serialize(&mut buffer);
        let block_index = PlainBlockIndex::deserialize(&buffer).unwrap();
        assert_eq!(block_index.num_blocks(), 0);
    }

    #[test]
    fn test_block_containing() {
        let block_index = block_index_for_test(&[3, 1, 65_536, 2]);
        assert_eq!(block_index.num_values(), 65_542);
        let expected_blocks = [
            (0, 0),
            (2, 0),
            (3, 1),
            (4, 2),
            (65_539, 2),
            (65_540, 3),
            (65_541, 3),
        ];
        for (value_idx, expected_block) in expected_blocks {
            assert_eq!(block_index.block_containing(value_idx), expected_block);
            assert!(block_index.value_range(expected_block).contains(&value_idx));
        }
        assert_eq!(block_index.byte_range(0), 0..100);
        assert_eq!(block_index.byte_range(1), 100..201);
        assert_eq!(block_index.blocks_num_bytes(), 100 + 101 + 102 + 103);
    }

    #[test]
    fn test_serialize_deserialize() {
        let block_index = block_index_for_test(&[3, 1, 65_536, 2]);
        let mut buffer = Vec::new();
        block_index.serialize(&mut buffer);
        assert_eq!(buffer.len(), 4 + 5 * 4 + 5 * 8);
        let deserialized = PlainBlockIndex::deserialize(&buffer).unwrap();
        assert_eq!(deserialized.value_starts, block_index.value_starts);
        assert_eq!(deserialized.byte_starts, block_index.byte_starts);
    }

    #[test]
    fn test_deserialize_invalid() {
        let block_index = block_index_for_test(&[3, 1]);
        let mut buffer = Vec::new();
        block_index.serialize(&mut buffer);
        assert!(PlainBlockIndex::deserialize(&buffer[..buffer.len() - 1]).is_err());
        assert!(PlainBlockIndex::deserialize(&[]).is_err());
        // Second value start equal to the first: empty block.
        let mut corrupted = buffer.clone();
        corrupted[8..12].copy_from_slice(&0u32.to_le_bytes());
        assert!(PlainBlockIndex::deserialize(&corrupted).is_err());
    }
}
