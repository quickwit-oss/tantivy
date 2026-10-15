//! The block index of a plain-encoded column: value idx -> block, and block -> byte range.
//!
//! ```text
//! u32           num_blocks (B)
//! [u32; B + 1]  value_starts: first value idx of each block, then num_values
//! [u64; B + 1]  byte_starts: start of each block in the blocks area, then its length
//! ```
//!
//! The entries have a fixed width: the index is read in place, without being deserialized.

use std::io;
use std::ops::Range;

use common::OwnedBytes;

use crate::RowId;

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Returns true if `values` are strictly increasing.
///
/// Branch-free and without early exit, so that it vectorizes: optimized for valid indexes.
fn is_strictly_increasing<T: Ord>(values: impl Iterator<Item = T> + Clone) -> bool {
    values
        .clone()
        .zip(values.skip(1))
        .fold(true, |ok, (previous, next)| ok & (previous < next))
}

/// Builds the block index of a plain column, block by block.
///
/// Invariant: `value_starts` and `byte_starts` have the same length, start with 0, and are
/// strictly increasing.
pub(crate) struct PlainBlockIndexBuilder {
    value_starts: Vec<RowId>,
    byte_starts: Vec<u64>,
}

impl Default for PlainBlockIndexBuilder {
    fn default() -> Self {
        PlainBlockIndexBuilder {
            value_starts: vec![0],
            byte_starts: vec![0],
        }
    }
}

impl PlainBlockIndexBuilder {
    /// Appends a block holding `num_values` values, encoded on `num_bytes` bytes.
    pub(crate) fn push_block(&mut self, num_values: u32, num_bytes: u64) {
        assert!(
            num_values > 0 && num_bytes > 0,
            "plain blocks are never empty"
        );
        // The end of the new block is the start of the next one.
        let value_end: u32 = self
            .value_starts
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(num_values)
            .expect("a column holds at most u32::MAX values");
        let byte_end: u64 = self.byte_starts.last().copied().unwrap_or(0) + num_bytes;
        self.value_starts.push(value_end);
        self.byte_starts.push(byte_end);
    }

    pub(crate) fn serialize(&self, output: &mut Vec<u8>) {
        let num_blocks = (self.value_starts.len() - 1) as u32;
        output.reserve(4 + self.value_starts.len() * 12);
        output.extend_from_slice(&num_blocks.to_le_bytes());
        for value_start in &self.value_starts {
            output.extend_from_slice(&value_start.to_le_bytes());
        }
        for byte_start in &self.byte_starts {
            output.extend_from_slice(&byte_start.to_le_bytes());
        }
    }

    /// Serializes the index and opens it.
    pub(crate) fn build(&self) -> PlainBlockIndex {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer);
        PlainBlockIndex::open(OwnedBytes::new(buffer))
            .expect("the index built by the builder is valid")
    }
}

/// Maps value idxs to blocks, and blocks to their byte range.
///
/// A view on the serialized index: the entries are read in place. Cheap to clone.
///
/// Invariants, checked by `open`: `value_starts` and `byte_starts` both have `num_blocks + 1`
/// entries, start with 0, and are strictly increasing (blocks are never empty).
#[derive(Clone)]
pub(crate) struct PlainBlockIndex {
    num_blocks: u32,
    /// `num_blocks + 1` little endian `u32`.
    value_starts: OwnedBytes,
    /// `num_blocks + 1` little endian `u64`.
    byte_starts: OwnedBytes,
}

impl PlainBlockIndex {
    /// Returns the index of a column without any block.
    pub(crate) fn empty() -> PlainBlockIndex {
        PlainBlockIndexBuilder::default().build()
    }

    /// Opens a serialized block index, after checking its invariants.
    pub(crate) fn open(bytes: OwnedBytes) -> io::Result<PlainBlockIndex> {
        if bytes.len() < 4 {
            return Err(invalid_data("truncated plain block index"));
        }
        let (num_blocks_bytes, body) = bytes.split(4);
        let num_blocks = u32::from_le_bytes(num_blocks_bytes.as_slice().try_into().unwrap());
        let num_entries = num_blocks as usize + 1;
        if body.len() != num_entries * (4 + 8) {
            return Err(invalid_data("invalid plain block index length"));
        }
        let (value_starts, byte_starts) = body.split(num_entries * 4);
        let block_index = PlainBlockIndex {
            num_blocks,
            value_starts,
            byte_starts,
        };
        if block_index.value_start(0) != 0 || block_index.byte_start(0) != 0 {
            return Err(invalid_data("plain block index does not start at 0"));
        }
        // Blocks are never empty: both value and byte starts are strictly increasing.
        // `block_containing` relies on it (binary search), and so do the range computations.
        let (value_starts, _) = block_index.value_starts.as_slice().as_chunks::<4>();
        let (byte_starts, _) = block_index.byte_starts.as_slice().as_chunks::<8>();
        let value_starts = value_starts
            .iter()
            .map(|value_start| u32::from_le_bytes(*value_start));
        let byte_starts = byte_starts
            .iter()
            .map(|byte_start| u64::from_le_bytes(*byte_start));
        if !(is_strictly_increasing(value_starts) & is_strictly_increasing(byte_starts)) {
            return Err(invalid_data(
                "plain block index starts are not strictly increasing",
            ));
        }
        Ok(block_index)
    }

    /// Returns the entry `entry_id` of `value_starts`. Precondition: `entry_id <= num_blocks`.
    fn value_start(&self, entry_id: u32) -> RowId {
        let (value_starts, _) = self.value_starts.as_slice().as_chunks::<4>();
        u32::from_le_bytes(value_starts[entry_id as usize])
    }

    /// Returns the entry `entry_id` of `byte_starts`. Precondition: `entry_id <= num_blocks`.
    fn byte_start(&self, entry_id: u32) -> u64 {
        let (byte_starts, _) = self.byte_starts.as_slice().as_chunks::<8>();
        u64::from_le_bytes(byte_starts[entry_id as usize])
    }

    pub(crate) fn num_blocks(&self) -> u32 {
        self.num_blocks
    }

    pub(crate) fn num_values(&self) -> u32 {
        self.value_start(self.num_blocks)
    }

    /// Length of the blocks area.
    pub(crate) fn blocks_num_bytes(&self) -> u64 {
        self.byte_start(self.num_blocks)
    }

    /// Returns the value idxs of block `block_id`.
    pub(crate) fn value_range(&self, block_id: u32) -> Range<RowId> {
        self.value_start(block_id)..self.value_start(block_id + 1)
    }

    /// Returns the byte range of block `block_id` in the blocks area.
    pub(crate) fn byte_range(&self, block_id: u32) -> Range<u64> {
        self.byte_start(block_id)..self.byte_start(block_id + 1)
    }

    /// Returns the block containing the value `value_idx`.
    ///
    /// Precondition: `value_idx < self.num_values()`.
    pub(crate) fn block_containing(&self, value_idx: RowId) -> u32 {
        debug_assert!(value_idx < self.num_values());
        let (value_starts, _) = self.value_starts.as_slice().as_chunks::<4>();
        // `value_starts[0] == 0 <= value_idx`, so the partition point is at least 1.
        let num_blocks_starting_before = value_starts
            .partition_point(|value_start| u32::from_le_bytes(*value_start) <= value_idx);
        (num_blocks_starting_before - 1) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builder_for_test(block_num_values: &[u32]) -> PlainBlockIndexBuilder {
        let mut builder = PlainBlockIndexBuilder::default();
        for (block_id, &num_values) in block_num_values.iter().enumerate() {
            builder.push_block(num_values, 100 + block_id as u64);
        }
        builder
    }

    #[test]
    fn test_empty_block_index() {
        let block_index = PlainBlockIndex::empty();
        assert_eq!(block_index.num_blocks(), 0);
        assert_eq!(block_index.num_values(), 0);
        assert_eq!(block_index.blocks_num_bytes(), 0);
    }

    #[test]
    fn test_block_containing() {
        let block_index = builder_for_test(&[3, 1, 65_536, 2]).build();
        assert_eq!(block_index.num_blocks(), 4);
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
        assert_eq!(block_index.value_range(1), 3..4);
        assert_eq!(block_index.byte_range(0), 0..100);
        assert_eq!(block_index.byte_range(1), 100..201);
        assert_eq!(block_index.blocks_num_bytes(), 100 + 101 + 102 + 103);
    }

    #[test]
    fn test_serialized_length() {
        let mut buffer = Vec::new();
        builder_for_test(&[3, 1, 65_536, 2]).serialize(&mut buffer);
        assert_eq!(buffer.len(), 4 + 5 * 4 + 5 * 8);
    }

    /// Serializes a block index with the given entries, without checking them.
    ///
    /// Precondition: `value_starts.len() == byte_starts.len()`.
    fn serialize_raw(value_starts: &[u32], byte_starts: &[u64]) -> OwnedBytes {
        let num_blocks = value_starts.len() as u32 - 1;
        let mut buffer: Vec<u8> = Vec::with_capacity(4 + value_starts.len() * 12);
        buffer.extend_from_slice(&num_blocks.to_le_bytes());
        for value_start in value_starts {
            buffer.extend_from_slice(&value_start.to_le_bytes());
        }
        for byte_start in byte_starts {
            buffer.extend_from_slice(&byte_start.to_le_bytes());
        }
        OwnedBytes::new(buffer)
    }

    #[test]
    fn test_open_valid_block_index() {
        let block_index = PlainBlockIndex::open(serialize_raw(&[0, 3, 4], &[0, 100, 201])).unwrap();
        assert_eq!(block_index.num_blocks(), 2);
        assert_eq!(block_index.value_range(1), 3..4);
    }

    #[test]
    fn test_open_non_increasing_value_starts_is_an_error() {
        // Decreasing.
        assert!(PlainBlockIndex::open(serialize_raw(&[0, 3, 2], &[0, 100, 201])).is_err());
        // Equal: an empty block.
        assert!(PlainBlockIndex::open(serialize_raw(&[0, 3, 3], &[0, 100, 201])).is_err());
    }

    #[test]
    fn test_open_non_increasing_byte_starts_is_an_error() {
        // Decreasing.
        assert!(PlainBlockIndex::open(serialize_raw(&[0, 3, 4], &[0, 100, 50])).is_err());
        // Equal: an empty block.
        assert!(PlainBlockIndex::open(serialize_raw(&[0, 3, 4], &[0, 100, 100])).is_err());
    }
}
