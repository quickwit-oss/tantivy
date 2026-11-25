//! Blocks storing their values as is.
//!
//! Used for small blocks, and for blocks that OnPair16 does not make smaller.
//!
//! ```text
//! offset  size          field
//! 0       1             kind = BLOCK_KIND_RAW
//! 1       4             offsets_len
//! 5       offsets_len   byte offsets: BlockwiseLinear, n + 1 entries
//! ..      ..            value bytes, concatenated
//! ```

use std::io;

use common::OwnedBytes;

use super::{BLOCK_KIND_RAW, load_offsets, read_u32, serialize_offsets};
use crate::column_values::{BlockwiseLinearReader, ColumnValues};

pub(super) const RAW_HEADER_NUM_BYTES: usize = 5;

/// Encodes a raw block into `output`, replacing its content.
///
/// `bytes` and `offsets` describe the values of the block, see `encode_block`.
pub(super) fn encode_raw_block(
    bytes: &[u8],
    offsets: &[u32],
    output: &mut Vec<u8>,
) -> io::Result<()> {
    output.clear();
    output.push(BLOCK_KIND_RAW);
    let offsets_len_pos = output.len();
    output.extend_from_slice(&0u32.to_le_bytes());
    let offsets_start = output.len();
    serialize_offsets(offsets, output)?;
    let offsets_len = (output.len() - offsets_start) as u32;
    output[offsets_len_pos..offsets_len_pos + 4].copy_from_slice(&offsets_len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

#[doc(hidden)]
pub struct RawBlock {
    /// `num_values + 1` offsets of the values in `bytes[values_start..]`.
    byte_offsets: BlockwiseLinearReader,
    bytes: OwnedBytes,
    values_start: usize,
}

impl RawBlock {
    /// Opens a raw block holding `num_values` values.
    ///
    /// Precondition: the first byte of `bytes` is `BLOCK_KIND_RAW`.
    /// Panics if `bytes` is corrupted.
    pub(super) fn open(bytes: OwnedBytes, num_values: u32) -> RawBlock {
        let slice = bytes.as_slice();
        assert!(slice.len() >= RAW_HEADER_NUM_BYTES, "truncated raw block");
        // Skip the block kind, checked by `PlainBlock::open`.
        let mut header: &[u8] = &slice[1..RAW_HEADER_NUM_BYTES];
        let offsets_len = read_u32(&mut header) as usize;
        debug_assert!(header.is_empty());
        let values_start = RAW_HEADER_NUM_BYTES
            .checked_add(offsets_len)
            .expect("raw block offsets length overflow");
        assert!(values_start <= slice.len(), "truncated raw block");
        let offsets_bytes = bytes.slice(RAW_HEADER_NUM_BYTES..values_start);
        let byte_offsets = load_offsets(offsets_bytes, num_values + 1);
        RawBlock {
            byte_offsets,
            bytes,
            values_start,
        }
    }

    /// Appends the bytes of the value at position `slot` in the block to `output`.
    pub(super) fn append_value(&self, slot: u32, output: &mut Vec<u8>) {
        let (start, end) = self.byte_offsets.get_pair(slot);
        let value_bytes = &self.bytes.as_slice()[self.values_start..];
        let start = start as usize;
        let end = end as usize;
        assert!(
            start <= end && end <= value_bytes.len(),
            "invalid value offsets in raw plain block"
        );
        output.extend_from_slice(&value_bytes[start..end]);
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{concatenate_values, log_lines};
    use super::*;

    #[test]
    fn test_raw_block_roundtrip() {
        // Large enough for `encode_block` to try OnPair16: the raw encoding is tested directly.
        let values = log_lines(500);
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let (bytes, offsets) = concatenate_values(&value_refs);
        let mut block_bytes = vec![42u8; 3];
        encode_raw_block(&bytes, &offsets, &mut block_bytes).unwrap();
        assert_eq!(block_bytes[0], BLOCK_KIND_RAW);
        let raw_block = RawBlock::open(OwnedBytes::new(block_bytes), values.len() as u32);
        for (slot, expected_value) in values.iter().enumerate() {
            let mut value = Vec::new();
            raw_block.append_value(slot as u32, &mut value);
            assert_eq!(&value, expected_value);
        }
    }
}
