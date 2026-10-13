//! Encoding and decoding of the blocks of a plain-encoded column.
//!
//! A block is self-contained: it can be decoded without reading any other block.
//! All integers are little endian.
//!
//! The first byte of a block is its kind:
//! - `BLOCK_KIND_RAW`: values stored as is, see `raw_block`.
//! - `BLOCK_KIND_ONPAIR16`: values compressed with OnPair16, see `onpair16_block`.

mod onpair16_block;
mod raw_block;

use std::io;
use std::sync::Arc;

use common::OwnedBytes;

use self::onpair16_block::{OnPair16Block, encode_onpair16_block};
use self::raw_block::{RAW_HEADER_NUM_BYTES, RawBlock, encode_raw_block};
use crate::column_values::{CodecType, ColumnValues, load_u64_based_column_values};

/// Kind of a block storing its values as is.
const BLOCK_KIND_RAW: u8 = 0;
/// Kind of a block compressed with OnPair16.
const BLOCK_KIND_ONPAIR16: u8 = 1;

/// Blocks with fewer raw bytes than this are stored raw: an OnPair dictionary always contains the
/// 256 single-byte tokens, which does not pay off on small blocks.
const MIN_ONPAIR16_BLOCK_NUM_BYTES: usize = 4 << 10;

/// Codecs considered for the code offsets and the byte offsets of a block.
const OFFSETS_CODECS: [CodecType; 2] = [CodecType::Bitpacked, CodecType::BlockwiseLinear];

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Reads a little endian `u16` from the front of `bytes`, and advances `bytes` past it.
///
/// Precondition: `bytes` holds at least 2 bytes. Callers check the header length with
/// `split_checked` first.
fn read_u16(bytes: &mut &[u8]) -> u16 {
    let (u16_bytes, rest) = bytes
        .split_first_chunk::<2>()
        .expect("not enough bytes to read a u16");
    *bytes = rest;
    u16::from_le_bytes(*u16_bytes)
}

/// Reads a little endian `u32` from the front of `bytes`, and advances `bytes` past it.
///
/// Precondition: `bytes` holds at least 4 bytes. Callers check the header length with
/// `split_checked` first.
fn read_u32(bytes: &mut &[u8]) -> u32 {
    let (u32_bytes, rest) = bytes
        .split_first_chunk::<4>()
        .expect("not enough bytes to read a u32");
    *bytes = rest;
    u32::from_le_bytes(*u32_bytes)
}

/// Splits `len` bytes off the front of `bytes`, or returns an error if there are not enough.
fn split_checked(bytes: OwnedBytes, len: usize) -> io::Result<(OwnedBytes, OwnedBytes)> {
    if bytes.len() < len {
        return Err(invalid_data("truncated plain block"));
    }
    Ok(bytes.split(len))
}

/// Loads `ColumnValues<u32>` with `expected_num_vals` values.
fn load_offsets(
    bytes: OwnedBytes,
    expected_num_vals: u32,
) -> io::Result<Arc<dyn ColumnValues<u32>>> {
    let offsets: Arc<dyn ColumnValues<u32>> = load_u64_based_column_values::<u32>(bytes)?;
    if offsets.num_vals() != expected_num_vals {
        return Err(invalid_data("unexpected number of offsets in plain block"));
    }
    Ok(offsets)
}

/// Encodes the values of a block into `output`.
///
/// `bytes` holds the `n` values of the block, concatenated. `offsets` has `n + 1` entries, with
/// `offsets[0] == 0`: value `i` is `bytes[offsets[i]..offsets[i + 1]]`.
///
/// The block is compressed with OnPair16, unless it is small or OnPair16 does not make it
/// smaller. In that case, it is stored raw.
pub(crate) fn encode_block(
    bytes: &[u8],
    offsets: &[u32],
    output: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    debug_assert_eq!(offsets.first().copied(), Some(0));
    debug_assert_eq!(offsets.last().copied(), Some(bytes.len() as u32));
    output.clear();
    if bytes.len() < MIN_ONPAIR16_BLOCK_NUM_BYTES {
        return encode_raw_block(bytes, offsets, output);
    }
    encode_onpair16_block(bytes, offsets, output)?;
    if output.len() <= bytes.len() + RAW_HEADER_NUM_BYTES {
        // A raw block is at least that large.
        return Ok(());
    }
    encode_raw_block(bytes, offsets, scratch)?;
    if scratch.len() < output.len() {
        std::mem::swap(output, scratch);
    }
    Ok(())
}

/// The decoded metadata of a block. All of the buffers are zero-copy views on the block bytes,
/// except the dictionary offsets.
pub(crate) enum PlainBlock {
    OnPair16(OnPair16Block),
    Raw(RawBlock),
}

impl PlainBlock {
    /// Opens a block holding `num_values` values.
    pub(crate) fn open(bytes: OwnedBytes, num_values: u32) -> io::Result<PlainBlock> {
        let Some(&kind) = bytes.as_slice().first() else {
            return Err(invalid_data("empty plain block"));
        };
        match kind {
            BLOCK_KIND_RAW => Ok(PlainBlock::Raw(RawBlock::open(bytes, num_values)?)),
            BLOCK_KIND_ONPAIR16 => Ok(PlainBlock::OnPair16(OnPair16Block::open(
                bytes, num_values,
            )?)),
            _ => Err(invalid_data("unknown plain block kind")),
        }
    }

    /// Appends the bytes of the value at position `slot` in the block to `output`.
    ///
    /// `codes_buffer` is scratch space.
    pub(crate) fn append_value(
        &self,
        slot: u32,
        codes_buffer: &mut Vec<u16>,
        output: &mut Vec<u8>,
    ) -> io::Result<()> {
        match self {
            PlainBlock::OnPair16(onpair16_block) => {
                onpair16_block.append_value(slot, codes_buffer, output)
            }
            PlainBlock::Raw(raw_block) => raw_block.append_value(slot, output),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn concatenate_values(values: &[&[u8]]) -> (Vec<u8>, Vec<u32>) {
        let mut bytes = Vec::new();
        let mut offsets = vec![0u32];
        for value in values {
            bytes.extend_from_slice(value);
            offsets.push(bytes.len() as u32);
        }
        (bytes, offsets)
    }

    pub(super) fn log_lines(num_lines: usize) -> Vec<Vec<u8>> {
        (0..num_lines)
            .map(|i| {
                format!(
                    "2026-10-12T10:{:02}:{:02}Z INFO request handled path=/api/v1/items/{} \
                     status={} duration_ms={}",
                    (i / 60) % 60,
                    i % 60,
                    i * 7 % 1000,
                    [200, 404, 500][i % 3],
                    i % 97
                )
                .into_bytes()
            })
            .collect()
    }

    fn encode(values: &[&[u8]]) -> Vec<u8> {
        let (bytes, offsets) = concatenate_values(values);
        let mut output = Vec::new();
        let mut scratch = Vec::new();
        encode_block(&bytes, &offsets, &mut output, &mut scratch).unwrap();
        output
    }

    fn decode_all(block_bytes: Vec<u8>, num_values: usize) -> Vec<Vec<u8>> {
        let block = PlainBlock::open(OwnedBytes::new(block_bytes), num_values as u32).unwrap();
        let mut codes_buffer = Vec::new();
        (0..num_values as u32)
            .map(|slot| {
                let mut value = Vec::new();
                block
                    .append_value(slot, &mut codes_buffer, &mut value)
                    .unwrap();
                value
            })
            .collect()
    }

    #[track_caller]
    fn test_roundtrip_aux(values: &[Vec<u8>], expected_kind: u8) {
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let block_bytes = encode(&value_refs);
        assert_eq!(block_bytes[0], expected_kind);
        assert_eq!(decode_all(block_bytes, values.len()), values);
    }

    #[test]
    fn test_small_block_is_raw() {
        let values: Vec<Vec<u8>> = vec![b"hello".to_vec(), b"".to_vec(), b"happy".to_vec()];
        test_roundtrip_aux(&values, BLOCK_KIND_RAW);
    }

    #[test]
    fn test_single_empty_value() {
        test_roundtrip_aux(&[Vec::new()], BLOCK_KIND_RAW);
    }

    #[test]
    fn test_compressible_block_is_onpair16() {
        let values = log_lines(2_000);
        let raw_num_bytes: usize = values.iter().map(Vec::len).sum();
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let block_bytes = encode(&value_refs);
        assert_eq!(block_bytes[0], BLOCK_KIND_ONPAIR16);
        assert!(block_bytes.len() < raw_num_bytes);
        assert_eq!(decode_all(block_bytes, values.len()), values);
    }

    #[test]
    fn test_onpair16_block_with_empty_values() {
        let mut values = log_lines(500);
        values.insert(0, Vec::new());
        values.insert(100, Vec::new());
        values.push(Vec::new());
        test_roundtrip_aux(&values, BLOCK_KIND_ONPAIR16);
    }

    #[test]
    fn test_incompressible_block_is_raw() {
        // Pseudo random bytes do not compress.
        let mut state = 42u64;
        let values: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                (0..100)
                    .map(|_| {
                        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        (state >> 56) as u8
                    })
                    .collect()
            })
            .collect();
        test_roundtrip_aux(&values, BLOCK_KIND_RAW);
    }

    #[test]
    fn test_truncated_block_is_an_error() {
        let values = log_lines(500);
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let block_bytes = encode(&value_refs);
        for truncated_len in [0, 1, 10, 16, 100, block_bytes.len() - 1] {
            let truncated = OwnedBytes::new(block_bytes[..truncated_len].to_vec());
            assert!(PlainBlock::open(truncated, values.len() as u32).is_err());
        }
    }

    #[test]
    fn test_wrong_num_values_is_an_error() {
        let values = log_lines(500);
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let block_bytes = OwnedBytes::new(encode(&value_refs));
        assert!(PlainBlock::open(block_bytes, values.len() as u32 + 1).is_err());
    }

    #[test]
    fn test_unknown_block_kind_is_an_error() {
        let mut block_bytes = encode(&[b"abc".as_slice()]);
        block_bytes[0] = 7;
        assert!(PlainBlock::open(OwnedBytes::new(block_bytes), 1).is_err());
    }
}
