//! Encoding and decoding of the blocks of a plain-encoded column.
//!
//! A block is self-contained: it can be decoded without reading any other block.
//! All integers are little endian.
//!
//! # OnPair16 block
//!
//! ```text
//! offset  size                                  field
//! 0       1                                     kind = BLOCK_KIND_ONPAIR16
//! 1       1                                     code_bits = ceil(log2(num_tokens))
//! 2       2                                     num_tokens - 1
//! 4       4                                     dict_bytes_len (padding included)
//! 8       4                                     code_offsets_len
//! 12      4                                     num_codes
//! 16      ceil(num_tokens / 2)                  token lengths: 4 bits per token, (len - 1),
//!                                               token 2k in the low nibble
//! ..      dict_bytes_len                        token bytes, concatenated, + read padding
//! ..      code_offsets_len                      code offsets: ColumnValues<u32>, n + 1 entries
//! ..      ceil(num_codes * code_bits / 8) + 7   codes, bitpacked (raw u16 when code_bits = 16),
//!                                               followed by 7 zero bytes
//! ```
//!
//! # Raw block
//!
//! ```text
//! offset  size          field
//! 0       1             kind = BLOCK_KIND_RAW
//! 1       3             reserved (zero)
//! 4       4             offsets_len
//! 8       offsets_len   byte offsets: ColumnValues<u32>, n + 1 entries
//! ..      ..            value bytes, concatenated
//! ```

use std::io;
use std::mem::MaybeUninit;
use std::sync::Arc;

use common::OwnedBytes;
use onpair::{CompactDictionaryView, MaxDictBits};
use tantivy_bitpacker::{BitPacker, BitUnpacker};

use crate::column_values::{
    CodecType, ColumnValues, load_u64_based_column_values, serialize_u64_based_column_values,
};

/// Kind of a block storing its values as is.
const BLOCK_KIND_RAW: u8 = 0;
/// Kind of a block compressed with OnPair16.
const BLOCK_KIND_ONPAIR16: u8 = 1;

const ONPAIR16_HEADER_NUM_BYTES: usize = 16;
const RAW_HEADER_NUM_BYTES: usize = 8;

/// Zero bytes appended after the codes, so that the bit unpacker can always do 8-byte loads.
const CODES_PADDING_NUM_BYTES: usize = 7;

/// Blocks with fewer raw bytes than this are stored raw: an OnPair dictionary always contains the
/// 256 single-byte tokens, which does not pay off on small blocks.
const MIN_ONPAIR16_BLOCK_NUM_BYTES: usize = 4 << 10;

/// Codecs considered for the code offsets and the byte offsets of a block.
const OFFSETS_CODECS: [CodecType; 2] = [CodecType::Bitpacked, CodecType::BlockwiseLinear];

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Returns the OnPair dictionary budget (in bits) for a block of `num_bytes` raw bytes.
///
/// The dictionary has at most `num_bytes / 256` tokens, between 2^9 (the minimum accepted by
/// onpair) and 2^12. 1 MiB gives 12 bits.
fn max_dict_bits_for(num_bytes: usize) -> u8 {
    let num_bytes_log2 = num_bytes.max(1).ilog2();
    num_bytes_log2.saturating_sub(8).clamp(9, 12) as u8
}

/// Encodes the values of a block into `output`.
///
/// `offsets` follows the Arrow layout: it has `n + 1` entries, `offsets[0] == 0`, and value `i`
/// is `bytes[offsets[i]..offsets[i + 1]]`.
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

fn encode_raw_block(bytes: &[u8], offsets: &[u32], output: &mut Vec<u8>) -> io::Result<()> {
    output.clear();
    output.push(BLOCK_KIND_RAW);
    output.extend_from_slice(&[0u8; 3]);
    let offsets_len_pos = output.len();
    output.extend_from_slice(&0u32.to_le_bytes());
    let offsets_start = output.len();
    serialize_u64_based_column_values::<u32>(&offsets, &OFFSETS_CODECS, output)?;
    let offsets_len = (output.len() - offsets_start) as u32;
    output[offsets_len_pos..offsets_len_pos + 4].copy_from_slice(&offsets_len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_onpair16_block(bytes: &[u8], offsets: &[u32], output: &mut Vec<u8>) -> io::Result<()> {
    let max_dict_bits = MaxDictBits::new(max_dict_bits_for(bytes.len()))
        .map_err(|_| io::Error::other("invalid onpair dictionary budget"))?;
    let config = onpair::Config {
        max_dict_bits,
        // The default config has a fixed seed: the encoding is deterministic.
        ..onpair::DEFAULT_CONFIG
    };
    let column: onpair::Column<u32> = onpair::compress(bytes, offsets, config)
        .map_err(|onpair_error| io::Error::new(io::ErrorKind::InvalidInput, onpair_error))?;
    let (dictionary, codes, code_offsets) = column.into_raw();
    let num_tokens = dictionary.num_tokens();
    let code_bits = dictionary.code_bits();
    let dict_offsets: &[u32] = dictionary.offsets();
    let dict_bytes: &[u8] = dictionary.bytes();
    // A dictionary holds between 1 and 2^16 tokens of 1 to 16 bytes.
    assert!((1..=1 << 16).contains(&num_tokens));
    assert!((1..=16).contains(&code_bits));

    output.push(BLOCK_KIND_ONPAIR16);
    output.push(code_bits);
    output.extend_from_slice(&((num_tokens - 1) as u16).to_le_bytes());
    output.extend_from_slice(&(dict_bytes.len() as u32).to_le_bytes());
    let code_offsets_len_pos = output.len();
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&(codes.len() as u32).to_le_bytes());
    debug_assert_eq!(output.len(), ONPAIR16_HEADER_NUM_BYTES);

    // Token lengths, 4 bits each.
    let token_lens_start = output.len();
    output.resize(token_lens_start + num_tokens.div_ceil(2), 0u8);
    for token_id in 0..num_tokens {
        let token_len = dict_offsets[token_id + 1] - dict_offsets[token_id];
        debug_assert!((1..=16).contains(&token_len));
        output[token_lens_start + token_id / 2] |= ((token_len - 1) as u8) << (4 * (token_id % 2));
    }
    output.extend_from_slice(dict_bytes);

    let code_offsets_start = output.len();
    serialize_u64_based_column_values::<u32>(&&code_offsets[..], &OFFSETS_CODECS, output)?;
    let code_offsets_len = (output.len() - code_offsets_start) as u32;
    output[code_offsets_len_pos..code_offsets_len_pos + 4]
        .copy_from_slice(&code_offsets_len.to_le_bytes());

    write_codes(&codes, code_bits, output)?;
    Ok(())
}

/// Writes the codes with `code_bits` bits each, followed by `CODES_PADDING_NUM_BYTES` zero bytes.
///
/// At 16 bits, the bitpacked layout is exactly raw little endian `u16`.
fn write_codes(codes: &[u16], code_bits: u8, output: &mut Vec<u8>) -> io::Result<()> {
    if code_bits == 16 {
        output.reserve(codes.len() * 2 + CODES_PADDING_NUM_BYTES);
        for code in codes {
            output.extend_from_slice(&code.to_le_bytes());
        }
    } else {
        let mut bit_packer = BitPacker::new();
        for &code in codes {
            bit_packer.write(code as u64, code_bits, output)?;
        }
        bit_packer.close(output)?;
    }
    output.extend_from_slice(&[0u8; CODES_PADDING_NUM_BYTES]);
    Ok(())
}

fn codes_num_bytes(num_codes: usize, code_bits: u8) -> usize {
    (num_codes * code_bits as usize).div_ceil(8) + CODES_PADDING_NUM_BYTES
}

/// The decoded metadata of a block. All of the buffers are zero-copy views on the block bytes,
/// except the dictionary offsets.
pub(crate) enum PlainBlock {
    OnPair16(OnPair16Block),
    Raw(RawBlock),
}

pub(crate) struct OnPair16Block {
    /// Token bytes, read padding included.
    dict_bytes: OwnedBytes,
    /// `num_tokens + 1` offsets of the tokens in `dict_bytes`.
    ///
    /// Invariant: `(dict_bytes, dict_offsets)` passed `CompactDictionaryView::validate_safety`.
    dict_offsets: Vec<u32>,
    /// `num_values + 1` offsets of the values in the code stream.
    code_offsets: Arc<dyn ColumnValues<u32>>,
    code_unpacker: BitUnpacker,
    /// Bitpacked codes, followed by `CODES_PADDING_NUM_BYTES` zero bytes.
    codes: OwnedBytes,
    num_codes: u32,
}

pub(crate) struct RawBlock {
    /// `num_values + 1` offsets of the values in `bytes`.
    byte_offsets: Arc<dyn ColumnValues<u32>>,
    bytes: OwnedBytes,
}

fn read_u32(bytes: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap())
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

impl RawBlock {
    fn open(bytes: OwnedBytes, num_values: u32) -> io::Result<RawBlock> {
        let (header, body) = split_checked(bytes, RAW_HEADER_NUM_BYTES)?;
        let offsets_len = read_u32(header.as_slice(), 4) as usize;
        let (offsets_bytes, value_bytes) = split_checked(body, offsets_len)?;
        let byte_offsets = load_offsets(offsets_bytes, num_values + 1)?;
        Ok(RawBlock {
            byte_offsets,
            bytes: value_bytes,
        })
    }

    fn append_value(&self, slot: u32, output: &mut Vec<u8>) -> io::Result<()> {
        let start = self.byte_offsets.get_val(slot) as usize;
        let end = self.byte_offsets.get_val(slot + 1) as usize;
        if start > end || end > self.bytes.len() {
            return Err(invalid_data("invalid value offsets in raw plain block"));
        }
        output.extend_from_slice(&self.bytes.as_slice()[start..end]);
        Ok(())
    }
}

impl OnPair16Block {
    fn open(bytes: OwnedBytes, num_values: u32) -> io::Result<OnPair16Block> {
        let (header, body) = split_checked(bytes, ONPAIR16_HEADER_NUM_BYTES)?;
        let header = header.as_slice();
        let code_bits = header[1];
        let num_tokens = u16::from_le_bytes([header[2], header[3]]) as usize + 1;
        let dict_bytes_len = read_u32(header, 4) as usize;
        let code_offsets_len = read_u32(header, 8) as usize;
        let num_codes = read_u32(header, 12);
        if code_bits != onpair::code_bits_for_num_tokens(num_tokens) {
            return Err(invalid_data("invalid code bits in plain block"));
        }
        let (token_lens, body) = split_checked(body, num_tokens.div_ceil(2))?;
        let (dict_bytes, body) = split_checked(body, dict_bytes_len)?;
        let (code_offsets_bytes, codes) = split_checked(body, code_offsets_len)?;
        if codes.len() != codes_num_bytes(num_codes as usize, code_bits) {
            return Err(invalid_data("invalid codes length in plain block"));
        }
        let mut dict_offsets: Vec<u32> = Vec::with_capacity(num_tokens + 1);
        dict_offsets.push(0u32);
        let mut dict_offset = 0u32;
        for token_id in 0..num_tokens {
            let nibbles = token_lens.as_slice()[token_id / 2];
            let token_len = ((nibbles >> (4 * (token_id % 2))) & 15u8) as u32 + 1;
            dict_offset += token_len;
            dict_offsets.push(dict_offset);
        }
        CompactDictionaryView::validate_safety(dict_bytes.as_slice(), &dict_offsets)
            .map_err(|_| invalid_data("invalid dictionary in plain block"))?;
        let code_offsets = load_offsets(code_offsets_bytes, num_values + 1)?;
        Ok(OnPair16Block {
            dict_bytes,
            dict_offsets,
            code_offsets,
            code_unpacker: BitUnpacker::new(code_bits),
            codes,
            num_codes,
        })
    }

    fn dictionary_view(&self) -> CompactDictionaryView<'_> {
        // SAFETY: the buffers passed `validate_safety` in `open`, and they are never mutated.
        unsafe {
            CompactDictionaryView::new_unchecked(self.dict_bytes.as_slice(), &self.dict_offsets)
        }
    }

    fn append_value(
        &self,
        slot: u32,
        codes_buffer: &mut Vec<u16>,
        output: &mut Vec<u8>,
    ) -> io::Result<()> {
        let code_start = self.code_offsets.get_val(slot);
        let code_end = self.code_offsets.get_val(slot + 1);
        if code_start > code_end || code_end > self.num_codes {
            return Err(invalid_data("invalid code offsets in plain block"));
        }
        codes_buffer.clear();
        let codes_data = self.codes.as_slice();
        if self.code_unpacker.bit_width() == 16 {
            let codes_bytes = &codes_data[code_start as usize * 2..code_end as usize * 2];
            let (code_byte_pairs, _) = codes_bytes.as_chunks::<2>();
            codes_buffer.extend(code_byte_pairs.iter().copied().map(u16::from_le_bytes));
        } else {
            codes_buffer.extend(
                (code_start..code_end)
                    .map(|code_pos| self.code_unpacker.get(code_pos, codes_data) as u16),
            );
        }
        let dictionary = self.dictionary_view();
        // Panics if a code is out of the dictionary range: corrupted data never causes UB.
        let decoded_len = onpair::decoded_len(codes_buffer, dictionary);
        output.reserve(decoded_len + onpair::DECODE_PADDING);
        let spare_capacity: &mut [MaybeUninit<u8>] = output.spare_capacity_mut();
        // SAFETY: `spare_capacity` holds at least `decoded_len + DECODE_PADDING` bytes.
        let num_written = unsafe { onpair::decode_into(codes_buffer, dictionary, spare_capacity) };
        debug_assert_eq!(num_written, decoded_len);
        // SAFETY: `decode_into` initialized the first `num_written` bytes of the spare capacity.
        unsafe { output.set_len(output.len() + num_written) };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arrow_layout(values: &[&[u8]]) -> (Vec<u8>, Vec<u32>) {
        let mut bytes = Vec::new();
        let mut offsets = vec![0u32];
        for value in values {
            bytes.extend_from_slice(value);
            offsets.push(bytes.len() as u32);
        }
        (bytes, offsets)
    }

    fn encode(values: &[&[u8]]) -> Vec<u8> {
        let (bytes, offsets) = arrow_layout(values);
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

    fn log_lines(num_lines: usize) -> Vec<Vec<u8>> {
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
    fn test_max_dict_bits_for() {
        assert_eq!(max_dict_bits_for(0), 9);
        assert_eq!(max_dict_bits_for(4 << 10), 9);
        assert_eq!(max_dict_bits_for(256 << 10), 10);
        assert_eq!(max_dict_bits_for(1 << 20), 12);
        assert_eq!(max_dict_bits_for(1 << 30), 12);
    }

    #[test]
    fn test_codes_roundtrip_all_code_bits() {
        for code_bits in 1u8..=16 {
            let max_code = (1u32 << code_bits) - 1;
            let codes: Vec<u16> = (0..1_000u32)
                .map(|i| (i.wrapping_mul(2_654_435_761) % (max_code + 1)) as u16)
                .collect();
            let mut output = Vec::new();
            write_codes(&codes, code_bits, &mut output).unwrap();
            assert_eq!(output.len(), codes_num_bytes(codes.len(), code_bits));
            let bit_unpacker = BitUnpacker::new(code_bits);
            for (code_pos, &code) in codes.iter().enumerate() {
                assert_eq!(bit_unpacker.get(code_pos as u32, &output) as u16, code);
            }
            if code_bits == 16 {
                for (code_pos, &code) in codes.iter().enumerate() {
                    let code_bytes = [output[code_pos * 2], output[code_pos * 2 + 1]];
                    assert_eq!(u16::from_le_bytes(code_bytes), code);
                }
            }
        }
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
