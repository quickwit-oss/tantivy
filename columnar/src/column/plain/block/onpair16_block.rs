//! Blocks compressed with OnPair16.
//!
//! Each block has its own dictionary of at most 2^16 tokens of at most 16 bytes. A value is a
//! sequence of codes, each code being a token of the dictionary.
//!
//! ```text
//! offset  size                                  field
//! 0       1                                     kind = BLOCK_KIND_ONPAIR16
//! 1       2                                     num_tokens - 1
//! 3       4                                     dict_bytes_len (padding included)
//! 7       4                                     code_offsets_len
//! 11      4                                     num_codes
//! 15      ceil(num_tokens / 2)                  token lengths: 4 bits per token, (len - 1),
//!                                               token 2k in the low nibble
//! ..      dict_bytes_len                        token bytes, concatenated, + read padding
//! ..      code_offsets_len                      code offsets: ColumnValues<u32>, n + 1 entries
//! ..      ceil(num_codes * code_bits / 8) + 7   codes, bitpacked (raw u16 when code_bits = 16),
//!                                               followed by 7 zero bytes
//! ```
//!
//! `code_bits` is not stored: it is `ceil(log2(num_tokens))`, the minimal width of a token id.

use std::io;
use std::mem::MaybeUninit;
use std::sync::Arc;

use common::OwnedBytes;
use onpair::{CompactDictionaryView, MaxDictBits, Threshold};
use tantivy_bitpacker::{BitPacker, BitUnpacker};

use super::{
    BLOCK_KIND_ONPAIR16, OFFSETS_CODECS, invalid_data, load_offsets, read_u16, read_u32,
    split_checked,
};
use crate::column_values::{ColumnValues, serialize_u64_based_column_values};

const ONPAIR16_HEADER_NUM_BYTES: usize = 15;

/// Zero bytes appended after the codes, so that the bit unpacker can always do 8-byte loads.
const CODES_PADDING_NUM_BYTES: usize = 7;

/// Returns the OnPair dictionary budget (in bits) for a block of `num_bytes` raw bytes.
///
/// This is called max_dict_bits to follow OnPair's naming, but could have been called
/// `max_code_bits`: it is also the maximum number of bits used to describe a token.
///
/// The dictionary has at most `num_bytes / 256` tokens, between 2^9 (the minimum accepted by
/// onpair) and 2^12. 1 MiB gives 12 bits.
///
/// The size is not necessarily entirely used by OnPair.
fn max_dict_bits_for(num_bytes: usize) -> MaxDictBits {
    let num_bytes_log2 = num_bytes.max(1).ilog2();
    let max_dict_bits = num_bytes_log2.saturating_sub(8).clamp(9, 12) as u8;
    MaxDictBits::new(max_dict_bits).unwrap()
}

/// Appends an OnPair16 block to `output`.
///
/// `bytes` and `offsets` describe the values of the block, see `encode_block`.
pub(super) fn encode_onpair16_block(
    bytes: &[u8],
    offsets: &[u32],
    output: &mut Vec<u8>,
) -> io::Result<()> {
    let max_dict_bits = max_dict_bits_for(bytes.len());
    let config = onpair::Config {
        max_dict_bits,
        threshold: Threshold::new(0.15).unwrap(),
        seed: Some(42),
    };
    let column: onpair::Column<u32> = onpair::compress(bytes, offsets, config)
        .map_err(|onpair_error| io::Error::new(io::ErrorKind::InvalidInput, onpair_error))?;
    let (dictionary, codes, code_offsets) = column.into_raw();
    let num_tokens: usize = dictionary.num_tokens();
    let code_bits: u8 = dictionary.code_bits();
    let dict_offsets: &[u32] = dictionary.offsets();
    let dict_bytes: &[u8] = dictionary.bytes();
    // The header stores `num_tokens - 1` on a u16.
    assert!((1..=16).contains(&code_bits));
    // Codes are token ids, smaller than `num_tokens`: they must fit on `code_bits` bits.
    assert!((1..=(1 << code_bits)).contains(&num_tokens));

    let block_start = output.len();
    output.push(BLOCK_KIND_ONPAIR16);
    output.extend_from_slice(&((num_tokens - 1) as u16).to_le_bytes());
    output.extend_from_slice(&(dict_bytes.len() as u32).to_le_bytes());
    let code_offsets_len_pos = output.len();
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&(codes.len() as u32).to_le_bytes());
    debug_assert_eq!(output.len() - block_start, ONPAIR16_HEADER_NUM_BYTES);

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

impl OnPair16Block {
    /// Opens an OnPair16 block holding `num_values` values.
    ///
    /// Precondition: the first byte of `bytes` is `BLOCK_KIND_ONPAIR16`.
    pub(super) fn open(bytes: OwnedBytes, num_values: u32) -> io::Result<OnPair16Block> {
        let (header, body) = split_checked(bytes, ONPAIR16_HEADER_NUM_BYTES)?;
        // Skip the block kind, checked by `PlainBlock::open`.
        let mut header: &[u8] = &header.as_slice()[1..];
        let num_tokens: usize = read_u16(&mut header) as usize + 1;
        let dict_bytes_len = read_u32(&mut header) as usize;
        let code_offsets_len = read_u32(&mut header) as usize;
        let num_codes = read_u32(&mut header);
        debug_assert!(header.is_empty());
        // The writer uses the minimal width of the token ids, computed by onpair.
        let code_bits = onpair::code_bits_for_num_tokens(num_tokens);
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

    /// Appends the bytes of the value at position `slot` in the block to `output`.
    ///
    /// `codes_buffer` is scratch space.
    pub(super) fn append_value(
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
        codes_buffer.reserve((code_end - code_start) as usize);
        codes_buffer.extend(
            (code_start..code_end)
                .map(|code_pos| self.code_unpacker.get(code_pos, codes_data) as u16),
        );
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
    use super::super::tests::{concatenate_values, log_lines};
    use super::*;

    #[test]
    fn test_max_dict_bits_builds_without_failure() {
        for b in 9..=12 {
            assert!(MaxDictBits::new(b).is_ok());
        }
    }

    #[test]
    fn test_onpair16_block_roundtrip() {
        // Small enough for `encode_block` to store it raw: the OnPair16 encoding is tested
        // directly.
        let values: Vec<Vec<u8>> = vec![b"hello".to_vec(), Vec::new(), b"happy hello".to_vec()];
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let (bytes, offsets) = concatenate_values(&value_refs);
        let mut block_bytes = Vec::new();
        encode_onpair16_block(&bytes, &offsets, &mut block_bytes).unwrap();
        assert_eq!(block_bytes[0], BLOCK_KIND_ONPAIR16);
        let onpair16_block =
            OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32).unwrap();
        let mut codes_buffer = Vec::new();
        for (slot, expected_value) in values.iter().enumerate() {
            let mut value = Vec::new();
            onpair16_block
                .append_value(slot as u32, &mut codes_buffer, &mut value)
                .unwrap();
            assert_eq!(&value, expected_value);
        }
    }

    #[test]
    fn test_onpair16_block_invalid_num_codes() {
        let values = log_lines(100);
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let (bytes, offsets) = concatenate_values(&value_refs);
        let mut block_bytes = Vec::new();
        encode_onpair16_block(&bytes, &offsets, &mut block_bytes).unwrap();
        // `num_codes` no longer matches the length of the codes.
        let num_codes = read_u32(&mut &block_bytes[11..]);
        block_bytes[11..15].copy_from_slice(&(num_codes + 8).to_le_bytes());
        assert!(OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32).is_err());
    }

    #[test]
    fn test_max_dict_bits_for() {
        assert_eq!(max_dict_bits_for(0).value(), 9);
        assert_eq!(max_dict_bits_for(4 << 10).value(), 9);
        assert_eq!(max_dict_bits_for(256 << 10).value(), 10);
        assert_eq!(max_dict_bits_for(1 << 20).value(), 12);
        assert_eq!(max_dict_bits_for(1 << 30).value(), 12);
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
}
