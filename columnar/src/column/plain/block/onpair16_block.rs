//! Blocks compressed with OnPair16.
//!
//! Each block has its own dictionary of at most 2^12 tokens (`MAX_CODE_BITS`) of at most 16
//! bytes. A value is a sequence of codes, each code being a token of the dictionary.
//!
//! ```text
//! offset  size                                  field
//! 0       1                                     kind = BLOCK_KIND_ONPAIR16
//! 1       2                                     num_tokens - 1
//! 3       4                                     dict_bytes_len (padding included)
//! 7       4                                     code_offsets_len
//! 11      4                                     num_codes
//! 15      64 * ceil(num_tokens / 128)           token ends (dict offsets 1..=num_tokens):
//!                                               BitPacker4x strictly sorted, 4 bits, chunks
//!                                               of 128, the last chunk padded
//! ..      dict_bytes_len                        token bytes, concatenated, + read padding
//! ..      code_offsets_len                      code offsets: ColumnValues<u32>, n + 1 entries
//! ..      ceil(num_codes * code_bits / 8) + 7   codes, bitpacked (code_bits <= 12),
//!                                               followed by 7 zero bytes
//! ```
//!
//! `code_bits` is not stored: it is `ceil(log2(num_tokens))`, the minimal width of a token id.

use std::mem::MaybeUninit;
use std::sync::Arc;

use bitpacking::{BitPacker as _, BitPacker4x};
use common::OwnedBytes;
use onpair::{CompactDictionaryView, MaxDictBits, Threshold};
use tantivy_bitpacker::{BitPacker, BitUnpacker};

use super::{BLOCK_KIND_ONPAIR16, OFFSETS_CODECS, load_offsets, read_u16, read_u32};
use crate::column_values::{ColumnValues, serialize_u64_based_column_values};

const ONPAIR16_HEADER_NUM_BYTES: usize = 15;

/// Zero bytes appended after the codes, so that the bit unpacker can always do 8-byte loads.
const CODES_PADDING_NUM_BYTES: usize = 7;

/// Minimum dictionary budget, in bits: the minimum accepted by onpair.
const MIN_CODE_BITS: u8 = 9;

/// Maximum dictionary budget, in bits: a dictionary has at most `2^MAX_CODE_BITS` tokens.
///
/// This is well below the 16 bits onpair16 allows. Onpair16 has no logic to stop growing the
/// dictionary when new tokens no longer pay off: it always fills the whole budget. A larger
/// budget means wider codes and a larger dictionary to decode on every block open. We expect
/// 12 bits to be enough for logs.
///
/// The writer never exceeds it (see `max_dict_bits_for`), and the reader rejects blocks that do.
const MAX_CODE_BITS: u8 = 12;

/// Number of token ends per chunk compressed with `BitPacker4x`.
const TOKEN_ENDS_CHUNK_LEN: usize = BitPacker4x::BLOCK_LEN;

/// Bits per token end, strictly sorted delta encoded: a token length minus 1, in 0..=15.
///
/// With 4 bits, any decoded delta gives a token length in 1..=16, even on corrupted data.
const TOKEN_END_NUM_BITS: u8 = 4;

const TOKEN_ENDS_CHUNK_NUM_BYTES: usize = TOKEN_ENDS_CHUNK_LEN * TOKEN_END_NUM_BITS as usize / 8;

fn token_ends_num_bytes(num_tokens: usize) -> usize {
    num_tokens.div_ceil(TOKEN_ENDS_CHUNK_LEN) * TOKEN_ENDS_CHUNK_NUM_BYTES
}

/// Number of entries of a `DictOffsetsBuffer`: the dict offsets of the largest dictionary,
/// `2^MAX_CODE_BITS` tokens. `2^MAX_CODE_BITS` is a multiple of `TOKEN_ENDS_CHUNK_LEN`, so the
/// padding of the last token ends chunk fits too.
const DICT_OFFSETS_BUFFER_LEN: usize = (1 << MAX_CODE_BITS) + 1;

const _: () = assert!((1usize << MAX_CODE_BITS).is_multiple_of(TOKEN_ENDS_CHUNK_LEN));

/// Buffer receiving the dict offsets of an OnPair16 block, large enough for any block.
///
/// Reused from block to block: see `PlainBlock::open` and `PlainBlock::recycle`.
pub type DictOffsetsBuffer = Box<[u32; DICT_OFFSETS_BUFFER_LEN]>;

fn new_dict_offsets_buffer() -> DictOffsetsBuffer {
    // Built on the heap, without going through the stack.
    vec![0u32; DICT_OFFSETS_BUFFER_LEN]
        .into_boxed_slice()
        .try_into()
        .expect("the length is DICT_OFFSETS_BUFFER_LEN")
}

/// Appends the token ends `dict_offsets[1..]` to `output`.
///
/// Precondition: `dict_offsets[0] == 0` and every token length is in 1..=16.
fn write_token_ends(dict_offsets: &[u32], output: &mut Vec<u8>) {
    let bitpacker = BitPacker4x::new();
    let mut chunk = [0u32; TOKEN_ENDS_CHUNK_LEN];
    let mut previous_end = 0u32;
    for chunk_ends in dict_offsets[1..].chunks(TOKEN_ENDS_CHUNK_LEN) {
        chunk[..chunk_ends.len()].copy_from_slice(chunk_ends);
        let last_end = chunk_ends[chunk_ends.len() - 1];
        // Only the last chunk is padded: consecutive ends, i.e. deltas of 0.
        for (padding_id, padded_end) in chunk[chunk_ends.len()..].iter_mut().enumerate() {
            *padded_end = last_end + 1 + padding_id as u32;
        }
        let chunk_start = output.len();
        output.resize(chunk_start + TOKEN_ENDS_CHUNK_NUM_BYTES, 0u8);
        let num_bytes = bitpacker.compress_strictly_sorted(
            Some(previous_end),
            &chunk,
            &mut output[chunk_start..],
            TOKEN_END_NUM_BITS,
        );
        debug_assert_eq!(num_bytes, TOKEN_ENDS_CHUNK_NUM_BYTES);
        previous_end = last_end;
    }
}

/// Decodes the `num_tokens + 1` dict offsets from the token ends written by `write_token_ends`,
/// into `dict_offsets[..num_tokens + 1]`. The entries after them are overwritten with the
/// padding of the last chunk, or left unchanged.
///
/// Preconditions: `token_ends.len() == token_ends_num_bytes(num_tokens)`, and
/// `dict_offsets.len() >= 1 + 128 * ceil(num_tokens / 128)`.
fn read_dict_offsets(token_ends: &[u8], num_tokens: usize, dict_offsets: &mut [u32]) {
    debug_assert_eq!(token_ends.len(), token_ends_num_bytes(num_tokens));
    debug_assert!(
        dict_offsets.len() > num_tokens.div_ceil(TOKEN_ENDS_CHUNK_LEN) * TOKEN_ENDS_CHUNK_LEN
    );
    dict_offsets[0] = 0;
    let bitpacker = BitPacker4x::new();
    let mut previous_end = 0u32;
    let (compressed_chunks, _) = token_ends.as_chunks::<TOKEN_ENDS_CHUNK_NUM_BYTES>();
    for (chunk_id, compressed_chunk) in compressed_chunks.iter().enumerate() {
        let chunk_start = 1 + chunk_id * TOKEN_ENDS_CHUNK_LEN;
        let chunk_ends = &mut dict_offsets[chunk_start..chunk_start + TOKEN_ENDS_CHUNK_LEN];
        bitpacker.decompress_strictly_sorted(
            Some(previous_end),
            compressed_chunk,
            chunk_ends,
            TOKEN_END_NUM_BITS,
        );
        previous_end = chunk_ends[TOKEN_ENDS_CHUNK_LEN - 1];
    }
}

/// Returns the OnPair dictionary budget (in bits) for a block of `num_bytes` raw bytes.
///
/// This is called max_dict_bits to follow OnPair's naming, but could have been called
/// `max_code_bits`: it is also the maximum number of bits used to describe a token.
///
/// The dictionary has at most `num_bytes / 256` tokens, between `2^MIN_CODE_BITS` and
/// `2^MAX_CODE_BITS`. 1 MiB gives 12 bits.
///
/// The size is not necessarily entirely used by OnPair.
fn max_dict_bits_for(num_bytes: usize) -> MaxDictBits {
    let num_bytes_log2 = num_bytes.max(1).ilog2();
    let max_dict_bits = num_bytes_log2
        .saturating_sub(8)
        .clamp(MIN_CODE_BITS as u32, MAX_CODE_BITS as u32) as u8;
    MaxDictBits::new(max_dict_bits).unwrap()
}

/// Appends an OnPair16 block to `output`.
///
/// `bytes` and `offsets` describe the values of the block, see `encode_block`.
pub(super) fn encode_onpair16_block(bytes: &[u8], offsets: &[u32], output: &mut Vec<u8>) {
    let max_dict_bits = max_dict_bits_for(bytes.len());
    let config = onpair::Config {
        max_dict_bits,
        threshold: Threshold::new(0.15).unwrap(),
        seed: Some(42),
    };
    // onpair only rejects invalid offsets, which breaks the contract of `encode_block`.
    let column: onpair::Column<u32> =
        onpair::compress(bytes, offsets, config).expect("invalid offsets for plain block");
    let (dictionary, codes, code_offsets) = column.into_raw();
    let num_tokens: usize = dictionary.num_tokens();
    let code_bits: u8 = dictionary.code_bits();
    let dict_offsets: &[u32] = dictionary.offsets();
    let dict_bytes: &[u8] = dictionary.bytes();
    // The reader rejects blocks with more than `2^MAX_CODE_BITS` tokens.
    assert!((1..=MAX_CODE_BITS).contains(&code_bits));
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

    write_token_ends(dict_offsets, output);
    output.extend_from_slice(dict_bytes);

    let code_offsets_start = output.len();
    serialize_u64_based_column_values::<u32>(&&code_offsets[..], &OFFSETS_CODECS, output)
        .expect("writing to a Vec cannot fail");
    let code_offsets_len = (output.len() - code_offsets_start) as u32;
    output[code_offsets_len_pos..code_offsets_len_pos + 4]
        .copy_from_slice(&code_offsets_len.to_le_bytes());

    write_codes(&codes, code_bits, output);
}

/// Writes the codes with `code_bits` bits each, followed by `CODES_PADDING_NUM_BYTES` zero bytes.
fn write_codes(codes: &[u16], code_bits: u8, output: &mut Vec<u8>) {
    let mut bit_packer = BitPacker::new();
    for &code in codes {
        bit_packer
            .write(code as u64, code_bits, output)
            .expect("writing to a Vec cannot fail");
    }
    bit_packer
        .close(output)
        .expect("writing to a Vec cannot fail");
    output.extend_from_slice(&[0u8; CODES_PADDING_NUM_BYTES]);
}

fn codes_num_bytes(num_codes: usize, code_bits: u8) -> usize {
    (num_codes * code_bits as usize).div_ceil(8) + CODES_PADDING_NUM_BYTES
}

#[doc(hidden)]
pub struct OnPair16Block {
    /// Token bytes, read padding included.
    dict_bytes: OwnedBytes,
    /// `num_tokens + 1` offsets of the tokens in `dict_bytes`.
    ///
    /// Invariant: `(dict_bytes, dict_offsets)` satisfies the structural invariants of
    /// `CompactDictionaryView` (checked by `open`).
    ///
    /// Only the first `num_tokens + 1` entries are used.
    dict_offsets: DictOffsetsBuffer,
    num_tokens: usize,
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
    ///
    /// Panics if `bytes` is corrupted.
    ///
    /// `dict_offsets_buffer` is a reusable allocation, created if it is `None`. The block takes
    /// it; `into_dict_offsets` gives it back.
    pub(super) fn open(
        bytes: OwnedBytes,
        num_values: u32,
        dict_offsets_buffer: &mut Option<DictOffsetsBuffer>,
    ) -> OnPair16Block {
        // `OwnedBytes::split` panics on a truncated (corrupted) block.
        let (header, body) = bytes.split(ONPAIR16_HEADER_NUM_BYTES);
        // Skip the block kind, checked by `PlainBlock::open`.
        let mut header: &[u8] = &header.as_slice()[1..];
        let num_tokens: usize = read_u16(&mut header) as usize + 1;
        // `DictOffsetsBuffer` holds the offsets of at most `2^MAX_CODE_BITS` tokens.
        assert!(
            num_tokens <= 1 << MAX_CODE_BITS,
            "too many tokens in plain block"
        );
        let dict_bytes_len = read_u32(&mut header) as usize;
        let code_offsets_len = read_u32(&mut header) as usize;
        let num_codes = read_u32(&mut header);
        debug_assert!(header.is_empty());
        // The writer uses the minimal width of the token ids, computed by onpair.
        let code_bits = onpair::code_bits_for_num_tokens(num_tokens);
        let (token_ends, body) = body.split(token_ends_num_bytes(num_tokens));
        let (dict_bytes, body) = body.split(dict_bytes_len);
        let (code_offsets_bytes, codes) = body.split(code_offsets_len);
        assert_eq!(
            codes.len(),
            codes_num_bytes(num_codes as usize, code_bits),
            "invalid codes length in plain block"
        );
        let dict_offsets_storage = dict_offsets_buffer.get_or_insert_with(new_dict_offsets_buffer);
        read_dict_offsets(
            token_ends.as_slice(),
            num_tokens,
            &mut dict_offsets_storage[..],
        );
        let dict_offsets: &[u32] = &dict_offsets_storage[..num_tokens + 1];
        // The decoder reads `MAX_TOKEN_SIZE` bytes from the start of each token: the dictionary
        // bytes must be readable that far after the start of the last token. The other
        // invariants of `CompactDictionaryView` hold by construction: at most 2^12 tokens,
        // `dict_offsets[0] == 0`, and token lengths in 1..=16 (4-bit deltas).
        let last_token_start = dict_offsets[num_tokens - 1] as usize;
        assert!(
            last_token_start + onpair::MAX_TOKEN_SIZE <= dict_bytes.len(),
            "invalid dictionary in plain block"
        );
        debug_assert!(
            CompactDictionaryView::validate_safety(dict_bytes.as_slice(), dict_offsets).is_ok(),
            "the dictionary of the plain block does not pass onpair's safety checks"
        );
        let code_offsets = load_offsets(code_offsets_bytes, num_values + 1);
        OnPair16Block {
            dict_bytes,
            dict_offsets: dict_offsets_buffer
                .take()
                .expect("the buffer was created above if needed"),
            num_tokens,
            code_offsets,
            code_unpacker: BitUnpacker::new(code_bits),
            codes,
            num_codes,
        }
    }

    /// Returns the allocation of the dict offsets, to open another block with it.
    pub(super) fn into_dict_offsets(self) -> DictOffsetsBuffer {
        self.dict_offsets
    }

    fn dictionary_view(&self) -> CompactDictionaryView<'_> {
        // SAFETY: `open` checked the read padding, and the other invariants of the view hold by
        // construction (see `open`). The buffers are never mutated.
        unsafe {
            CompactDictionaryView::new_unchecked(
                self.dict_bytes.as_slice(),
                &self.dict_offsets[..self.num_tokens + 1],
            )
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
    ) {
        let code_start = self.code_offsets.get_val(slot);
        let code_end = self.code_offsets.get_val(slot + 1);
        assert!(
            code_start <= code_end && code_end <= self.num_codes,
            "invalid code offsets in plain block"
        );
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
        encode_onpair16_block(&bytes, &offsets, &mut block_bytes);
        assert_eq!(block_bytes[0], BLOCK_KIND_ONPAIR16);
        let onpair16_block =
            OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32, &mut None);
        let mut codes_buffer = Vec::new();
        for (slot, expected_value) in values.iter().enumerate() {
            let mut value = Vec::new();
            onpair16_block.append_value(slot as u32, &mut codes_buffer, &mut value);
            assert_eq!(&value, expected_value);
        }
    }

    #[test]
    #[should_panic(expected = "invalid codes length")]
    fn test_onpair16_block_invalid_num_codes() {
        let values = log_lines(100);
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let (bytes, offsets) = concatenate_values(&value_refs);
        let mut block_bytes = Vec::new();
        encode_onpair16_block(&bytes, &offsets, &mut block_bytes);
        // `num_codes` no longer matches the length of the codes.
        let num_codes = read_u32(&mut &block_bytes[11..]);
        block_bytes[11..15].copy_from_slice(&(num_codes + 8).to_le_bytes());
        OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32, &mut None);
    }

    #[test]
    fn test_token_ends_roundtrip() {
        for num_tokens in [1usize, 2, 127, 128, 129, 256, 300, 4096] {
            let mut dict_offsets: Vec<u32> = vec![0];
            for token_id in 0..num_tokens {
                // Token lengths cycle through 1..=16.
                let token_len = (token_id % 16) as u32 + 1;
                dict_offsets.push(dict_offsets[token_id] + token_len);
            }
            let mut token_ends = Vec::new();
            write_token_ends(&dict_offsets, &mut token_ends);
            assert_eq!(token_ends.len(), token_ends_num_bytes(num_tokens));
            let mut decoded_dict_offsets = new_dict_offsets_buffer();
            read_dict_offsets(&token_ends, num_tokens, &mut decoded_dict_offsets[..]);
            assert_eq!(&decoded_dict_offsets[..num_tokens + 1], &dict_offsets[..]);
        }
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
            write_codes(&codes, code_bits, &mut output);
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
