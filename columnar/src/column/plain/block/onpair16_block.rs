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
//! 15      ceil(num_tokens / 2)                  token lengths: 4 bits per token, (len - 1),
//!                                               token 2k in the low nibble
//! ..      dict_bytes_len                        token bytes, concatenated, + read padding
//! ..      code_offsets_len                      code offsets: BlockwiseLinear, n + 1 entries
//! ..      ceil(num_codes * code_bits / 8) + 7   codes, bitpacked on code_bits bits,
//!                                               followed by 7 zero bytes
//! ```
//!
//! `code_bits` is not stored: it is `ceil(log2(num_tokens))`, the minimal width of a token id.

use std::mem::MaybeUninit;
use std::ops::Range;

use common::OwnedBytes;
use onpair::{MaxDictBits, Threshold};
use tantivy_bitpacker::BitPacker;

use super::offsets::{BlockOffsets, serialize_offsets};
use super::{BLOCK_KIND_ONPAIR16, read_u16, read_u32};

const ONPAIR16_HEADER_NUM_BYTES: usize = 15;

/// Zero bytes appended after the codes, so that `load_codes` can always do 8-byte loads.
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

/// Maximum number of codes decoded in one chunk: `append_value` reserves
/// `DECODE_CHUNK_CODES * MAX_TOKEN_SIZE` = 4 KiB at a time, even for very large values.
const DECODE_CHUNK_CODES: usize = 256;

/// Location of a token in the dictionary bytes. See `decode_dict_entries`.
///
/// Layout contract: same size and alignment as a `u32`, `start` at byte offset 0 and `len` at
/// byte offset 2. The SIMD decoders rely on it: they store interleaved `(start, len)` `u16` lanes
/// directly into a `[DictEntry]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C, align(4))]
struct DictEntry {
    /// Offset of the token in the dictionary bytes.
    start: u16,
    /// Length of the token, in `1..=MAX_TOKEN_SIZE`.
    len: u16,
}

const _: () = {
    assert!(size_of::<DictEntry>() == size_of::<u32>());
    assert!(align_of::<DictEntry>() == align_of::<u32>());
    assert!(std::mem::offset_of!(DictEntry, start) == 0);
    assert!(std::mem::offset_of!(DictEntry, len) == 2);
};

/// Decodes the 4-bit token lengths into one `DictEntry` per token.
///
/// A token start fits on 16 bits: there are at most `2^MAX_CODE_BITS = 4096` tokens of at most 16
/// bytes, so the last token starts at most at `4095 * 16 = 65_520`.
///
/// Precondition: `1 <= num_tokens <= 2^MAX_CODE_BITS` and
/// `token_lens.len() == num_tokens.div_ceil(2)`.
fn decode_dict_entries(token_lens: &[u8], num_tokens: usize) -> Vec<DictEntry> {
    debug_assert!((1..=1 << MAX_CODE_BITS).contains(&num_tokens));
    debug_assert_eq!(token_lens.len(), num_tokens.div_ceil(2));
    let num_slots = token_lens.len() * 2;
    let mut dict_entries: Vec<DictEntry> = Vec::with_capacity(num_slots);
    let spare = &mut dict_entries.spare_capacity_mut()[..num_slots];
    // SIMD for the first `8 * k` bytes, if available, then scalar for the rest.
    let (num_simd_bytes, token_start) = decode_dict_entries_simd(token_lens, spare);
    decode_dict_entries_scalar_uninit(
        &token_lens[num_simd_bytes..],
        &mut spare[num_simd_bytes * 2..],
        token_start,
    );
    // SAFETY: `decode_dict_entries_simd` initialized `spare[..num_simd_bytes * 2]` and
    // `decode_dict_entries_scalar_uninit` initialized `spare[num_simd_bytes * 2..num_slots]`.
    // Since `num_tokens <= num_slots`, the first `num_tokens` entries are initialized.
    // If `num_tokens` is odd, the last high nibble is padding and is left out.
    unsafe {
        dict_entries.set_len(num_tokens);
    }
    dict_entries
}

/// Decodes the entries of the tokens of `token_lens` (2 tokens per byte) into uninitialized
/// memory, the first token starting at `token_start`.
///
/// Precondition: `dict_entries.len() == 2 * token_lens.len()`.
fn decode_dict_entries_scalar_uninit(
    token_lens: &[u8],
    dict_entries: &mut [MaybeUninit<DictEntry>],
    mut token_start: u32,
) {
    debug_assert_eq!(dict_entries.len(), 2 * token_lens.len());
    let (entry_pairs, _) = dict_entries.as_chunks_mut::<2>();
    // Token 2k is in the low nibble.
    // `token_start` is a `u32`: after the last pair of a 4096-token dictionary, it reaches 65_536.
    // The stored starts are at most 65_520 (see `decode_dict_entries`), the dropped padding entry
    // included, so the `as u16` casts are lossless.
    for (&nibbles, entry_pair) in token_lens.iter().zip(entry_pairs) {
        let low_len = (nibbles & 0x0F) as u32 + 1;
        let high_len = (nibbles >> 4) as u32 + 1;
        entry_pair[0].write(DictEntry {
            start: token_start as u16,
            len: low_len as u16,
        });
        entry_pair[1].write(DictEntry {
            start: (token_start + low_len) as u16,
            len: high_len as u16,
        });
        // One addition per pair of tokens on the dependency chain.
        token_start += low_len + high_len;
    }
}

#[cfg(test)]
fn decode_dict_entries_scalar(token_lens: &[u8], dict_entries: &mut [DictEntry], token_start: u32) {
    // SAFETY: `MaybeUninit<DictEntry>` has the same layout as `DictEntry`, and
    // `decode_dict_entries_scalar_uninit` only writes initialized `DictEntry` values.
    let uninit = unsafe {
        std::slice::from_raw_parts_mut(
            dict_entries.as_mut_ptr().cast::<MaybeUninit<DictEntry>>(),
            dict_entries.len(),
        )
    };
    decode_dict_entries_scalar_uninit(token_lens, uninit, token_start);
}

/// Decodes the entries of the tokens of the first `8 * k` bytes of `token_lens` (16 tokens per 8
/// bytes) with SIMD, `k` being as large as possible.
///
/// Returns the number of bytes of `token_lens` decoded, and the start of the next token. Returns
/// `(0, 0)` on targets without SIMD support.
///
/// Precondition: `dict_entries.len() == 2 * token_lens.len()`.
fn decode_dict_entries_simd(
    token_lens: &[u8],
    dict_entries: &mut [MaybeUninit<DictEntry>],
) -> (usize, u32) {
    debug_assert_eq!(dict_entries.len(), 2 * token_lens.len());
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        // SAFETY: NEON is always available on aarch64.
        return unsafe { decode_dict_entries_neon(token_lens, dict_entries) };
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
    {
        // SAFETY: SSE2 is enabled at compile time (`target_feature = "sse2"`).
        return unsafe { decode_dict_entries_sse2(token_lens, dict_entries) };
    }
    #[allow(unreachable_code)]
    (0, 0)
}

/// NEON version of `decode_dict_entries_simd`.
///
/// Processes 16 bytes (32 tokens = two 16-token `u8` vectors) per iteration, then an optional
/// 8-byte (16-token) tail.
///
/// Because each token length is in `1..=16`, the exclusive prefix sum of 16 tokens is at most
/// `15 * 16 = 240 <= 255`, so it is computed directly in `uint8x16_t` without overflow. The
/// running offset stays in a NEON `uint16x8_t` register to avoid SIMD-to-GPR transfers, with a
/// single vector add on the loop-carried dependency chain per 32 tokens.
///
/// If all 4096 tokens have length 16, `running_start` wraps to `0` after the final chunk, which
/// is harmless because no scalar tail remains.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
unsafe fn decode_dict_entries_neon(
    token_lens: &[u8],
    dict_entries: &mut [MaybeUninit<DictEntry>],
) -> (usize, u32) {
    use std::arch::aarch64::*;

    /// Exclusive prefix sum of 16 token lengths in `1..=16` (max lane value `15 * 16 = 240`).
    #[inline(always)]
    unsafe fn excl_prefix_sum_u8x16(lens: uint8x16_t, zeros_u8: uint8x16_t) -> uint8x16_t {
        unsafe {
            // Sliding 8-element inclusive sum (max `8 * 16 = 128`).
            let sum = vaddq_u8(lens, vextq_u8::<15>(zeros_u8, lens));
            let sum = vaddq_u8(sum, vextq_u8::<14>(zeros_u8, sum));
            let sum = vaddq_u8(sum, vextq_u8::<12>(zeros_u8, sum));
            // Shift right by 1 to make it exclusive, then add lane `k - 8` (max `15 * 16 = 240`).
            let shifted = vextq_u8::<15>(zeros_u8, sum);
            vaddq_u8(shifted, vextq_u8::<8>(zeros_u8, shifted))
        }
    }

    #[inline(always)]
    unsafe fn store_group(dst: *mut u16, starts: uint16x8_t, group_lens: uint16x8_t) {
        // Interleaved `(start, len)` `u16` pairs are `DictEntry`s in memory.
        unsafe {
            vst1q_u16(dst, vzip1q_u16(starts, group_lens));
            vst1q_u16(dst.add(8), vzip2q_u16(starts, group_lens));
        }
    }

    let (len_chunks16, len_rem) = token_lens.as_chunks::<16>();
    let (entry_chunks32, entry_rem) = dict_entries.as_chunks_mut::<32>();
    // SAFETY: loads read within `len_chunks16` / `len_rem`, stores write within
    // `entry_chunks32` / `entry_rem`. Storing `u16` lanes into `MaybeUninit<DictEntry>` memory is
    // valid: see `DictEntry`'s layout contract, and any bit pattern is a valid `u16`.
    unsafe {
        let mask_0f = vdupq_n_u8(0x0F);
        let ones = vdupq_n_u8(1);
        let zeros_u8 = vdupq_n_u8(0);
        let mut running_start = vdupq_n_u16(0);

        for (len_chunk, entry_chunk) in len_chunks16.iter().zip(entry_chunks32.iter_mut()) {
            let packed = vld1q_u8(len_chunk.as_ptr());
            let low = vandq_u8(packed, mask_0f);
            let high = vshrq_n_u8::<4>(packed);
            // Token 2k is in the low nibble: interleave low and high nibbles.
            let lens_a = vaddq_u8(vzip1q_u8(low, high), ones);
            let lens_b = vaddq_u8(vzip2q_u8(low, high), ones);

            // Exclusive prefix sums of 16 tokens each in `u8` (in parallel).
            let excl_a = excl_prefix_sum_u8x16(lens_a, zeros_u8);
            let excl_b = excl_prefix_sum_u8x16(lens_b, zeros_u8);

            let g0 = vmovl_u8(vget_low_u8(lens_a));
            let g1 = vmovl_high_u8(lens_a);
            let g2 = vmovl_u8(vget_low_u8(lens_b));
            let g3 = vmovl_high_u8(lens_b);

            let excl0 = vmovl_u8(vget_low_u8(excl_a));
            let excl1 = vmovl_high_u8(excl_a);
            let excl2 = vmovl_u8(vget_low_u8(excl_b));
            let excl3 = vmovl_high_u8(excl_b);

            // Lane 7 of `excl_hi + g_hi` is the total of all 16 tokens (at most 256).
            let tot_a = vdupq_laneq_u16::<7>(vaddq_u16(excl1, g1));
            let tot_b = vdupq_laneq_u16::<7>(vaddq_u16(excl3, g3));
            let tot_ab = vaddq_u16(tot_a, tot_b);

            let base_a = running_start;
            let base_b = vaddq_u16(running_start, tot_a);
            running_start = vaddq_u16(running_start, tot_ab);

            let dst = entry_chunk.as_mut_ptr().cast::<u16>();
            store_group(dst, vaddq_u16(base_a, excl0), g0);
            store_group(dst.add(16), vaddq_u16(base_a, excl1), g1);
            store_group(dst.add(32), vaddq_u16(base_b, excl2), g2);
            store_group(dst.add(48), vaddq_u16(base_b, excl3), g3);
        }

        let mut num_decoded_bytes = len_chunks16.len() * 16;
        if len_rem.len() >= 8 {
            let packed = vld1_u8(len_rem.as_ptr());
            let low = vand_u8(packed, vget_low_u8(mask_0f));
            let high = vshr_n_u8::<4>(packed);
            let nibbles = vzip_u8(low, high);
            let lens_a = vaddq_u8(vcombine_u8(nibbles.0, nibbles.1), ones);
            let excl_a = excl_prefix_sum_u8x16(lens_a, zeros_u8);

            let g0 = vmovl_u8(vget_low_u8(lens_a));
            let g1 = vmovl_high_u8(lens_a);
            let excl0 = vmovl_u8(vget_low_u8(excl_a));
            let excl1 = vmovl_high_u8(excl_a);

            let tot_a = vdupq_laneq_u16::<7>(vaddq_u16(excl1, g1));
            let base_a = running_start;
            running_start = vaddq_u16(running_start, tot_a);

            let dst = entry_rem.as_mut_ptr().cast::<u16>();
            store_group(dst, vaddq_u16(base_a, excl0), g0);
            store_group(dst.add(16), vaddq_u16(base_a, excl1), g1);
            num_decoded_bytes += 8;
        }

        (num_decoded_bytes, vgetq_lane_u16::<0>(running_start) as u32)
    }
}

/// SSE2 version of `decode_dict_entries_simd`. See `decode_dict_entries_neon` for the lane widths
/// and why the 16-token exclusive prefix sum fits in `u8` without overflow.
#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
#[target_feature(enable = "sse2")]
unsafe fn decode_dict_entries_sse2(
    token_lens: &[u8],
    dict_entries: &mut [MaybeUninit<DictEntry>],
) -> (usize, u32) {
    use std::arch::x86_64::*;

    /// Exclusive prefix sum of 16 token lengths in `1..=16` (max lane value `15 * 16 = 240`).
    #[inline(always)]
    unsafe fn excl_prefix_sum_epi8(lens: __m128i) -> __m128i {
        unsafe {
            let sum = _mm_add_epi8(lens, _mm_slli_si128::<1>(lens));
            let sum = _mm_add_epi8(sum, _mm_slli_si128::<2>(sum));
            let sum = _mm_add_epi8(sum, _mm_slli_si128::<4>(sum));
            let shifted = _mm_slli_si128::<1>(sum);
            _mm_add_epi8(shifted, _mm_slli_si128::<8>(shifted))
        }
    }

    #[inline(always)]
    unsafe fn broadcast_lane7_epi16(vec: __m128i) -> __m128i {
        unsafe {
            let hi = _mm_shufflehi_epi16::<0xFF>(vec);
            _mm_shuffle_epi32::<0xFF>(hi)
        }
    }

    let (len_chunks, _) = token_lens.as_chunks::<8>();
    let (entry_chunks, _) = dict_entries.as_chunks_mut::<16>();
    // SAFETY: loads read the 8 bytes of `len_chunk`, stores write within the 16 entries of
    // `entry_chunk`. Storing `u16` lanes into `MaybeUninit<DictEntry>` memory is valid: see its
    // layout contract, and any bit pattern is a valid `u16`.
    unsafe {
        let mask_0f = _mm_set1_epi8(0x0F);
        let ones = _mm_set1_epi8(1);
        let zeros = _mm_setzero_si128();
        let mut running_start = zeros;
        for (len_chunk, entry_chunk) in len_chunks.iter().zip(entry_chunks.iter_mut()) {
            let packed = _mm_loadl_epi64(len_chunk.as_ptr().cast::<__m128i>());
            // Token 2k is in the low nibble: interleave low and high nibbles.
            let low_nibbles = _mm_and_si128(packed, mask_0f);
            let high_nibbles = _mm_and_si128(_mm_srli_epi16::<4>(packed), mask_0f);
            let lens = _mm_add_epi8(_mm_unpacklo_epi8(low_nibbles, high_nibbles), ones);
            let excl = excl_prefix_sum_epi8(lens);

            let g0 = _mm_unpacklo_epi8(lens, zeros);
            let g1 = _mm_unpackhi_epi8(lens, zeros);
            let excl0 = _mm_unpacklo_epi8(excl, zeros);
            let excl1 = _mm_unpackhi_epi8(excl, zeros);

            let starts0 = _mm_add_epi16(running_start, excl0);
            let starts1 = _mm_add_epi16(running_start, excl1);
            let tot = broadcast_lane7_epi16(_mm_add_epi16(excl1, g1));
            running_start = _mm_add_epi16(running_start, tot);

            // Interleaved `(start, len)` `u16` pairs are `DictEntry`s in memory.
            let dst = entry_chunk.as_mut_ptr().cast::<__m128i>();
            _mm_storeu_si128(dst, _mm_unpacklo_epi16(starts0, g0));
            _mm_storeu_si128(dst.add(1), _mm_unpackhi_epi16(starts0, g0));
            _mm_storeu_si128(dst.add(2), _mm_unpacklo_epi16(starts1, g1));
            _mm_storeu_si128(dst.add(3), _mm_unpackhi_epi16(starts1, g1));
        }
        (
            len_chunks.len() * 8,
            _mm_extract_epi16::<0>(running_start) as u16 as u32,
        )
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
    serialize_offsets(&code_offsets, output);
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

/// Reads the 64 bits of `codes` starting at bit offset `bit_pos`.
///
/// # Safety
///
/// `bit_pos / 8 + 8 <= codes.len()`.
#[inline(always)]
unsafe fn load_codes_at_bit(codes: &[u8], bit_pos: usize) -> u64 {
    debug_assert!(bit_pos / 8 + 8 <= codes.len());
    let packed = unsafe {
        codes
            .as_ptr()
            .add(bit_pos >> 3)
            .cast::<u64>()
            .read_unaligned()
    };
    u64::from_le(packed) >> (bit_pos & 7)
}

/// Reads the 64 bits of `codes` starting at code `code_pos`: code `code_pos` is in the low
/// `code_bits` bits, followed by the next codes. With `code_bits <= 12`, the next 3 codes are
/// complete too: 4 codes span at most `7 + 4 * 12 = 55` bits.
///
/// # Safety
///
/// `codes` was written by `write_codes` with `code_bits` bits per code, and
/// `code_pos < num_codes`. The 8 bytes read from the byte of code `code_pos` are then in
/// bounds, thanks to the `CODES_PADDING_NUM_BYTES` trailing bytes.
#[cfg(test)]
#[inline(always)]
unsafe fn load_codes(codes: &[u8], code_pos: usize, code_bits: usize) -> u64 {
    unsafe { load_codes_at_bit(codes, code_pos * code_bits) }
}

/// Copies the token of `dict_entry` to the start of `dst`, and returns the token length.
///
/// Copies a whole `MAX_TOKEN_SIZE` block (onpair's over-copy): the bytes written after the
/// token length are garbage.
///
/// # Safety
///
/// `dict_entry.start + MAX_TOKEN_SIZE <= dict_bytes.len()` and `dst.len() >= MAX_TOKEN_SIZE`.
#[inline(always)]
unsafe fn copy_token(
    dict_bytes: &[u8],
    dict_entry: DictEntry,
    dst: &mut [MaybeUninit<u8>],
) -> usize {
    let token_start = dict_entry.start as usize;
    debug_assert!(token_start + onpair::MAX_TOKEN_SIZE <= dict_bytes.len());
    debug_assert!(dst.len() >= onpair::MAX_TOKEN_SIZE);
    // `[u8; MAX_TOKEN_SIZE]` has alignment 1, and `MaybeUninit<u8>` has the layout of `u8`.
    unsafe {
        let token = dict_bytes
            .as_ptr()
            .add(token_start)
            .cast::<[u8; onpair::MAX_TOKEN_SIZE]>()
            .read_unaligned();
        dst.as_mut_ptr()
            .cast::<[u8; onpair::MAX_TOKEN_SIZE]>()
            .write_unaligned(token);
    }
    dict_entry.len as usize
}

#[doc(hidden)]
pub struct OnPair16Block {
    /// The block bytes. The dictionary bytes and the codes are read in place.
    bytes: OwnedBytes,
    /// Start of the dictionary bytes in `bytes`.
    dict_bytes_start: usize,
    /// Start of the codes in `bytes`.
    codes_start: usize,
    /// One entry per token, see `decode_dict_entries`.
    ///
    /// Invariant, checked by `open`: each token is readable for `MAX_TOKEN_SIZE` bytes in the
    /// dictionary bytes (onpair's read padding).
    dict_entries: Vec<DictEntry>,
    /// `num_values + 1` offsets of the values in the code stream.
    code_offsets: BlockOffsets,
    /// Width of a code, in `1..=MAX_CODE_BITS`.
    code_bits: u8,
    num_codes: u32,
}

impl OnPair16Block {
    /// Opens an OnPair16 block holding `num_values` values.
    ///
    /// Precondition: the first byte of `bytes` is `BLOCK_KIND_ONPAIR16`.
    /// Panics if `bytes` is corrupted.
    pub(super) fn open(bytes: OwnedBytes, num_values: u32) -> OnPair16Block {
        let block_bytes = bytes.as_slice();
        assert!(
            block_bytes.len() >= ONPAIR16_HEADER_NUM_BYTES,
            "truncated onpair16 block header"
        );
        // Skip the block kind, checked by `PlainBlock::open`.
        let mut header: &[u8] = &block_bytes[1..ONPAIR16_HEADER_NUM_BYTES];
        let num_tokens: usize = read_u16(&mut header) as usize + 1;
        assert!(
            num_tokens <= 1 << MAX_CODE_BITS,
            "too many tokens in plain block"
        );
        let dict_bytes_len = read_u32(&mut header) as usize;
        let code_offsets_len = read_u32(&mut header) as usize;
        let num_codes = read_u32(&mut header);
        debug_assert!(header.is_empty());
        // The writer uses the minimal width of the token ids, computed by onpair.
        let code_bits: u8 = (usize::BITS - (num_tokens - 1).leading_zeros()).max(1) as u8;
        debug_assert_eq!(code_bits, onpair::code_bits_for_num_tokens(num_tokens));

        let dict_bytes_start = ONPAIR16_HEADER_NUM_BYTES + num_tokens.div_ceil(2);
        let code_offsets_start = dict_bytes_start + dict_bytes_len;
        let codes_start = code_offsets_start + code_offsets_len;
        assert!(codes_start <= block_bytes.len(), "truncated plain block");
        assert_eq!(
            block_bytes.len() - codes_start,
            codes_num_bytes(num_codes as usize, code_bits),
            "invalid codes length in plain block"
        );

        let dict_entries = decode_dict_entries(
            &block_bytes[ONPAIR16_HEADER_NUM_BYTES..dict_bytes_start],
            num_tokens,
        );
        // Token starts are increasing: if the last token has its read padding, all tokens do.
        let last_token_start = dict_entries[num_tokens - 1].start as usize;
        assert!(
            last_token_start + onpair::MAX_TOKEN_SIZE <= dict_bytes_len,
            "invalid dictionary in plain block"
        );

        let code_offsets = BlockOffsets::open(
            block_bytes,
            code_offsets_start,
            code_offsets_len,
            num_values + 1,
        );
        OnPair16Block {
            bytes,
            dict_bytes_start,
            codes_start,
            dict_entries,
            code_offsets,
            code_bits,
            num_codes,
        }
    }

    /// Appends the bytes of the value at position `slot` in the block to `output`.
    pub(super) fn append_value(&self, slot: u32, output: &mut Vec<u8>) {
        let (code_start, code_end) = self.code_offsets.get_pair(self.bytes.as_slice(), slot);
        assert!(
            code_start <= code_end && code_end <= self.num_codes,
            "invalid code offsets in plain block"
        );
        let code_start = code_start as usize;
        let code_end = code_end as usize;
        let num_value_codes = code_end - code_start;
        if num_value_codes == 0 {
            return;
        }
        if num_value_codes <= DECODE_CHUNK_CODES {
            // Each token copy writes `MAX_TOKEN_SIZE` bytes, and advances the output by the token
            // length, at most `MAX_TOKEN_SIZE`: the value writes at most this many bytes.
            output.reserve(num_value_codes * onpair::MAX_TOKEN_SIZE);
            // SAFETY: `code_start..code_end` is within `0..num_codes`, and the spare capacity of
            // `output` has room for `MAX_TOKEN_SIZE` bytes per code.
            let num_written =
                unsafe { self.decode_codes(code_start..code_end, output.spare_capacity_mut()) };
            // SAFETY: `decode_codes` wrote the first `num_written` bytes of the spare capacity.
            unsafe { output.set_len(output.len() + num_written) };
        } else {
            self.append_value_chunked(code_start, code_end, output);
        }
    }

    #[cold]
    #[inline(never)]
    fn append_value_chunked(&self, mut code_pos: usize, code_end: usize, output: &mut Vec<u8>) {
        while code_pos < code_end {
            let chunk_end = (code_pos + DECODE_CHUNK_CODES).min(code_end);
            // Each token copy writes `MAX_TOKEN_SIZE` bytes, and advances the output by the token
            // length, at most `MAX_TOKEN_SIZE`: the chunk writes at most this many bytes.
            output.reserve((chunk_end - code_pos) * onpair::MAX_TOKEN_SIZE);
            // SAFETY: `code_pos..chunk_end` is within `0..num_codes`, and the spare capacity of
            // `output` has room for `MAX_TOKEN_SIZE` bytes per code.
            let num_written =
                unsafe { self.decode_codes(code_pos..chunk_end, output.spare_capacity_mut()) };
            // SAFETY: `decode_codes` wrote the first `num_written` bytes of the spare capacity.
            unsafe { output.set_len(output.len() + num_written) };
            code_pos = chunk_end;
        }
    }

    /// Decodes the codes at positions `code_range` to `dst`, and returns the number of bytes
    /// written.
    ///
    /// Each token is copied as a whole `MAX_TOKEN_SIZE` block (onpair's over-copy): the bytes
    /// written after the returned length are garbage.
    ///
    /// Panics if a code is out of the dictionary.
    ///
    /// # Safety
    ///
    /// `code_range.end <= self.num_codes`, and
    /// `dst.len() >= code_range.len() * MAX_TOKEN_SIZE`.
    #[inline(always)]
    unsafe fn decode_codes(&self, code_range: Range<usize>, dst: &mut [MaybeUninit<u8>]) -> usize {
        debug_assert!(dst.len() >= code_range.len() * onpair::MAX_TOKEN_SIZE);
        let block_bytes = self.bytes.as_slice();
        // SAFETY: `open` checks `dict_bytes_start <= codes_start <= block_bytes.len()`.
        // `dict_bytes` also covers the code offsets and the codes: `copy_token` reads within the
        // dictionary bytes by the `dict_entries` invariant, and within this slice in any case.
        let dict_bytes = unsafe { block_bytes.get_unchecked(self.dict_bytes_start..) };
        let codes = unsafe { block_bytes.get_unchecked(self.codes_start..) };
        let dict_entries: &[DictEntry] = &self.dict_entries;
        let code_bits = self.code_bits as usize;
        let code_mask = (1u64 << code_bits) - 1;
        // SAFETY: `codes` was written by `write_codes` (length checked by `open`), and the
        // caller guarantees `code_pos < code_range.end <= num_codes`.
        let load_codes = |bit_pos: usize| -> u64 { unsafe { load_codes_at_bit(codes, bit_pos) } };
        // `copy_token` calls below. SAFETY: the entry comes from `dict_entries`, so its token has
        // `MAX_TOKEN_SIZE` readable bytes in `dict_bytes` (invariant of `dict_entries`). Each
        // call advances `num_written` by at most `MAX_TOKEN_SIZE`, and `dst` has room for
        // `MAX_TOKEN_SIZE` bytes per code of `code_range`: before each call,
        // `num_written + MAX_TOKEN_SIZE <= dst.len()`, so `dst[num_written..]` is in bounds and
        // at least `MAX_TOKEN_SIZE` long.
        let mut num_written = 0;
        let mut code_pos = code_range.start;
        let mut bit_pos = code_pos * code_bits;
        let four_code_bits = 4 * code_bits;
        // 4 codes per load: `code_bits <= 12`, so they span at most `7 + 4 * 12 = 55` bits.
        while code_pos + 4 <= code_range.end {
            let packed = load_codes(bit_pos);
            let codes = [
                (packed & code_mask) as usize,
                ((packed >> code_bits) & code_mask) as usize,
                ((packed >> (2 * code_bits)) & code_mask) as usize,
                ((packed >> (3 * code_bits)) & code_mask) as usize,
            ];
            // One check for the 4 codes. It is the bounds check of the `get_unchecked` below.
            assert!(
                codes[0].max(codes[1]).max(codes[2].max(codes[3])) < dict_entries.len(),
                "code index out of range in plain block"
            );
            // SAFETY: checked just above.
            let entries = codes.map(|code| unsafe { *dict_entries.get_unchecked(code) });
            for dict_entry in entries {
                // SAFETY: see above.
                num_written += unsafe {
                    copy_token(dict_bytes, dict_entry, dst.get_unchecked_mut(num_written..))
                };
            }
            code_pos += 4;
            bit_pos += four_code_bits;
        }
        // Remaining 1..=3 codes all fit in a single 64-bit load (`7 + 3 * 12 = 43 <= 64` bits).
        if code_pos < code_range.end {
            let mut packed = load_codes(bit_pos);
            for _ in code_pos..code_range.end {
                let code = (packed & code_mask) as usize;
                packed >>= code_bits;
                let dict_entry = *dict_entries
                    .get(code)
                    .expect("code index out of range in plain block");
                // SAFETY: see above.
                num_written += unsafe {
                    copy_token(dict_bytes, dict_entry, dst.get_unchecked_mut(num_written..))
                };
            }
        }
        num_written
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
        let onpair16_block = OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32);
        for (slot, expected_value) in values.iter().enumerate() {
            let mut value = Vec::new();
            onpair16_block.append_value(slot as u32, &mut value);
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
        OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32);
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
        // `write_codes` against `load_codes`, as used by `decode_codes`: one code per load, and
        // 4 codes per load. Small code counts exercise the loads that reach into the padding.
        for code_bits in 1u8..=MAX_CODE_BITS {
            let code_mask = (1u64 << code_bits) - 1;
            for num_codes in [1usize, 2, 3, 4, 5, 7, 8, 9, 1_000] {
                let codes: Vec<u16> = (0..num_codes as u64)
                    .map(|i| (i.wrapping_mul(2_654_435_761) & code_mask) as u16)
                    .collect();
                let mut output = Vec::new();
                write_codes(&codes, code_bits, &mut output);
                assert_eq!(output.len(), codes_num_bytes(num_codes, code_bits));
                let load =
                    |code_pos: usize| unsafe { load_codes(&output, code_pos, code_bits as usize) };
                for (code_pos, &code) in codes.iter().enumerate() {
                    assert_eq!(load(code_pos) & code_mask, code as u64);
                }
                for code_pos in 0..num_codes.saturating_sub(3) {
                    let packed = load(code_pos);
                    for i in 0..4 {
                        let code = (packed >> (i * code_bits as usize)) & code_mask;
                        assert_eq!(code, codes[code_pos + i] as u64);
                    }
                }
            }
        }
    }

    #[test]
    fn test_decode_dict_entries() {
        for num_tokens in [1usize, 2, 7, 8, 9, 15, 16, 17, 127, 128, 3312, 4096] {
            for all_max_len in [false, true] {
                let mut token_lens = vec![0u8; num_tokens.div_ceil(2)];
                let mut expected_entries: Vec<DictEntry> = Vec::with_capacity(num_tokens);
                let mut offset: u32 = 0;
                for token_id in 0..num_tokens {
                    let token_len: u32 = if all_max_len {
                        16
                    } else {
                        (token_id % 16) as u32 + 1
                    };
                    token_lens[token_id / 2] |= ((token_len - 1) as u8) << (4 * (token_id % 2));
                    expected_entries.push(DictEntry {
                        start: offset as u16,
                        len: token_len as u16,
                    });
                    offset += token_len;
                }
                // SIMD (if available) then scalar.
                assert_eq!(
                    decode_dict_entries(&token_lens, num_tokens),
                    expected_entries
                );
                // Scalar only.
                let mut scalar_entries = vec![DictEntry::default(); token_lens.len() * 2];
                decode_dict_entries_scalar(&token_lens, &mut scalar_entries, 0);
                scalar_entries.truncate(num_tokens);
                assert_eq!(scalar_entries, expected_entries);
            }
        }
    }

    #[test]
    fn test_onpair16_block_large_value_chunked_decode() {
        // Exercises values whose code count exceeds `DECODE_CHUNK_CODES` (256).
        let long_value: Vec<u8> = (0..10_000usize).map(|i| b'a' + (i % 26) as u8).collect();
        let mut values = log_lines(200);
        values.push(long_value);
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_slice()).collect();
        let (bytes, offsets) = concatenate_values(&value_refs);
        let mut block_bytes = Vec::new();
        encode_onpair16_block(&bytes, &offsets, &mut block_bytes);
        let block = OnPair16Block::open(OwnedBytes::new(block_bytes), values.len() as u32);
        for (slot, expected_value) in values.iter().enumerate() {
            let mut decoded = Vec::new();
            block.append_value(slot as u32, &mut decoded);
            assert_eq!(&decoded, expected_value);
        }
    }
}
