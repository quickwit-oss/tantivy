//! Bulk-OR a posting bitset into a window of [`TinySet`]s.
//!
//! Mirrors Lucene's `FixedBitSet.orRange` used by
//! `Lucene104PostingsReader.BlockPostingsEnum.intoBitSet` for UNARY blocks.

use common::TinySet;

/// OR `len` bits of `src` starting at bit `src_from` into `dest` starting at
/// bit `dest_from`.
///
/// `src` is a little-endian bitset (`bit s` lives at `src[s/8] & (1 << s%8)`),
/// matching [`super::compression::BlockEncoder::compress_bitset_sorted`].
/// `dest[i]` covers destination bits `[i*64, (i+1)*64)`.
pub(crate) fn or_range_into_tinysets(
    src: &[u8],
    src_from: u32,
    dest: &mut [TinySet],
    dest_from: u32,
    len: u32,
) {
    if len == 0 {
        return;
    }
    debug_assert!((dest_from as usize) + (len as usize) <= dest.len() * 64);

    let mut remaining = len;
    let mut src_bit = src_from;
    let mut dest_bit = dest_from;

    while remaining > 0 {
        let dest_off = dest_bit % 64;
        let dest_idx = (dest_bit / 64) as usize;
        let space = 64 - dest_off;
        let take = remaining.min(space);
        let src_word = load_unaligned_bits(src, src_bit, take);
        dest[dest_idx].union_mut(TinySet::from_bits(src_word << dest_off));
        src_bit += take;
        dest_bit += take;
        remaining -= take;
    }
}

/// Load the low `nbits` bits of `src` starting at bit `bit` (`nbits <= 64`).
fn load_unaligned_bits(src: &[u8], bit: u32, nbits: u32) -> u64 {
    debug_assert!(nbits <= 64);
    if nbits == 0 {
        return 0;
    }
    let byte_idx = (bit / 8) as usize;
    let bit_off = bit % 8;
    // `bit_off + nbits` can be 71, so the scratch word is u128.
    let bytes_needed = ((bit_off + nbits + 7) / 8) as usize;
    let mut v = 0u128;
    for i in 0..bytes_needed {
        let b = src.get(byte_idx + i).copied().unwrap_or(0);
        v |= u128::from(b) << (8 * i);
    }
    v >>= bit_off;
    let bits = v as u64;
    if nbits < 64 {
        bits & ((1u64 << nbits) - 1)
    } else {
        bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bit_in_bytes(bytes: &[u8], bit: u32) -> bool {
        let b = bytes.get((bit / 8) as usize).copied().unwrap_or(0);
        (b & (1 << (bit % 8))) != 0
    }

    #[test]
    fn or_range_matches_bit_walk() {
        // 5 longs = 320 bits, with a mix of set bits.
        let mut src = vec![0u8; 40];
        for bit in [0u32, 1, 7, 8, 63, 64, 65, 127, 200, 255, 256, 319] {
            src[(bit / 8) as usize] |= 1 << (bit % 8);
        }
        for src_from in [0u32, 1, 7, 8, 63, 64, 200] {
            for dest_from in [0u32, 1, 5, 63, 64, 100] {
                for len in [0u32, 1, 7, 8, 64, 65, 128, 200] {
                    let dest_bits = dest_from + len;
                    if dest_bits > 16 * 64 {
                        continue;
                    }
                    if src_from + len > 320 {
                        continue;
                    }
                    let mut dest = [TinySet::empty(); 16];
                    or_range_into_tinysets(&src, src_from, &mut dest, dest_from, len);
                    for i in 0..len {
                        let expected = bit_in_bytes(&src, src_from + i);
                        let db = dest_from + i;
                        let got = dest[(db / 64) as usize].contains(db % 64);
                        assert_eq!(
                            got, expected,
                            "src_from={src_from} dest_from={dest_from} len={len} i={i}"
                        );
                    }
                }
            }
        }
    }
}
