//! Two-level bitpacked offset codec for plain blocks.
//!
//! Encodes a non-decreasing `u32` sequence `offsets[0..num_vals]` in two levels, with zero
//! allocation and O(1) work on open:
//! - Level 1 (`bases`): the first value of each `BLOCK_SIZE`-element block, `bases[block_id] =
//!   offsets[block_id * BLOCK_SIZE]`, bitpacked on `base_bit_width` bits.
//! - Level 2 (`deltas`): within-block deltas for elements `1..BLOCK_SIZE` of each block (omitting
//!   element `0`, whose delta is always `0`), bitpacked on `delta_bit_width` bits.
//!
//! ```text
//! offset  size                                          field
//! 0       4                                             num_vals (u32 LE)
//! 4       1                                             base_bit_width (0..=32)
//! 5       1                                             delta_bit_width (0..=32)
//! 6       ceil(num_blocks * base_bit_width / 8)         bitpacked block bases
//! ..      ceil(num_deltas * delta_bit_width / 8)        bitpacked within-block deltas
//! ```
//!
//! `num_blocks = num_vals.div_ceil(BLOCK_SIZE)` and `num_deltas = num_vals - num_blocks`.

use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use super::read_u32;

const HEADER_NUM_BYTES: usize = 6;

/// Number of offsets per Level 1 block (must be a power of 2).
///
/// With element 0 omitted from Level 2, the cost per value is approximately
/// `base_bit_width / B + ((B - 1) / B) * log2((B - 1) * mean_step)`. Empirically across
/// `B in {4, 8, 16, 32}`, `B = 8` minimizes total offset size on both small (`apache_2k`) and
/// large (`varied_logs_1m`) plain blocks.
const BLOCK_SIZE: u32 = 8;

/// Serializes `sorted_offsets` in two-level bitpacked format into `output`.
///
/// Precondition: `sorted_offsets` is sorted in non-decreasing order.
pub(super) fn serialize_offsets(sorted_offsets: &[u32], output: &mut Vec<u8>) {
    debug_assert!(sorted_offsets.is_sorted());
    let num_vals: u32 = sorted_offsets
        .len()
        .try_into()
        .expect("too many offsets in plain block");
    let block_size = BLOCK_SIZE as usize;

    let max_base: u32 = sorted_offsets
        .chunks(block_size)
        .last()
        .and_then(|chunk| chunk.first().copied())
        .unwrap_or(0);
    let max_delta: u32 = sorted_offsets
        .chunks(block_size)
        .map(|chunk| chunk[chunk.len() - 1] - chunk[0])
        .max()
        .unwrap_or(0);

    let base_bit_width: u8 = compute_num_bits(max_base as u64);
    let delta_bit_width: u8 = compute_num_bits(max_delta as u64);

    output.extend_from_slice(&num_vals.to_le_bytes());
    output.push(base_bit_width);
    output.push(delta_bit_width);

    if base_bit_width > 0 {
        let mut base_packer = BitPacker::new();
        for chunk in sorted_offsets.chunks(block_size) {
            base_packer
                .write(chunk[0] as u64, base_bit_width, output)
                .expect("writing to a Vec cannot fail");
        }
        base_packer
            .close(output)
            .expect("writing to a Vec cannot fail");
    }

    if delta_bit_width > 0 {
        let mut delta_packer = BitPacker::new();
        for chunk in sorted_offsets.chunks(block_size) {
            let base = chunk[0];
            for &offset in &chunk[1..] {
                let delta = offset - base;
                delta_packer
                    .write(delta as u64, delta_bit_width, output)
                    .expect("writing to a Vec cannot fail");
            }
        }
        delta_packer
            .close(output)
            .expect("writing to a Vec cannot fail");
    }
}

/// Zero-copy reader for two-level bitpacked offsets stored inside a plain block's byte buffer.
///
/// Invariant, checked by `open`: `bases_start <= deltas_start <= end <= block_bytes.len()` for
/// the block buffer passed to `open`, and callers must pass that same slice to `get_pair`.
pub(super) struct BlockOffsets {
    bases_start: usize,
    deltas_start: usize,
    end: usize,
    base_unpacker: BitUnpacker,
    delta_unpacker: BitUnpacker,
    num_vals: u32,
}

impl BlockOffsets {
    /// Opens the offsets encoded in `block_bytes[start..start + len]` and validates that it
    /// contains `expected_num_vals` entries.
    ///
    /// Precondition: `start + len <= block_bytes.len()`.
    /// Panics if the header or byte length is corrupted, or if `num_vals != expected_num_vals`.
    pub(super) fn open(
        block_bytes: &[u8],
        start: usize,
        len: usize,
        expected_num_vals: u32,
    ) -> BlockOffsets {
        let end = start
            .checked_add(len)
            .expect("offsets range overflow in plain block");
        assert!(end <= block_bytes.len(), "truncated offsets in plain block");
        let slice = &block_bytes[start..end];
        assert!(
            slice.len() >= HEADER_NUM_BYTES,
            "corrupted offsets in plain block"
        );
        let mut header = &slice[..HEADER_NUM_BYTES];
        let num_vals = read_u32(&mut header);
        let base_bit_width = header[0];
        let delta_bit_width = header[1];
        assert_eq!(
            num_vals, expected_num_vals,
            "unexpected number of offsets in plain block"
        );
        assert!(
            base_bit_width <= 32 && delta_bit_width <= 32,
            "corrupted offsets bit width in plain block"
        );

        let num_blocks = num_vals.div_ceil(BLOCK_SIZE) as usize;
        let num_deltas = num_vals as usize - num_blocks;
        let bases_num_bytes = (num_blocks * base_bit_width as usize).div_ceil(8);
        let deltas_num_bytes = (num_deltas * delta_bit_width as usize).div_ceil(8);
        assert_eq!(
            slice.len(),
            HEADER_NUM_BYTES + bases_num_bytes + deltas_num_bytes,
            "corrupted offsets in plain block"
        );

        let bases_start = start + HEADER_NUM_BYTES;
        let deltas_start = bases_start + bases_num_bytes;
        BlockOffsets {
            bases_start,
            deltas_start,
            end,
            base_unpacker: BitUnpacker::new(base_bit_width),
            delta_unpacker: BitUnpacker::new(delta_bit_width),
            num_vals,
        }
    }

    /// Returns `(offsets[idx], offsets[idx + 1])`.
    ///
    /// Precondition: `block_bytes` is the same buffer passed to `open`, and
    /// `idx + 1 < num_vals`.
    #[inline(always)]
    pub(super) fn get_pair(&self, block_bytes: &[u8], idx: u32) -> (u32, u32) {
        debug_assert!((idx as usize) + 1 < self.num_vals as usize);
        debug_assert!(self.end <= block_bytes.len());
        // SAFETY: `open` validated `bases_start <= deltas_start <= end <= block_bytes.len()`.
        let bases = unsafe { block_bytes.get_unchecked(self.bases_start..self.deltas_start) };
        let deltas = unsafe { block_bytes.get_unchecked(self.deltas_start..self.end) };
        let block_id = idx / BLOCK_SIZE;
        let in_block = idx % BLOCK_SIZE;
        if in_block == 0 {
            let base = self.base_unpacker.get(block_id, bases) as u32;
            let delta1 = self.delta_unpacker.get(idx - block_id, deltas) as u32;
            (base, base.wrapping_add(delta1))
        } else if in_block + 1 < BLOCK_SIZE {
            let base = self.base_unpacker.get(block_id, bases) as u32;
            let (delta0, delta1) = self.delta_unpacker.get_pair(idx - block_id - 1, deltas);
            (
                base.wrapping_add(delta0 as u32),
                base.wrapping_add(delta1 as u32),
            )
        } else {
            let (base0, base1) = self.base_unpacker.get_pair(block_id, bases);
            let delta0 = self.delta_unpacker.get(idx - block_id - 1, deltas) as u32;
            // The first element of block `block_id + 1` has delta 0, so its value is `base1`.
            ((base0 as u32).wrapping_add(delta0), base1 as u32)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_and_check(offsets: &[u32]) {
        let mut buf: Vec<u8> = vec![0xAA; 7];
        let start = buf.len();
        serialize_offsets(offsets, &mut buf);
        let len = buf.len() - start;
        buf.extend_from_slice(&[0xBB; 5]);

        let reader = BlockOffsets::open(&buf, start, len, offsets.len() as u32);
        for i in 0..offsets.len().saturating_sub(1) {
            assert_eq!(
                reader.get_pair(&buf, i as u32),
                (offsets[i], offsets[i + 1]),
                "mismatch at idx {i} for len {}",
                offsets.len()
            );
        }
    }

    #[test]
    fn test_offsets_edge_cases() {
        roundtrip_and_check(&[0]);
        roundtrip_and_check(&[0, 0]);
        roundtrip_and_check(&[0, 0, 0, 0, 0]);
        roundtrip_and_check(&[0, 1]);
        roundtrip_and_check(&[0, u32::MAX]);
        roundtrip_and_check(&[0, 0, u32::MAX, u32::MAX]);
    }

    #[test]
    fn test_offsets_around_block_boundaries() {
        for num_vals in [
            1usize,
            2,
            BLOCK_SIZE as usize - 1,
            BLOCK_SIZE as usize,
            BLOCK_SIZE as usize + 1,
            BLOCK_SIZE as usize * 2 - 1,
            BLOCK_SIZE as usize * 2,
            BLOCK_SIZE as usize * 2 + 1,
            1_000,
        ] {
            let mut offsets: Vec<u32> = Vec::with_capacity(num_vals);
            let mut acc: u32 = 0;
            for i in 0..num_vals {
                offsets.push(acc);
                acc += ((i * 7) % 31) as u32;
            }
            roundtrip_and_check(&offsets);
        }
    }

    #[test]
    #[should_panic(expected = "unexpected number of offsets")]
    fn test_offsets_wrong_num_vals_panics() {
        let offsets = [0u32, 3, 7, 12];
        let mut buf = Vec::new();
        serialize_offsets(&offsets, &mut buf);
        BlockOffsets::open(&buf, 0, buf.len(), offsets.len() as u32 + 1);
    }

    #[test]
    #[should_panic(expected = "corrupted offsets")]
    fn test_offsets_truncated_panics() {
        let offsets: Vec<u32> = (0..100).map(|i| i * 5).collect();
        let mut buf = Vec::new();
        serialize_offsets(&offsets, &mut buf);
        BlockOffsets::open(&buf, 0, buf.len() - 1, offsets.len() as u32);
    }
}
