//! Block-decode policy for the u64 codecs: when a `get_range` is worth routing
//! through whole-block unpacking, and the loop that drives it.
//!
//! The unpack kernels live in `tantivy_bitpacker::block_decode`. Everything
//! below is about *whether* to call them, which is a columnar-side question
//! because it depends on how expensive each codec's `get_val` is.

pub(crate) use tantivy_bitpacker::block_decode::{
    BLOCK_LEN, decode_block, simd_enabled, simd_kernel_applies,
};

/// Minimum rows a range must take from a *partial* block before whole-block
/// decode + copy beats the per-value unpacker.
///
/// A partial-block decode costs a fixed amount regardless of how few rows the
/// range needs; without this floor a 1-row range pays for 128 decoded values.
#[inline]
pub(crate) fn partial_block_min_rows(bit_width: u8) -> usize {
    match bit_width as usize {
        0 | 64 => 48,
        w if simd_kernel_applies(w as u8) => 48,
        _ => 80,
    }
}

/// How expensive a codec's `get_val` is, which is what decides where its
/// batch `get_range` starts beating a per-value loop. See [`min_batch_rows`].
#[derive(Clone, Copy)]
pub(crate) enum DecodeCost {
    /// One bitpacked residual stream (`Bitpacked`, `Linear`, `BlockwiseLinear`):
    /// `get_val` is a load, shift and mask, so the loop stays competitive for
    /// a long time.
    Flat,
    /// Per-block metadata re-read on every `get_val` (`BlockFor`, `Alp`), so
    /// the loop falls behind sooner.
    Blocked,
}

/// Minimum rows a whole `get_range` call must cover before routing it through
/// [`get_range_via_blocks`] beats a plain `get_val` loop over the same rows --
/// the value these codecs report from [`crate::ColumnValues::min_batch_rows`].
///
/// Distinct from [`partial_block_min_rows`], which decides per *block* whether
/// to decode one; this decides whether the call is worth batching at all.
/// Calibrated with the `gate_sweep` ignored test in `monotonic_column`, which
/// A/Bs both arms in one process over the concrete readers. Both thresholds
/// sit further out without the SIMD kernel, which only the batch side uses.
#[inline]
pub(crate) fn min_batch_rows(cost: DecodeCost) -> usize {
    match (cost, simd_enabled()) {
        (DecodeCost::Flat, true) => 96,
        (DecodeCost::Flat, false) => 128,
        (DecodeCost::Blocked, true) => 48,
        (DecodeCost::Blocked, false) => 96,
    }
}

/// A codec that decodes whole 128-value blocks, already mapped to final
/// column values. The three required methods are everything
/// [`get_range_via_blocks`] needs that differs between codecs, so the codecs
/// cannot drift apart in how they drive the block loop.
pub(crate) trait BlockDecode: crate::ColumnValues<u64> {
    /// Decodes the 128 rows of `block_idx`, already mapped to column values.
    ///
    /// Only called for blocks reported by [`Self::num_full_blocks`].
    fn decode_block_mapped(&self, block_idx: usize, out: &mut [u64; BLOCK_LEN]);

    /// Number of leading blocks whose 128 slots are all present in the packed
    /// stream. Columns are not padded to a block boundary, so the trailing
    /// partial block has no bytes for its missing slots and is served by
    /// `get_val` instead.
    fn num_full_blocks(&self) -> usize;

    /// This block's [`partial_block_min_rows`]. Called once per block touched,
    /// so codecs with a single stream-wide bit width should resolve it at load
    /// time rather than recompute it here.
    fn partial_min_rows(&self, block_idx: usize) -> usize;

    /// Batched [`crate::ColumnValues::get_range`]. See [`get_range_via_blocks`].
    #[inline]
    fn decode_range(&self, start: u64, output: &mut [u64]) {
        get_range_via_blocks(
            start,
            output,
            self.num_full_blocks(),
            |block_idx| self.partial_min_rows(block_idx),
            |block_idx, out| self.decode_block_mapped(block_idx, out),
            |row| self.get_val(row),
        );
    }
}

/// Drives a codec's whole-block decode for [`crate::ColumnValues::get_range`].
///
/// Rows in full 128-value blocks go through `decode_block_mapped(block_idx,
/// out)`, which must produce final column values. A range boundary inside a
/// block goes through a stack buffer when the range takes at least
/// `partial_min_rows(block_idx)` rows from that block (see
/// [`partial_block_min_rows`]); fewer rows, rows at or past `num_full_blocks *
/// BLOCK_LEN` (the trailing partial block, whose missing slots have no bytes
/// in the packed stream), and everything past it use `get_val(row)`.
#[inline]
pub(crate) fn get_range_via_blocks(
    start: u64,
    output: &mut [u64],
    num_full_blocks: usize,
    mut partial_min_rows: impl FnMut(usize) -> usize,
    mut decode_block_mapped: impl FnMut(usize, &mut [u64; BLOCK_LEN]),
    mut get_val: impl FnMut(u32) -> u64,
) {
    let first_row = start as usize;
    let end_row = first_row + output.len();

    let mut row = first_row;
    let mut out_offset = 0usize;
    while row < end_row {
        let block_idx = row / BLOCK_LEN;
        let in_block = row % BLOCK_LEN;
        let take = (BLOCK_LEN - in_block).min(end_row - row);
        if block_idx >= num_full_blocks {
            break;
        }
        if take == BLOCK_LEN {
            let out: &mut [u64; BLOCK_LEN] = (&mut output[out_offset..out_offset + BLOCK_LEN])
                .try_into()
                .unwrap();
            decode_block_mapped(block_idx, out);
        } else if take >= partial_min_rows(block_idx) {
            let mut buf = [0u64; BLOCK_LEN];
            decode_block_mapped(block_idx, &mut buf);
            output[out_offset..out_offset + take].copy_from_slice(&buf[in_block..in_block + take]);
        } else {
            for (i, o) in output[out_offset..out_offset + take].iter_mut().enumerate() {
                *o = get_val((row + i) as u32);
            }
        }
        row += take;
        out_offset += take;
    }
    // Trailing partial block (and everything past it).
    for (i, o) in output[out_offset..].iter_mut().enumerate() {
        *o = get_val((row + i) as u32);
    }
}
