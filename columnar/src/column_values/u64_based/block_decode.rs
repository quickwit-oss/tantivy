//! Block-decode policy for the u64 codecs: when a `get_range` is worth routing
//! through the batch unpacker, and the loop that drives it.
//!
//! The unpack kernels live in `tantivy_bitpacker::block_decode`. Everything
//! below is about *whether* to call them, which is a columnar-side question
//! because it depends on how expensive each codec's `get_val` is.

use tantivy_bitpacker::block_decode::simd_enabled;

pub(crate) use tantivy_bitpacker::block_decode::BLOCK_LEN;

/// How much of a codec's per-value work the batch arm *also* pays
#[derive(Clone, Copy)]
pub(crate) enum DecodeCost {
    /// `Bitpacked`: batch applies the mapping as a flat add/multiply.
    Flat,
    /// `Linear`: as `Flat`, plus a line eval per row that both arms pay.
    Interpolated,
    /// `BlockFor`, `Alp`, `BlockwiseLinear`: `get_val` re-reads per-block
    /// metadata per row, which the batch arm hoists out of the loop.
    Blocked,
}

/// Rows a whole `get_range` call must cover before batching it beats a `get_val` loop;
#[inline]
pub(crate) fn min_batch_rows(cost: DecodeCost) -> usize {
    match (cost, simd_enabled()) {
        (DecodeCost::Flat, true) => 32,
        (DecodeCost::Flat, false) => 64,
        (DecodeCost::Interpolated, true) => 64,
        (DecodeCost::Interpolated, false) => usize::MAX,
        (DecodeCost::Blocked, _) => 16,
    }
}

/// A codec that decodes runs of packed values, already mapped to final column
/// values. The two required methods are everything [`Self::decode_range`]
/// needs that differs between codecs, so the codecs cannot drift apart in how
/// they drive the loop.
pub(crate) trait BlockDecode: crate::ColumnValues<u64> {
    /// Decodes `out.len()` rows from `block_idx * BLOCK_LEN + in_block`,
    /// already mapped to column values.
    ///
    /// Callers guarantee `in_block + out.len() <= BLOCK_LEN` and `block_idx <
    /// num_full_blocks()`, so every slot asked for is in the packed stream.
    fn decode_block_range(&self, block_idx: usize, in_block: usize, out: &mut [u64]);

    /// Leading blocks whose 128 slots are all present in the packed stream.
    /// Columns are not padded to a block boundary, so the trailing partial
    /// block has no bytes for its missing slots.
    fn num_full_blocks(&self) -> usize;

    /// Batched [`crate::ColumnValues::get_range`].
    #[inline]
    fn decode_range(&self, start: u64, output: &mut [u64]) {
        let mut row = start as usize;
        let packed_rows = self.num_full_blocks() * BLOCK_LEN;
        let head_len = output.len().min(packed_rows.saturating_sub(row));
        let (mut head, tail) = output.split_at_mut(head_len);
        while !head.is_empty() {
            let in_block = row % BLOCK_LEN;
            let take = (BLOCK_LEN - in_block).min(head.len());
            let (chunk, rest) = head.split_at_mut(take);
            self.decode_block_range(row / BLOCK_LEN, in_block, chunk);
            row += take;
            head = rest;
        }
        self.fill_per_value(row, tail);
    }

    /// Fallback for the trailing partial block, whose missing slots have no
    /// bytes in the packed stream. Not meant to be overridden.
    #[inline]
    fn fill_per_value(&self, first_row: usize, output: &mut [u64]) {
        for (i, o) in output.iter_mut().enumerate() {
            *o = self.get_val((first_row + i) as u32);
        }
    }
}

/// This only beats the default `(0..n).map(get_val)` when `get_val` is expensive.
/// Requires padded streams (every block fully decodable).
pub(crate) fn iter_via_blocks<'a, C: BlockDecode + ?Sized>(
    column: &'a C,
    num_rows: usize,
) -> Box<dyn Iterator<Item = u64> + 'a> {
    let num_blocks = num_rows.div_ceil(BLOCK_LEN);
    Box::new(
        (0..num_blocks)
            .flat_map(move |block_idx| {
                let mut buf = [0u64; BLOCK_LEN];
                column.decode_block_range(block_idx, 0, &mut buf);
                buf.into_iter()
            })
            .take(num_rows),
    )
}
