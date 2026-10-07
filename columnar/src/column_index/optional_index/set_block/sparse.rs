use std::ops::Range;

use crate::column_index::optional_index::{SelectCursor, Set, SetCodec};

pub struct SparseBlockCodec;

impl SetCodec for SparseBlockCodec {
    type Item = u16;
    type Reader<'a> = SparseBlock<'a>;

    fn serialize(
        els: impl Iterator<Item = u16>,
        mut wrt: impl std::io::Write,
    ) -> std::io::Result<()> {
        for el in els {
            wrt.write_all(&el.to_le_bytes())?;
        }
        Ok(())
    }

    fn open(data: &[u8]) -> Self::Reader<'_> {
        SparseBlock(data)
    }
}

#[derive(Copy, Clone)]
pub struct SparseBlock<'a>(&'a [u8]);

impl<'a> SelectCursor<u16> for SparseBlock<'a> {
    #[inline]
    fn select(&mut self, rank: u16) -> u16 {
        <SparseBlock<'a> as Set<u16>>::select(self, rank)
    }
}

impl Set<u16> for SparseBlock<'_> {
    type SelectCursor<'b>
        = Self
    where Self: 'b;

    #[inline(always)]
    fn contains(&self, el: u16) -> bool {
        self.binary_search(el).is_ok()
    }

    #[inline(always)]
    fn rank_if_exists(&self, el: u16) -> Option<u16> {
        self.binary_search(el).ok().map(|el| el as u16)
    }

    #[inline(always)]
    fn rank(&self, el: u16) -> u16 {
        self.binary_search(el).unwrap_or_else(|el| el) as u16
    }

    #[inline(always)]
    fn select(&self, rank: u16) -> u16 {
        let offset = rank as usize * 2;
        u16::from_le_bytes(self.0[offset..offset + 2].try_into().unwrap())
    }

    #[inline(always)]
    fn select_cursor(&self) -> Self::SelectCursor<'_> {
        *self
    }
}

impl SparseBlock<'_> {
    #[inline]
    fn num_vals(&self) -> u16 {
        (self.0.len() / 2) as u16
    }

    /// Returns the value at `idx`, without bounds checking.
    ///
    /// # Safety
    ///
    /// `idx < self.num_vals()` is required.
    #[inline(always)]
    unsafe fn value_at_idx_unchecked(&self, idx: usize) -> u16 {
        debug_assert!(idx < self.num_vals() as usize);
        let val_ptr: *const u16 = self.0.as_ptr() as *const u16;
        u16::from_le(unsafe { val_ptr.add(idx).read_unaligned() })
    }

    /// Looks up several sorted elements while keeping a cursor into the sparse block.
    ///
    /// The callback receives `(element, rank)` for each element present in the block. Once a
    /// lookup has passed a sparse value, later lookups never search the prefix containing that
    /// value again. Queries below the next sparse value only require a comparison with that value.
    ///
    /// els are required to be sorted, or the results could be incomplete.
    #[inline]
    pub(crate) fn rank_if_exists_batch(
        &self,
        mut sorted_els: impl Iterator<Item = u16>,
        mut collect: impl FnMut(u16, u16),
    ) {
        let num_vals = self.num_vals() as usize;
        let mut candidate_rank: usize = 0;

        while candidate_rank < num_vals {
            let candidate = unsafe { self.value_at_idx_unchecked(candidate_rank) };
            let Some(needle) = sorted_els.find(|needle| *needle >= candidate) else {
                return;
            };

            if candidate == needle {
                // The cursor stays on the matched value, so that a duplicate needle matches too.
                collect(needle, candidate_rank as u16);
                continue;
            }

            // Check the immediately following value first. This makes querying consecutive
            // sparse values linear instead of performing a binary search for every value.
            candidate_rank += 1;
            if candidate_rank >= num_vals {
                // `needle` and the remaining needles are larger than every value.
                return;
            }
            let candidate = unsafe { self.value_at_idx_unchecked(candidate_rank) };
            if candidate >= needle {
                if candidate == needle {
                    collect(needle, candidate_rank as u16);
                }
                // Otherwise `needle` is absent. Either way, the cursor now points to `candidate`.
                continue;
            }

            // `candidate_rank < num_vals`, so `left <= num_vals`.
            let left = candidate_rank + 1;
            // No need to always search the rest of the block here!
            // Values are distinct and sorted, so `vals[candidate_rank + k] >= candidate + k`.
            // With `k = needle - candidate`, the value at `candidate_rank + k` is already
            // `>= needle`: the search can stop right after that index.
            // `needle > candidate`, so `gap_bound >= left + 1`, hence `left <= right`.
            let gap_bound = candidate_rank + (needle - candidate) as usize + 1;
            let right = gap_bound.min(num_vals);
            // The value at `left - 1` is lower than `needle`, and the value at `right`, if any,
            // is greater than `needle`, so the insertion point found in `left..right` is also
            // the insertion point in the whole block.
            // The needle is usually a few values after the cursor, hence the exponential search.
            match self.range(left..right).exponential_search(needle) {
                Ok(rel_rank) => {
                    candidate_rank = left + rel_rank;
                    collect(needle, candidate_rank as u16);
                }
                Err(rel_rank) => {
                    // `left + rel_rank` may be `num_vals`: the next iteration then returns.
                    candidate_rank = left + rel_rank;
                }
            }
        }
    }

    /// Returns the sub-block made of the values at indices `els_range`.
    ///
    /// Indices in the returned block are relative to `els_range.start`: callers need to add it
    /// back to get indices in `self`.
    ///
    /// Panics if `els_range.start > els_range.end` or `els_range.end > self.num_vals()`.
    #[inline]
    fn range(&self, els_range: Range<usize>) -> Self {
        let Range { start, end } = els_range;
        SparseBlock(&self.0[start * 2..end * 2])
    }

    /// Searches `target` among all the values of the block, starting from its beginning.
    ///
    /// Returns the same result as `binary_search`: `Ok(idx)` if found, `Err(idx)` with the
    /// insertion point otherwise. It is faster when `target` is close to the start of the block.
    #[inline]
    fn exponential_search(&self, target: u16) -> Result<usize, usize> {
        let num_vals = self.num_vals() as usize;
        // Invariant: the values at indices `< left` are lower than `target`, and if `target` is
        // present, it is in `left..right`.
        let mut left = 0;
        let mut right = num_vals;
        // Probes indices `0, 1, 3, 7, 15`...
        let mut step: usize = 1;
        loop {
            let probe = left + step - 1;
            if probe >= right {
                break;
            }
            // SAFETY: `probe < right <= num_vals`.
            let probe_val = unsafe { self.value_at_idx_unchecked(probe) };
            if probe_val >= target {
                right = probe + 1;
                break;
            }
            left = probe + 1;
            step *= 2;
        }
        // Either `right == num_vals`, or the value at `right - 1` is `>= target`: the insertion
        // point found in `left..right` is also the insertion point in the whole block.
        match self.range(left..right).binary_search(target) {
            Ok(rel_idx) => Ok(left + rel_idx),
            Err(rel_idx) => Err(left + rel_idx),
        }
    }

    /// Searches `target` among all the values of the block.
    ///
    /// Returns `Ok(idx)` if found, `Err(idx)` with the insertion point otherwise.
    ///
    /// The loop does not stop early on a match: it always runs about `log2(size)` iterations, and
    /// the search direction is a conditional select rather than a branch. Branches on the
    /// comparison result would be unpredictable, hence `std::hint::select_unpredictable`.
    #[inline]
    fn binary_search(&self, target: u16) -> Result<usize, usize> {
        // Invariant: if `target` is present, it is in `base..base + size`, and
        // `base + size <= num_vals`.
        let mut base = 0;
        let mut size = self.num_vals() as usize;
        if size == 0 {
            return Err(base);
        }
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // SAFETY: `half < size`, so `mid < base + size <= num_vals`.
            let mid_val = unsafe { self.value_at_idx_unchecked(mid) };
            // the hint asks the compiler to emit a conditional move (`cmov`/`csel`)
            // rather than a branch.
            base = std::hint::select_unpredictable(mid_val > target, base, mid);
            size -= half;
        }
        // SAFETY: `size == 1`, so `base < base + size <= num_vals`.
        let base_val = unsafe { self.value_at_idx_unchecked(base) };
        if base_val == target {
            Ok(base)
        } else {
            // `base_val < target` means `target` belongs right after `base`.
            Err(base + (base_val < target) as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn serialize_sparse(vals: &[u16]) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(vals.len() * 2);
        SparseBlockCodec::serialize(vals.iter().copied(), &mut buffer).unwrap();
        buffer
    }

    #[test]
    fn test_binary_search_empty() {
        let buffer = serialize_sparse(&[]);
        let block = SparseBlockCodec::open(&buffer);
        assert_eq!(block.binary_search(0), Err(0));
        assert_eq!(block.binary_search(17), Err(0));
        assert_eq!(block.binary_search(u16::MAX), Err(0));
    }

    #[test]
    fn test_binary_search_single_value() {
        let buffer = serialize_sparse(&[10]);
        let block = SparseBlockCodec::open(&buffer);
        assert_eq!(block.binary_search(9), Err(0));
        assert_eq!(block.binary_search(10), Ok(0));
        assert_eq!(block.binary_search(11), Err(1));
    }

    #[test]
    fn test_binary_search_extreme_values() {
        let buffer = serialize_sparse(&[0, u16::MAX]);
        let block = SparseBlockCodec::open(&buffer);
        assert_eq!(block.binary_search(0), Ok(0));
        assert_eq!(block.binary_search(1), Err(1));
        assert_eq!(block.binary_search(u16::MAX - 1), Err(1));
        assert_eq!(block.binary_search(u16::MAX), Ok(1));
    }

    #[test]
    fn test_binary_search_every_position() {
        let vals: Vec<u16> = (0..100u16).map(|val| val * 3 + 1).collect();
        let buffer = serialize_sparse(&vals);
        let block = SparseBlockCodec::open(&buffer);
        for (idx, &val) in vals.iter().enumerate() {
            assert_eq!(block.binary_search(val), Ok(idx));
            assert_eq!(block.binary_search(val - 1), Err(idx));
            assert_eq!(block.binary_search(val + 1), Err(idx + 1));
        }
    }

    #[test]
    fn test_exponential_search_every_position() {
        let empty_buffer = serialize_sparse(&[]);
        let empty_block = SparseBlockCodec::open(&empty_buffer);
        assert_eq!(empty_block.exponential_search(0), Err(0));
        assert_eq!(empty_block.exponential_search(u16::MAX), Err(0));

        let vals: Vec<u16> = (0..100u16).map(|val| val * 3 + 1).collect();
        let buffer = serialize_sparse(&vals);
        let block = SparseBlockCodec::open(&buffer);
        for (idx, &val) in vals.iter().enumerate() {
            assert_eq!(block.exponential_search(val), Ok(idx));
            assert_eq!(block.exponential_search(val - 1), Err(idx));
            assert_eq!(block.exponential_search(val + 1), Err(idx + 1));
        }
    }

    #[test]
    fn test_range_binary_search() {
        let vals: [u16; 5] = [1, 3, 5, 7, 9];
        let buffer = serialize_sparse(&vals);
        let block = SparseBlockCodec::open(&buffer);
        // Values before index 2 are ignored, and indices are relative to 2.
        let tail = block.range(2..5);
        assert_eq!(tail.binary_search(1), Err(0));
        assert_eq!(tail.binary_search(3), Err(0));
        assert_eq!(tail.binary_search(5), Ok(0));
        assert_eq!(tail.binary_search(6), Err(1));
        assert_eq!(tail.binary_search(9), Ok(2));
        assert_eq!(tail.binary_search(10), Err(3));
        // An empty range is allowed, including at `num_vals`.
        let empty = block.range(5..5);
        assert_eq!(empty.binary_search(9), Err(0));
        assert_eq!(empty.binary_search(10), Err(0));
    }

    #[test]
    fn test_binary_search_unaligned_data() {
        // The block data has no alignment guarantee: make sure we read correctly from an odd
        // address.
        let vals: [u16; 4] = [2, 0x0100, 0x1234, u16::MAX];
        let mut buffer: Vec<u8> = vec![0u8];
        buffer.extend_from_slice(&serialize_sparse(&vals));
        let block = SparseBlockCodec::open(&buffer[1..]);
        for (idx, &val) in vals.iter().enumerate() {
            assert_eq!(block.binary_search(val), Ok(idx));
        }
        assert_eq!(block.binary_search(1), Err(0));
        assert_eq!(block.binary_search(0x1235), Err(3));
    }

    fn vals_target_strategy() -> impl Strategy<Value = (Vec<u16>, u16)> {
        proptest::collection::btree_set(any::<u16>(), 0..1_000).prop_flat_map(|vals| {
            let vals: Vec<u16> = vals.into_iter().collect();
            // Pick targets among existing values half of the time, to exercise the `Ok` path.
            let target_strategy = if vals.is_empty() {
                any::<u16>().boxed()
            } else {
                prop_oneof![any::<u16>(), proptest::sample::select(vals.clone())].boxed()
            };
            (Just(vals), target_strategy)
        })
    }

    proptest! {
        #[test]
        fn test_proptest_binary_search((vals, target) in vals_target_strategy()) {
            let buffer = serialize_sparse(&vals);
            let block = SparseBlockCodec::open(&buffer);
            // `vals` is strictly increasing, so both the `Ok` and the `Err` result are uniquely
            // defined, and `slice::binary_search` is a valid reference.
            prop_assert_eq!(block.binary_search(target), vals.binary_search(&target));
        }

        #[test]
        fn test_proptest_exponential_search((vals, target) in vals_target_strategy()) {
            let buffer = serialize_sparse(&vals);
            let block = SparseBlockCodec::open(&buffer);
            prop_assert_eq!(block.exponential_search(target), vals.binary_search(&target));
        }
    }

    fn rank_if_exists_batch_to_vec(block: &SparseBlock, sorted_needles: &[u16]) -> Vec<(u16, u16)> {
        let mut found: Vec<(u16, u16)> = Vec::with_capacity(sorted_needles.len());
        block.rank_if_exists_batch(sorted_needles.iter().copied(), |needle, rank| {
            found.push((needle, rank))
        });
        found
    }

    /// Naive reference for `rank_if_exists_batch`: one independent lookup per needle.
    fn expected_rank_if_exists_batch(vals: &[u16], sorted_needles: &[u16]) -> Vec<(u16, u16)> {
        let mut found: Vec<(u16, u16)> = Vec::with_capacity(sorted_needles.len());
        for &needle in sorted_needles {
            if let Ok(rank) = vals.binary_search(&needle) {
                found.push((needle, rank as u16));
            }
        }
        found
    }

    #[test]
    fn test_rank_if_exists_batch_match_on_gap_bound() {
        // Consecutive values make the gap bound tight: when searching for 14 from rank 1
        // (value 11), the bound stops the search right after rank 1 + (14 - 11) = 4, which is
        // exactly where 14 is.
        let vals: [u16; 6] = [10, 11, 12, 13, 14, 20];
        let buffer = serialize_sparse(&vals);
        let block = SparseBlockCodec::open(&buffer);
        assert_eq!(
            rank_if_exists_batch_to_vec(&block, &[10, 14, 15, 20, 21]),
            vec![(10, 0), (14, 4), (20, 5)]
        );
    }

    fn vals_needles_strategy() -> impl Strategy<Value = (Vec<u16>, Vec<u16>)> {
        // A small value domain gives small gaps between values, so the gap bound often
        // matters. A large domain exercises the case where the bound exceeds the block.
        prop_oneof![Just(200u16), Just(2_000u16), Just(u16::MAX)].prop_flat_map(|max_val| {
            proptest::collection::btree_set(0..=max_val, 0..1_000).prop_flat_map(move |vals| {
                let vals: Vec<u16> = vals.into_iter().collect();
                // Pick needles among existing values half of the time, to exercise matches.
                let needle_strategy = if vals.is_empty() {
                    (0..=max_val).boxed()
                } else {
                    prop_oneof![0..=max_val, proptest::sample::select(vals.clone())].boxed()
                };
                let needles_strategy =
                    proptest::collection::vec(needle_strategy, 0..100).prop_map(|mut needles| {
                        needles.sort_unstable();
                        needles
                    });
                (Just(vals), needles_strategy)
            })
        })
    }

    proptest! {
        #[test]
        fn test_proptest_rank_if_exists_batch((vals, sorted_needles) in vals_needles_strategy()) {
            let buffer = serialize_sparse(&vals);
            let block = SparseBlockCodec::open(&buffer);
            prop_assert_eq!(
                rank_if_exists_batch_to_vec(&block, &sorted_needles),
                expected_rank_if_exists_batch(&vals, &sorted_needles)
            );
        }
    }
}
