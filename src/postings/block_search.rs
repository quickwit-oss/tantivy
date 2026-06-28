use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

/// Returns the index of the first element in `arr` that is greater than or
/// equal to `target`.
///
/// This is equivalent to:
///
/// ```ignore
/// arr.iter().take_while(|&&val| val < target).count()
/// ```
///
/// # Assumptions
///
/// - `arr` is sorted in nondecreasing order. Values may be repeated; the last block is often padded
///   with duplicates of its final value.
/// - `target` is less than or equal to the last element in `arr`, so the result is always a valid
///   index into the block.
///
/// # `K`
///
/// `K` is the branching factor. Each reduction probes `K - 1` segment-end
/// pivots, keeps the matching segment, and finally linearly scans the remaining
/// range. `K` must be one of `2`, `4`, `8`, `16`, `32`, or `64`.
///
/// The core idea vs a traditional binary search is that we can very cheaply scan blocks of
/// numbers, since they are already in the CPU cache line.
#[inline(always)]
pub fn kary_search<const K: usize>(arr: &[u32; COMPRESSION_BLOCK_SIZE], target: u32) -> usize {
    const {
        assert!(
            matches!(K, 2 | 4 | 8 | 16 | 32 | 64),
            "K must be one of 2, 4, 8, 16, 32, or 64"
        );
    };

    let mut base = 0usize;
    let mut range = COMPRESSION_BLOCK_SIZE;

    loop {
        let step = range / K;
        if step == 0 {
            break;
        }
        debug_assert_eq!(range % K, 0);
        // Count how many segment-end pivots are < target (branchless, unrolled).
        let mut count = 0usize;
        for i in 1..K {
            count += (unsafe { *arr.get_unchecked(base + i * step - 1) } < target) as usize;
        }
        base += count * step;
        range = step;
    }

    // Linear scan over the ≤K remaining elements.
    let mut count = 0usize;
    for i in 0..range {
        count += (unsafe { *arr.get_unchecked(base + i) } < target) as usize;
    }
    base + count
}

/// entry point used by postings; implemented as an 8-ary branchless search.
#[inline]
pub fn search_block(arr: &[u32; COMPRESSION_BLOCK_SIZE], target: u32) -> usize {
    kary_search::<8>(arr, target)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::{kary_search, search_block};
    use crate::docset::TERMINATED;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

    fn search_in_block_trivial_but_slow(block: &[u32], target: u32) -> usize {
        block.iter().take_while(|&&val| val < target).count()
    }

    fn util_test_search_in_block(block: &[u32], target: u32) {
        let cursor = search_in_block_trivial_but_slow(block, target);
        assert!(cursor < COMPRESSION_BLOCK_SIZE);
        assert!(block[cursor] >= target);
        if cursor > 0 {
            assert!(block[cursor - 1] < target);
        }
        assert_eq!(block.len(), COMPRESSION_BLOCK_SIZE);
        let mut output_buffer = [TERMINATED; COMPRESSION_BLOCK_SIZE];
        output_buffer[..block.len()].copy_from_slice(block);
        assert_eq!(search_block(&output_buffer, target), cursor);
    }

    fn util_test_search_in_block_all(block: &[u32]) {
        let mut targets = HashSet::new();
        targets.insert(0);
        for &val in block {
            if val > 0 {
                targets.insert(val - 1);
            }
            targets.insert(val);
        }
        for target in targets {
            util_test_search_in_block(block, target);
        }
    }

    #[test]
    fn test_search_in_branchless_binary_search() {
        let v: Vec<u32> = (0..COMPRESSION_BLOCK_SIZE).map(|i| i as u32 * 2).collect();
        util_test_search_in_block_all(&v[..]);
    }

    #[test]
    fn test_search_in_branchless_binary_search_corner_cases() {
        let all_same = vec![7u32; COMPRESSION_BLOCK_SIZE];
        util_test_search_in_block_all(&all_same);

        let repeated_across_pivots: Vec<u32> = (0..COMPRESSION_BLOCK_SIZE)
            .map(|i| (i / 17) as u32)
            .collect();
        util_test_search_in_block_all(&repeated_across_pivots);

        let mut padded_last_block = vec![0u32; COMPRESSION_BLOCK_SIZE];
        for (i, value) in padded_last_block.iter_mut().enumerate() {
            *value = if i < COMPRESSION_BLOCK_SIZE / 2 {
                i as u32
            } else {
                TERMINATED
            };
        }
        util_test_search_in_block_all(&padded_last_block);
    }

    #[test]
    fn test_kary_search_allowed_branching_factors() {
        let mut block = [TERMINATED; COMPRESSION_BLOCK_SIZE];
        for (idx, value) in block.iter_mut().enumerate() {
            *value = (idx / 3) as u32;
        }

        for target in [0, 1, 17, block[COMPRESSION_BLOCK_SIZE - 1]] {
            let expected = search_in_block_trivial_but_slow(&block, target);
            assert_eq!(kary_search::<2>(&block, target), expected);
            assert_eq!(kary_search::<4>(&block, target), expected);
            assert_eq!(kary_search::<8>(&block, target), expected);
            assert_eq!(kary_search::<16>(&block, target), expected);
            assert_eq!(kary_search::<32>(&block, target), expected);
            assert_eq!(kary_search::<64>(&block, target), expected);
        }
    }

    fn monotonous_block() -> impl Strategy<Value = Vec<u32>> {
        prop::collection::vec(0u32..5u32, COMPRESSION_BLOCK_SIZE).prop_map(|mut deltas| {
            let mut el = 0;
            for i in 0..COMPRESSION_BLOCK_SIZE {
                el += deltas[i];
                deltas[i] = el;
            }
            deltas
        })
    }

    proptest! {
        #[test]
        fn test_proptest_branchless_binary_search(block in monotonous_block()) {
            util_test_search_in_block_all(&block[..]);
        }
    }
}
