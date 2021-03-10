use std::ops::Range;

use crate::postings::compression::AlignedBuffer;

/// This modules define the logic used to search for a doc in a given
/// block. (at most 128 docs)
///
/// Searching within a block is a hotspot when running intersection.
/// so it was worth defining it in its own module.

#[cfg(target_arch = "x86_64")]
mod sse2 {
    use crate::postings::compression::{AlignedBuffer, COMPRESSION_BLOCK_SIZE};
    use std::arch::x86_64::__m128i as DataType;
    use std::arch::x86_64::_mm_add_epi32 as op_add;
    use std::arch::x86_64::_mm_cmplt_epi32 as op_lt;
    use std::arch::x86_64::_mm_load_si128 as op_load; // requires 128-bits alignment
    use std::arch::x86_64::_mm_set1_epi32 as set1;
    use std::arch::x86_64::_mm_setzero_si128 as set0;
    use std::arch::x86_64::_mm_sub_epi32 as op_sub;
    use std::arch::x86_64::{_mm_cvtsi128_si32, _mm_shuffle_epi32};

    const MASK1: i32 = 78;
    const MASK2: i32 = 177;

    /// Performs an exhaustive linear search over the
    ///
    /// There is no early exit here. We simply count the
    /// number of elements that are `< target`.
    pub(crate) fn linear_search_sse2_128(arr: &AlignedBuffer, target: u32) -> usize {
        unsafe {
            let ptr = arr as *const AlignedBuffer as *const DataType;
            let vkey = set1(target as i32);
            let mut cnt = set0();
            // We work over 4 `__m128i` at a time.
            // A single `__m128i` actual contains 4 `u32`.
            for i in 0..(COMPRESSION_BLOCK_SIZE as isize) / (4 * 4) {
                let cmp1 = op_lt(op_load(ptr.offset(i * 4)), vkey);
                let cmp2 = op_lt(op_load(ptr.offset(i * 4 + 1)), vkey);
                let cmp3 = op_lt(op_load(ptr.offset(i * 4 + 2)), vkey);
                let cmp4 = op_lt(op_load(ptr.offset(i * 4 + 3)), vkey);
                let sum = op_add(op_add(cmp1, cmp2), op_add(cmp3, cmp4));
                cnt = op_sub(cnt, sum);
            }
            cnt = op_add(cnt, _mm_shuffle_epi32(cnt, MASK1));
            cnt = op_add(cnt, _mm_shuffle_epi32(cnt, MASK2));
            _mm_cvtsi128_si32(cnt) as usize
        }
    }

    #[cfg(test)]
    mod test {
        use super::linear_search_sse2_128;
        use crate::postings::compression::{AlignedBuffer, COMPRESSION_BLOCK_SIZE};

        #[test]
        fn test_linear_search_sse2_128_u32() {
            let mut block = [0u32; COMPRESSION_BLOCK_SIZE];
            for el in 0u32..128u32 {
                block[el as usize] = el * 2 + 1 << 18;
            }
            let target = block[64] + 1;
            assert_eq!(linear_search_sse2_128(&AlignedBuffer(block), target), 65);
        }
    }
}

/// This `linear search` browser exhaustively through the array.
/// but the early exit is very difficult to predict.
///
/// Coupled with `exponential search` this function is likely
/// to be called with the same `len`
fn linear_search(arr: &[u32], target: u32) -> usize {
    arr.iter().map(|&el| if el < target { 1 } else { 0 }).sum()
}

fn exponential_search(arr: &[u32], target: u32) -> Range<usize> {
    let end = arr.len();
    let mut begin = 0;
    for &pivot in &[1, 3, 7, 15, 31, 63] {
        if pivot >= end {
            break;
        }
        if arr[pivot] > target {
            return begin..pivot;
        }
        begin = pivot;
    }
    begin..end
}

#[inline(never)]
fn galloping(block_docs: &[u32], target: u32) -> usize {
    let range = exponential_search(&block_docs, target);
    range.start + linear_search(&block_docs[range], target)
}

/// Tantivy may rely on SIMD instructions to search for a specific document within
/// a given block.
#[derive(Clone, Copy, PartialEq)]
pub enum BlockSearcher {
    #[cfg(target_arch = "x86_64")]
    SSE2,
    Scalar,
}

impl BlockSearcher {
    /// Search the first index containing an element greater or equal to
    /// the target.
    ///
    /// The results should be equivalent to
    /// ```compile_fail
    /// block[..]
    //       .iter()
    //       .take_while(|&&val| val < target)
    //       .count()
    /// ```
    ///
    /// The `start` argument is just used to hint that the response is
    /// greater than beyond `start`. The implementation may or may not use
    /// it for optimization.
    ///
    /// # Assumption
    ///
    /// The array len is > start.
    /// The block is sorted
    /// The target is assumed greater or equal to the `arr[start]`.
    /// The target is assumed smaller or equal to the last element of the block.
    ///
    /// Currently the scalar implementation starts by an exponential search, and
    /// then operates a linear search in the result subarray.
    ///
    /// If SSE2 instructions are available in the `(platform, running CPU)`,
    /// then we use a different implementation that does an exhaustive linear search over
    /// the block regardless of whether the block is full or not.
    ///
    /// Indeed, if the block is not full, the remaining items are TERMINATED.
    /// It is surprisingly faster, most likely because of the lack of branch misprediction.
    pub(crate) fn search_in_block(self, block_docs: &AlignedBuffer, target: u32) -> usize {
        #[cfg(target_arch = "x86_64")]
        {
            if self == BlockSearcher::SSE2 {
                return sse2::linear_search_sse2_128(block_docs, target);
            }
        }
        galloping(&block_docs.0[..], target)
    }
}

impl Default for BlockSearcher {
    fn default() -> BlockSearcher {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse2") {
                return BlockSearcher::SSE2;
            }
        }
        BlockSearcher::Scalar
    }
}

#[cfg(test)]
mod tests {
    use super::exponential_search;
    use super::linear_search;
    use super::BlockSearcher;
    use crate::docset::TERMINATED;
    use crate::postings::compression::{AlignedBuffer, COMPRESSION_BLOCK_SIZE};

    #[test]
    fn test_linear_search() {
        let len: usize = 50;
        let arr: Vec<u32> = (0..len).map(|el| 1u32 + (el as u32) * 2).collect();
        for target in 1..*arr.last().unwrap() {
            let res = linear_search(&arr[..], target);
            if res > 0 {
                assert!(arr[res - 1] < target);
            }
            if res < len {
                assert!(arr[res] >= target);
            }
        }
    }

    #[test]
    fn test_exponentiel_search() {
        assert_eq!(exponential_search(&[1, 2], 0), 0..1);
        assert_eq!(exponential_search(&[1, 2], 1), 0..1);
        assert_eq!(
            exponential_search(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 7),
            3..7
        );
    }

    fn util_test_search_in_block(block_searcher: BlockSearcher, block: &[u32], target: u32) {
        let cursor = search_in_block_trivial_but_slow(block, target);
        assert!(block.len() < COMPRESSION_BLOCK_SIZE);
        let mut output_buffer = [TERMINATED; COMPRESSION_BLOCK_SIZE];
        output_buffer[..block.len()].copy_from_slice(block);
        assert_eq!(
            block_searcher.search_in_block(&AlignedBuffer(output_buffer), target),
            cursor
        );
    }

    fn util_test_search_in_block_all(block_searcher: BlockSearcher, block: &[u32]) {
        use std::collections::HashSet;
        let mut targets = HashSet::new();
        for (i, val) in block.iter().cloned().enumerate() {
            if i > 0 {
                targets.insert(val - 1);
            }
            targets.insert(val);
        }
        for target in targets {
            util_test_search_in_block(block_searcher, block, target);
        }
    }

    fn search_in_block_trivial_but_slow(block: &[u32], target: u32) -> usize {
        block.iter().take_while(|&&val| val < target).count()
    }

    fn test_search_in_block_util(block_searcher: BlockSearcher) {
        for len in 1u32..128u32 {
            let v: Vec<u32> = (0..len).map(|i| i * 2).collect();
            util_test_search_in_block_all(block_searcher, &v[..]);
        }
    }

    #[test]
    fn test_search_in_block_scalar() {
        test_search_in_block_util(BlockSearcher::Scalar);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_search_in_block_sse2() {
        test_search_in_block_util(BlockSearcher::SSE2);
    }
}
