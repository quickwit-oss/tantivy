use postings::compression::COMPRESSION_BLOCK_SIZE;

#[cfg(target_arch = "x86_64")]
mod sse2 {
    use std::arch::x86_64::__m128i as DataType;
    use std::arch::x86_64::_mm_setzero_si128 as set0;
    use std::arch::x86_64::_mm_set1_epi32 as set1;
    use std::arch::x86_64::_mm_cmplt_epi32 as op_cmp;
    use std::arch::x86_64::_mm_add_epi32 as op_add;
    use std::arch::x86_64::_mm_sub_epi32 as op_sub;
    use std::arch::x86_64::_mm_load_si128 as op_load;
    use std::arch::x86_64::{_mm_shuffle_epi32, _mm_cvtsi128_si32};

    const MASK1: i32 = 78;
    const MASK2: i32 = 177;

    pub fn linear_search_sse2_128(arr: &[u32], target: u32) -> usize {
        unsafe {
            let ptr = arr.as_ptr() as *const DataType;
            let vkey = set1(target as i32);
            let mut cnt = set0();
            for i in 0..(128 / 16) {
                let cmp1 = op_cmp(op_load(ptr.offset(i*4)), vkey);
                let cmp2 = op_cmp(op_load(ptr.offset(i*4 + 1)), vkey);
                let cmp3 = op_cmp(op_load(ptr.offset(i*4 + 2)), vkey);
                let cmp4 = op_cmp(op_load(ptr.offset(i*4 + 3)), vkey);
                let sum = op_add(op_add(cmp1, cmp2), op_add(cmp3, cmp4));
                cnt = op_sub(cnt, sum);
            }
            cnt = op_add(cnt, _mm_shuffle_epi32(cnt, MASK1));
            cnt = op_add(cnt, _mm_shuffle_epi32(cnt, MASK2));
            _mm_cvtsi128_si32(cnt) as usize
        }
    }
}


/// This `linear search` browser exhaustively through the array.
/// but the early exit is very difficult to predict.
///
/// Coupled with `exponential search` this function is likely
/// to be called with the same `len`
fn linear_search(arr: &[u32], target: u32) -> usize {
    arr.iter()
        .map(|&el| if el < target { 1 } else { 0 })
        .sum()
}

fn exponential_search(arr: &[u32], target: u32) -> (usize, usize) {
    let end = arr.len();
    let mut begin = 0;
    for &pivot in &[1, 3, 7, 15, 31, 63] {
        if pivot >= end {
            break;
        }
        if arr[pivot] > target {
            return (begin, pivot);
        }
        begin = pivot;
    }
    (begin, end)
}

fn galloping(block_docs: &[u32], target: u32) -> usize {
    let (start, end) = exponential_search(&block_docs, target);
    start + linear_search(&block_docs[start..end], target)
}


/// Tantivy may rely on SIMD instructions to search for a specific document within
/// a given block.
#[derive(Clone, Copy, PartialEq)]
pub enum BlockSearcher {
    #[cfg(target_arch = "x86_64")]
    SSE2,
    Scalar
}

impl BlockSearcher {
    /// Search the first index containing an element greater or equal to
    /// the target.
    ///
    /// # Assumption
    ///
    /// The array is assumed non empty.
    /// The target is assumed greater or equal to the first element.
    /// The target is assumed smaller or equal to the last element.
    pub fn search_in_block(&self, block_docs: &[u32], start: usize, target: u32) -> usize {
        #[cfg(target_arch = "x86_64")]
        {
            if *self == BlockSearcher::SSE2 {
                if block_docs.len() == COMPRESSION_BLOCK_SIZE {
                    return sse2::linear_search_sse2_128(block_docs, target);
                }
            }
        }
        start + galloping(&block_docs[start..], target)
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
    use super::linear_search;
    use super::exponential_search;
    use super::BlockSearcher;

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
        assert_eq!(exponential_search(&[1, 2], 0), (0, 1));
        assert_eq!(exponential_search(&[1, 2], 1), (0, 1));
        assert_eq!(
            exponential_search(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 7),
            (3, 7)
        );
    }

    fn util_test_search_in_block(block_searcher: BlockSearcher, block: &[u32], target: u32) {
        let cursor = search_in_block_trivial_but_slow(block, target);
        for i in 0..cursor {
            assert_eq!(
                block_searcher.search_in_block(block, i, target),
                cursor
            );
        }
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
        block
            .iter()
            .cloned()
            .enumerate()
            .filter(|&(_, ref val)| *val >= target)
            .next()
            .unwrap()
            .0
    }

    #[test]
    fn test_search_in_block_scalar() {
        let block_search = BlockSearcher::Scalar;
        for len in 1u32..128u32 {
            let v: Vec<u32> = (0..len).map(|i| i * 2).collect();
            util_test_search_in_block_all(block_search, &v[..]);
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_search_in_block_sse2() {
        let block_search = BlockSearcher::SSE2;
        for len in 1u32..128u32 {
            let v: Vec<u32> = (0..len).map(|i| i * 2).collect();
            util_test_search_in_block_all(block_search, &v[..]);
        }
    }
}