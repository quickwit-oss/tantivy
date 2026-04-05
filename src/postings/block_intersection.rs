/// SIMD-friendly intersection of two sorted `u32` slices.
///
/// Given two sorted slices `a` and `b`, writes their intersection
/// into `output` and returns the number of elements written.
///
/// This is useful for intersecting decompressed posting list blocks
/// (128 elements each) without per-element `seek()` overhead.
///
/// # Arguments
/// - `a`, `b` — sorted slices of doc IDs (may contain duplicates)
/// - `output` — destination buffer, must be at least `min(a.len(), b.len())`
///
/// # Returns
/// Number of elements written to `output`.
#[inline]
pub fn intersect_sorted_u32(a: &[u32], b: &[u32], output: &mut [u32]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && a.len() >= 8 && b.len() >= 8 {
            // SAFETY: Runtime feature detection guarantees AVX2 is available
            return unsafe { intersect_sorted_avx2(a, b, output) };
        }
    }

    intersect_sorted_scalar(a, b, output)
}

/// Scalar merge-based intersection of two sorted slices.
///
/// Uses a two-pointer merge with galloping search for skipping
/// large gaps, which is faster than linear merge when one list
/// is much sparser than the other.
#[inline]
pub fn intersect_sorted_scalar(a: &[u32], b: &[u32], output: &mut [u32]) -> usize {
    let mut i = 0;
    let mut j = 0;
    let mut out = 0;
    let mut last_written: u32 = u32::MAX; // sentinel: no value written yet

    while i < a.len() && j < b.len() {
        let va = a[i];
        let vb = b[j];
        if va == vb {
            if va != last_written {
                output[out] = va;
                out += 1;
                last_written = va;
            }
            i += 1;
            j += 1;
        } else if va < vb {
            // Galloping: skip ahead in `a` to find va >= vb
            i = gallop(&a[i..], vb) + i;
        } else {
            // Galloping: skip ahead in `b` to find vb >= va
            j = gallop(&b[j..], va) + j;
        }
    }

    out
}

/// Galloping (exponential) search: finds the index of the first
/// element >= `target` in a sorted slice.
///
/// Returns the index relative to the start of `arr`.
#[inline]
fn gallop(arr: &[u32], target: u32) -> usize {
    if arr.is_empty() || arr[0] >= target {
        return 0;
    }

    // Exponential search for the boundary
    let mut bound = 1;
    while bound < arr.len() && arr[bound] < target {
        bound *= 2;
    }
    let upper = bound.min(arr.len());
    let lower = bound / 2;

    // Binary search within [lower, upper)
    match arr[lower..upper].binary_search(&target) {
        Ok(pos) => lower + pos,
        Err(pos) => lower + pos,
    }
}

/// AVX2 SIMD intersection of two sorted u32 slices.
///
/// Uses 8-wide SIMD registers to batch-compare elements from both arrays.
/// Falls back to scalar for the tail elements.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn intersect_sorted_avx2(a: &[u32], b: &[u32], output: &mut [u32]) -> usize {
    use std::arch::x86_64::*;

    let mut i = 0usize;
    let mut j = 0usize;
    let mut out = 0usize;

    // Process with SIMD while both arrays have enough elements
    // We use a scalar merge with SIMD-accelerated skip
    while i + 8 <= a.len() && j + 8 <= b.len() {
        let va_max = a[i + 7];
        let vb_max = b[j + 7];

        if va_max < b[j] {
            // Entire a block is below b's current — skip
            i += 8;
            continue;
        }
        if vb_max < a[i] {
            // Entire b block is below a's current — skip
            j += 8;
            continue;
        }

        // Load 8 elements from each
        let va = _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i);

        // For each element in a[i..i+8], check if it exists in b[j..j+8]
        // We broadcast each a element and compare against all 8 b elements
        let vb = _mm256_loadu_si256(b.as_ptr().add(j) as *const __m256i);

        // Track last written value to deduplicate
        let mut last_written: u32 = if out > 0 { output[out - 1] } else { u32::MAX };
        for k in 0..8 {
            let a_elem = a[i + k];
            if a_elem == last_written {
                continue; // Skip duplicate from `a`
            }
            let va_broadcast = _mm256_set1_epi32(a_elem as i32);
            let cmp = _mm256_cmpeq_epi32(va_broadcast, vb);
            let mask = _mm256_movemask_epi8(cmp);
            if mask != 0 {
                output[out] = a_elem;
                out += 1;
                last_written = a_elem;
            }
        }

        // Advance the pointer with the smaller max
        if va_max <= vb_max {
            i += 8;
        }
        if vb_max <= va_max {
            j += 8;
        }
    }

    // Scalar tail with dedup
    let mut last_written_tail: u32 = if out > 0 { output[out - 1] } else { u32::MAX };
    while i < a.len() && j < b.len() {
        let va = a[i];
        let vb = b[j];
        if va == vb {
            if va != last_written_tail {
                output[out] = va;
                out += 1;
                last_written_tail = va;
            }
            i += 1;
            j += 1;
        } else if va < vb {
            i += 1;
        } else {
            j += 1;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersect_empty() {
        let mut out = [0u32; 10];
        assert_eq!(intersect_sorted_u32(&[], &[1, 2, 3], &mut out), 0);
        assert_eq!(intersect_sorted_u32(&[1, 2, 3], &[], &mut out), 0);
        assert_eq!(intersect_sorted_u32(&[], &[], &mut out), 0);
    }

    #[test]
    fn test_intersect_no_overlap() {
        let mut out = [0u32; 10];
        let n = intersect_sorted_u32(&[1, 3, 5], &[2, 4, 6], &mut out);
        assert_eq!(n, 0);
    }

    #[test]
    fn test_intersect_full_overlap() {
        let mut out = [0u32; 10];
        let n = intersect_sorted_u32(&[1, 2, 3], &[1, 2, 3], &mut out);
        assert_eq!(n, 3);
        assert_eq!(&out[..3], &[1, 2, 3]);
    }

    #[test]
    fn test_intersect_partial_overlap() {
        let mut out = [0u32; 10];
        let n = intersect_sorted_u32(&[1, 3, 5, 7, 9], &[2, 3, 5, 8, 9], &mut out);
        assert_eq!(n, 3);
        assert_eq!(&out[..3], &[3, 5, 9]);
    }

    #[test]
    fn test_intersect_one_element() {
        let mut out = [0u32; 10];
        let n = intersect_sorted_u32(&[5], &[5], &mut out);
        assert_eq!(n, 1);
        assert_eq!(out[0], 5);
    }

    #[test]
    fn test_intersect_large_gap() {
        let a: Vec<u32> = (0..100).collect();
        let b: Vec<u32> = (90..200).collect();
        let mut out = vec![0u32; 100];
        let n = intersect_sorted_u32(&a, &b, &mut out);
        assert_eq!(n, 10);
        let expected: Vec<u32> = (90..100).collect();
        assert_eq!(&out[..n], &expected);
    }

    #[test]
    fn test_intersect_large_blocks() {
        // Simulate two 128-element posting list blocks
        let a: Vec<u32> = (0..256).step_by(2).collect(); // even numbers
        let b: Vec<u32> = (0..256).step_by(3).collect(); // multiples of 3
        let mut out = vec![0u32; 128];
        let n = intersect_sorted_u32(&a, &b, &mut out);
        let expected: Vec<u32> = (0..256).filter(|x| x % 2 == 0 && x % 3 == 0).collect();
        assert_eq!(n, expected.len());
        assert_eq!(&out[..n], &expected);
    }

    #[test]
    fn test_intersect_scalar_matches_reference() {
        let a: Vec<u32> = vec![1, 5, 10, 15, 20, 25, 30, 35, 40, 50, 100];
        let b: Vec<u32> = vec![2, 5, 7, 15, 22, 25, 35, 50, 60, 100, 200];
        let mut out_scalar = vec![0u32; 20];
        let mut out_ref = vec![0u32; 20];

        let n_scalar = intersect_sorted_scalar(&a, &b, &mut out_scalar);

        // Reference implementation
        let expected: Vec<u32> = a.iter().filter(|x| b.contains(x)).cloned().collect();
        let n_ref = expected.len();
        out_ref[..n_ref].copy_from_slice(&expected);

        assert_eq!(n_scalar, n_ref);
        assert_eq!(&out_scalar[..n_scalar], &out_ref[..n_ref]);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_intersect_avx2_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }
        // Large enough to exercise AVX2 path
        let a: Vec<u32> = (0..256).step_by(2).collect();
        let b: Vec<u32> = (0..256).step_by(3).collect();
        let mut out_simd = vec![0u32; 256];
        let mut out_scalar = vec![0u32; 256];

        let n_simd = intersect_sorted_u32(&a, &b, &mut out_simd);
        let n_scalar = intersect_sorted_scalar(&a, &b, &mut out_scalar);

        assert_eq!(n_simd, n_scalar);
        assert_eq!(&out_simd[..n_simd], &out_scalar[..n_scalar]);
    }

    #[test]
    fn test_intersect_with_duplicates_in_a() {
        // Regression test: a contains duplicates, b contains the value once
        let a = vec![1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut out = vec![0u32; 20];
        let n = intersect_sorted_u32(&a, &b, &mut out);
        // Should output [1, 2, 3, 4, 5, 6, 7, 8, 9] — no duplicate 1s
        let expected: Vec<u32> = (1..=9).collect();
        assert_eq!(n, expected.len(), "output count mismatch with duplicates in a");
        assert_eq!(&out[..n], &expected);
    }

    #[test]
    fn test_intersect_with_duplicates_in_both() {
        let a = vec![5, 5, 5, 5, 10, 10, 10, 10, 20, 20, 20, 20, 30, 30, 30, 30];
        let b = vec![5, 5, 10, 10, 15, 15, 20, 20, 25, 25, 30, 30, 35, 35, 40, 40];
        let mut out = vec![0u32; 20];
        let n = intersect_sorted_u32(&a, &b, &mut out);
        assert_eq!(&out[..n], &[5, 10, 20, 30]);
    }
}
