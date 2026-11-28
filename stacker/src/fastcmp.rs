/// fastcmp employs a trick to speed up the comparison of two slices of bytes.
/// It's also possible to inline compared to the memcmp call.
///
/// E.g. Comparing equality of slice length 7 in two steps, by comparing two 4 byte slices
/// unconditionally instead comparing the remaining 3 bytes if the first comparison was equal.
/// [1, 2, 3, 4, 5, 6, 7]
/// [1, 2, 3, 4]
///          [4, 5, 6, 7]
///
/// This method uses wide 16/32-byte loads for byte slices bigger than 16, else regular registers.
#[inline]
pub fn fast_short_slice_compare(left: &[u8], right: &[u8]) -> bool {
    let len = left.len();
    if len != right.len() {
        return false;
    }

    if len < 8 {
        if len >= 4 {
            return double_eq_u32(left, right);
        }
        return short_compare(left, right);
    }

    if len <= 16 {
        return double_eq_u64(left, right);
    }

    #[cfg(target_arch = "aarch64")]
    {
        if len <= 32 {
            return neon_double_eq_16(left, right);
        }
        neon_compare_gt32(left, right)
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        if len <= 32 {
            return double_eq_u128(left, right);
        }
        fast_nbyte_slice_compare::<16>(left, right)
    }
}

/// NEON-accelerated overlapping 16-byte equality comparison for `16 < len <= 32`.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn neon_double_eq_16(left: &[u8], right: &[u8]) -> bool {
    use std::arch::aarch64::{veorq_u8, vld1q_u8, vmaxvq_u32, vorrq_u8, vreinterpretq_u32_u8};

    debug_assert_eq!(left.len(), right.len());
    debug_assert!(left.len() >= 16);

    let tail = left.len() - 16;
    let lp = left.as_ptr();
    let rp = right.as_ptr();
    // SAFETY: `left.len() == right.len() >= 16`, so `0..16` and `tail..left.len()` are in bounds
    // and cover `0..left.len()`; `vld1q_u8` supports arbitrary byte alignment on aarch64.
    unsafe {
        let d0 = veorq_u8(vld1q_u8(lp), vld1q_u8(rp));
        let d1 = veorq_u8(vld1q_u8(lp.add(tail)), vld1q_u8(rp.add(tail)));
        vmaxvq_u32(vreinterpretq_u32_u8(vorrq_u8(d0, d1))) == 0
    }
}

/// Compares 64 bytes at `lp..lp+64` against `rp..rp+64` using four 128-bit NEON loads.
///
/// # Safety
/// Both `lp` and `rp` must be valid for reads of 64 bytes.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon_eq_64(lp: *const u8, rp: *const u8) -> bool {
    use std::arch::aarch64::{veorq_u8, vld1q_u8, vmaxvq_u32, vorrq_u8, vreinterpretq_u32_u8};

    // SAFETY: Caller guarantees 64 readable bytes from both `lp` and `rp`; `vld1q_u8` supports
    // arbitrary byte alignment on aarch64.
    unsafe {
        let d0 = veorq_u8(vld1q_u8(lp), vld1q_u8(rp));
        let d1 = veorq_u8(vld1q_u8(lp.add(16)), vld1q_u8(rp.add(16)));
        let d2 = veorq_u8(vld1q_u8(lp.add(32)), vld1q_u8(rp.add(32)));
        let d3 = veorq_u8(vld1q_u8(lp.add(48)), vld1q_u8(rp.add(48)));
        let diff = vorrq_u8(vorrq_u8(d0, d1), vorrq_u8(d2, d3));
        vmaxvq_u32(vreinterpretq_u32_u8(diff)) == 0
    }
}

/// NEON-accelerated equality comparison for equal-length slices with `len > 32`.
/// Kept out-of-line so the `> 32B` loop does not bloat callers' hot short-key probe loops.
#[cfg(target_arch = "aarch64")]
#[inline(never)]
fn neon_compare_gt32(left: &[u8], right: &[u8]) -> bool {
    use std::arch::aarch64::{veorq_u8, vld1q_u8, vmaxvq_u32, vorrq_u8, vreinterpretq_u32_u8};

    debug_assert_eq!(left.len(), right.len());
    debug_assert!(left.len() > 32);

    let len = left.len();
    let lp = left.as_ptr();
    let rp = right.as_ptr();

    // SAFETY: All pointer reads below are strictly within `0..len`, where `len == left.len() ==
    // right.len()` and `vld1q_u8` supports arbitrary byte alignment on aarch64.
    unsafe {
        if len <= 64 {
            // `len > 32`, so `0..32` and `(len - 32)..len` are in bounds and cover `0..len`.
            let tail = len - 32;
            let lt = lp.add(tail);
            let rt = rp.add(tail);
            let d0 = veorq_u8(vld1q_u8(lp), vld1q_u8(rp));
            let d1 = veorq_u8(vld1q_u8(lp.add(16)), vld1q_u8(rp.add(16)));
            let d2 = veorq_u8(vld1q_u8(lt), vld1q_u8(rt));
            let d3 = veorq_u8(vld1q_u8(lt.add(16)), vld1q_u8(rt.add(16)));
            let diff = vorrq_u8(vorrq_u8(d0, d1), vorrq_u8(d2, d3));
            return vmaxvq_u32(vreinterpretq_u32_u8(diff)) == 0;
        }

        // `len > 64`, so `last = len - 64 > 0`. Each iteration checks 64 bytes at `i..i + 64`,
        // and the final overlapping check covers `last..len`.
        let last = len - 64;
        let mut i = 0;
        loop {
            if !neon_eq_64(lp.add(i), rp.add(i)) {
                return false;
            }
            i += 64;
            if i >= last {
                break;
            }
        }
        neon_eq_64(lp.add(last), rp.add(last))
    }
}

// Note: The straightforward left.chunks_exact(SIZE).zip(right.chunks_exact(SIZE)) produces slower
// assembly.
/// Compares two equal-length slices of length `>= SIZE` in `SIZE`-byte steps.
#[cfg(any(not(target_arch = "aarch64"), test))]
#[inline]
pub fn fast_nbyte_slice_compare<const SIZE: usize>(left: &[u8], right: &[u8]) -> bool {
    debug_assert_eq!(left.len(), right.len());
    debug_assert!(left.len() >= SIZE);
    let last = left.len() - SIZE;
    let mut i = 0;
    loop {
        // SAFETY: `i <= last == left.len() - SIZE`, so `i + SIZE <= left.len() == right.len()`.
        if unsafe { left.get_unchecked(i..i + SIZE) != right.get_unchecked(i..i + SIZE) } {
            return false;
        }
        i += SIZE;
        if i >= last {
            break;
        }
    }
    // SAFETY: `last == left.len() - SIZE` and `left.len() == right.len() >= SIZE`.
    unsafe { left.get_unchecked(last..) == right.get_unchecked(last..) }
}

#[inline(always)]
fn short_compare(left: &[u8], right: &[u8]) -> bool {
    debug_assert_eq!(left.len(), right.len());
    debug_assert!(left.len() < 4);
    let len = left.len();
    if len >= 2 {
        return double_eq_u16(left, right);
    }
    len == 0 || left[0] == right[0]
}

macro_rules! define_double_eq {
    ($fn_name:ident, $int_ty:ty, $size:literal) => {
        #[inline(always)]
        fn $fn_name(left: &[u8], right: &[u8]) -> bool {
            debug_assert_eq!(left.len(), right.len());
            debug_assert!(left.len() >= $size);
            let tail = left.len() - $size;
            let lh = <$int_ty>::from_ne_bytes(left[..$size].try_into().unwrap());
            let rh = <$int_ty>::from_ne_bytes(right[..$size].try_into().unwrap());
            let lt = <$int_ty>::from_ne_bytes(left[tail..tail + $size].try_into().unwrap());
            let rt = <$int_ty>::from_ne_bytes(right[tail..tail + $size].try_into().unwrap());
            ((lh ^ rh) | (lt ^ rt)) == 0
        }
    };
}

define_double_eq!(double_eq_u16, u16, 2);
define_double_eq!(double_eq_u32, u32, 4);
define_double_eq!(double_eq_u64, u64, 8);
#[cfg(not(target_arch = "aarch64"))]
define_double_eq!(double_eq_u128, u128, 16);

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_slice_compare_bytes_len_8() {
        let a = &[1, 2, 3, 4, 5, 6, 7, 8];
        let b = &[1, 2, 3, 4, 5, 6, 7, 8];
        let c = &[1, 2, 3, 4, 5, 6, 7, 7];

        assert!(fast_short_slice_compare(a, b));
        assert!(!fast_short_slice_compare(a, c));
    }

    #[test]
    fn test_slice_compare_bytes_len_9() {
        let a = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
        let c = &[0, 2, 3, 4, 5, 6, 7, 8, 9];

        assert!(fast_short_slice_compare(a, b));
        assert!(!fast_short_slice_compare(a, c));
    }

    #[test]
    fn test_slice_compare_bytes_len_16() {
        let a = &[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        let b = &[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        let c = &[1, 2, 3, 4, 5, 6, 7, 7, 1, 2, 3, 4, 5, 6, 7, 8];

        assert!(fast_short_slice_compare(a, b));
        assert!(!fast_short_slice_compare(a, c));
    }

    #[test]
    fn test_slice_compare_bytes_short() {
        let a = &[1, 2, 3, 4];
        let b = &[1, 2, 3, 4];
        assert!(fast_short_slice_compare(a, b));

        let a = &[1, 2, 3];
        let b = &[1, 2, 3];
        assert!(fast_short_slice_compare(a, b));

        let a = &[1, 2];
        let b = &[1, 2];
        assert!(fast_short_slice_compare(a, b));
    }

    #[test]
    fn test_single_byte_diff_every_position() {
        for len in 0usize..=130 {
            let a: Vec<u8> = (0usize..len)
                .map(|i| (i.wrapping_mul(29) ^ 0x5A) as u8)
                .collect();
            assert!(fast_short_slice_compare(&a, &a));
            for diff_idx in 0..len {
                let mut b = a.clone();
                b[diff_idx] ^= 0x01;
                assert!(
                    !fast_short_slice_compare(&a, &b),
                    "len={len} diff_idx={diff_idx}"
                );
                assert!(
                    !fast_short_slice_compare(&b, &a),
                    "len={len} diff_idx={diff_idx}"
                );
            }
        }
    }

    #[test]
    fn test_different_lengths_shared_prefix() {
        for len in 0usize..=130 {
            let a: Vec<u8> = (0usize..len)
                .map(|i| (i.wrapping_mul(17) ^ 0x33) as u8)
                .collect();
            let mut longer = a.clone();
            longer.push(0);
            assert!(!fast_short_slice_compare(&a, &longer));
            assert!(!fast_short_slice_compare(&longer, &a));
        }
    }

    #[test]
    fn test_fast_nbyte_slice_compare_all_boundaries() {
        fn check_size<const SIZE: usize>() {
            for len in SIZE..=130 {
                let a: Vec<u8> = (0usize..len)
                    .map(|i| (i.wrapping_mul(41) ^ 0xA5) as u8)
                    .collect();
                assert!(fast_nbyte_slice_compare::<SIZE>(&a, &a));
                for diff_idx in 0..len {
                    let mut b = a.clone();
                    b[diff_idx] ^= 0x80;
                    assert!(
                        !fast_nbyte_slice_compare::<SIZE>(&a, &b),
                        "SIZE={SIZE} len={len} diff_idx={diff_idx}"
                    );
                    assert!(
                        !fast_nbyte_slice_compare::<SIZE>(&b, &a),
                        "SIZE={SIZE} len={len} diff_idx={diff_idx}"
                    );
                }
            }
        }
        check_size::<8>();
        check_size::<16>();
        check_size::<32>();
    }

    #[test]
    fn test_long_json_keys_common_prefix() {
        let prefix = b"attributes\x01k8s\x01container\x01pod_name\x01prod-us-east-1-";
        for suffix_len in 1usize..=80 {
            let mut a = prefix.to_vec();
            a.extend((0..suffix_len).map(|i| b'a' + (i % 26) as u8));
            assert!(fast_short_slice_compare(&a, &a));
            let mut b = a.clone();
            let last = b.len() - 1;
            b[last] ^= 0x01;
            assert!(!fast_short_slice_compare(&a, &b));
        }
    }

    proptest! {
        #[test]
        fn test_fast_short_slice_compare(left in prop::collection::vec(any::<u8>(), 0..100),
                                          right in prop::collection::vec(any::<u8>(), 0..100)) {
            let result = fast_short_slice_compare(&left, &right);
            let expected = left == right;
            prop_assert_eq!(result, expected, "left: {:?}, right: {:?}", left, right);
        }

        #[test]
        fn test_fast_short_slice_compare_equal(left in prop::collection::vec(any::<u8>(), 0..100)) {
            let result = fast_short_slice_compare(&left, &left);
            let expected = left == left;
            prop_assert_eq!(result, expected, "left: {:?}, right: {:?}", left, left);
        }

        #[test]
        fn test_fast_short_slice_compare_single_bit_flip(
            left in prop::collection::vec(any::<u8>(), 1..160),
            pos in any::<prop::sample::Index>(),
            bit in 0u8..8,
        ) {
            let mut right = left.clone();
            let idx = pos.index(right.len());
            right[idx] ^= 1u8 << bit;
            prop_assert!(!fast_short_slice_compare(&left, &right));
        }
    }
}
