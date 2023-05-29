/// fastcmp employs a trick to speed up the comparison of two slices of bytes.
/// It's also possible to inline compared to the memcmp call.
///
/// E.g. Comparing equality of slice length 7 in two steps, by comparing two 4 byte slices
/// unconditionally instead comparing the remaining 3 bytes if the first comparison was equal.
/// [1, 2, 3, 4, 5, 6, 7]
/// [1, 2, 3, 4]
///          [4, 5, 6, 7]
///
/// This method uses the XMM register for bytes slices bigger than 16, else regular registers.
#[inline]
pub fn fast_short_slice_compare(left: &[u8], right: &[u8]) -> bool {
    let len = left.len();
    if len != right.len() {
        return false;
    }

    // This could be less equals, but to make the job a little bit easier for the branch predictor
    // we put the length 8 into the bigger group (8-16 bytes), that compares two u64
    // assuming that range 8-16 are more common than 4-7

    // This weird branching is done on purpose to get the best assembly.
    // if len< 4 {
    // ..
    // if len < 8
    // will cause assembly inlined instead of jumps
    if len < 8 {
        if len >= 4 {
            return double_check_trick::<4>(left, right);
        } else {
            return short_compare(left, right);
        }
    }

    if len > 16 {
        return fast_nbyte_slice_compare::<16>(left, right);
    }

    double_check_trick::<8>(left, right)
}

#[inline]
// Note: The straigthforward left.chunks_exact(SIZE).zip(right.chunks_exact(SIZE)) produces slower
// assembly
fn fast_nbyte_slice_compare<const SIZE: usize>(left: &[u8], right: &[u8]) -> bool {
    let mut left_curr = left;
    let mut right_curr = right;
    loop {
        if left_curr.len() < SIZE || right_curr.len() < SIZE {
            break;
        }
        if left_curr[..SIZE] != right_curr[..SIZE] {
            return false;
        }

        left_curr = &left_curr[SIZE..];
        right_curr = &right_curr[SIZE..];
    }

    // The last SIZE bytes can be compared with a little trick in one go, by comparing the last
    // SIZE bytes. (sounds obvious if documented like that)
    // The bounds check are elided since we already checked the length above to be at least SIZE

    check_end::<SIZE>(left, right)
}

#[inline]
fn short_compare(left: &[u8], right: &[u8]) -> bool {
    for (l, r) in left.iter().zip(right) {
        if l != r {
            return false;
        }
    }
    true
}
#[inline]
fn check_end<const SIZE: usize>(left: &[u8], right: &[u8]) -> bool {
    left[left.len() - SIZE..] == right[right.len() - SIZE..]
}

#[inline]
fn double_check_trick<const SIZE: usize>(left: &[u8], right: &[u8]) -> bool {
    left[0..SIZE] == right[0..SIZE] && left[left.len() - SIZE..] == right[right.len() - SIZE..]
}

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

    proptest! {
        #[test]
        fn test_fast_short_slice_compare(left in prop::collection::vec(any::<u8>(), 0..100),
                                          right in prop::collection::vec(any::<u8>(), 0..100)) {
            let result = fast_short_slice_compare(&left, &right);
            let expected = left == right;
            prop_assert_eq!(result, expected, "left: {:?}, right: {:?}", left, right);
        }

        #[test]
        fn test_fast_short_slice_compare_equal(left in prop::collection::vec(any::<u8>(), 0..100),
                                          ) {
            let result = fast_short_slice_compare(&left, &left);
            let expected = left == left;
            prop_assert_eq!(result, expected, "left: {:?}, right: {:?}", left, left);
        }

    }
}
