use std::num::NonZeroU64;

use fastdivide::DividerU64;

/// Compute the gcd of two non null numbers.
///
/// It is recommended, but not required, to feed values such that `large >= small`.
fn compute_gcd(mut large: NonZeroU64, mut small: NonZeroU64) -> NonZeroU64 {
    loop {
        let rem: u64 = large.get() % small;
        if let Some(new_small) = NonZeroU64::new(rem) {
            (large, small) = (small, new_small);
        } else {
            return small;
        }
    }
}

// Find GCD for iterator of numbers
pub fn find_gcd(numbers: impl Iterator<Item = u64>) -> Option<NonZeroU64> {
    let mut numbers = numbers.flat_map(NonZeroU64::new);
    let mut gcd: NonZeroU64 = numbers.next()?;
    if gcd.get() == 1 {
        return Some(gcd);
    }

    let mut gcd_divider = DividerU64::divide_by(gcd.get());
    for val in numbers {
        let remainder = val.get() - (gcd_divider.divide(val.get())) * gcd.get();
        if remainder == 0 {
            continue;
        }
        gcd = compute_gcd(val, gcd);
        if gcd.get() == 1 {
            return Some(gcd);
        }

        gcd_divider = DividerU64::divide_by(gcd.get());
    }
    Some(gcd)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::column_values::gcd::{compute_gcd, find_gcd};

    #[test]
    fn test_compute_gcd() {
        let test_compute_gcd_aux = |large, small, expected| {
            let large = NonZeroU64::new(large).unwrap();
            let small = NonZeroU64::new(small).unwrap();
            let expected = NonZeroU64::new(expected).unwrap();
            assert_eq!(compute_gcd(small, large), expected);
            assert_eq!(compute_gcd(large, small), expected);
        };
        test_compute_gcd_aux(1, 4, 1);
        test_compute_gcd_aux(2, 4, 2);
        test_compute_gcd_aux(10, 25, 5);
        test_compute_gcd_aux(25, 25, 25);
    }

    #[test]
    fn find_gcd_test() {
        assert_eq!(find_gcd([0].into_iter()), None);
        assert_eq!(find_gcd([0, 10].into_iter()), NonZeroU64::new(10));
        assert_eq!(find_gcd([10, 0].into_iter()), NonZeroU64::new(10));
        assert_eq!(find_gcd([].into_iter()), None);
        assert_eq!(find_gcd([15, 30, 5, 10].into_iter()), NonZeroU64::new(5));
        assert_eq!(find_gcd([15, 16, 10].into_iter()), NonZeroU64::new(1));
        assert_eq!(find_gcd([0, 5, 5, 5].into_iter()), NonZeroU64::new(5));
        assert_eq!(find_gcd([0, 0].into_iter()), None);
    }
}
