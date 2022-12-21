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
    use std::io;
    use std::num::NonZeroU64;

    use common::OwnedBytes;

    use crate::gcd::{compute_gcd, find_gcd};
    use crate::{FastFieldCodecType, VecColumn};

    fn test_fastfield_gcd_i64_with_codec(
        codec_type: FastFieldCodecType,
        num_vals: usize,
    ) -> io::Result<()> {
        let mut vals: Vec<i64> = (-4..=(num_vals as i64) - 5).map(|val| val * 1000).collect();
        let mut buffer: Vec<u8> = Vec::new();
        crate::serialize(VecColumn::from(&vals), &mut buffer, &[codec_type])?;
        let buffer = OwnedBytes::new(buffer);
        let column = crate::open::<i64>(buffer.clone())?;
        assert_eq!(column.get_val(0), -4000i64);
        assert_eq!(column.get_val(1), -3000i64);
        assert_eq!(column.get_val(2), -2000i64);
        assert_eq!(column.max_value(), (num_vals as i64 - 5) * 1000);
        assert_eq!(column.min_value(), -4000i64);

        // Can't apply gcd
        let mut buffer_without_gcd = Vec::new();
        vals.pop();
        vals.push(1001i64);
        crate::serialize(
            VecColumn::from(&vals),
            &mut buffer_without_gcd,
            &[codec_type],
        )?;
        let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
        assert!(buffer_without_gcd.len() > buffer.len());

        Ok(())
    }

    #[test]
    fn test_fastfield_gcd_i64() -> io::Result<()> {
        for &codec_type in &[
            FastFieldCodecType::Bitpacked,
            FastFieldCodecType::BlockwiseLinear,
            FastFieldCodecType::Linear,
        ] {
            test_fastfield_gcd_i64_with_codec(codec_type, 5500)?;
        }
        Ok(())
    }

    fn test_fastfield_gcd_u64_with_codec(
        codec_type: FastFieldCodecType,
        num_vals: usize,
    ) -> io::Result<()> {
        let mut vals: Vec<u64> = (1..=num_vals).map(|i| i as u64 * 1000u64).collect();
        let mut buffer: Vec<u8> = Vec::new();
        crate::serialize(VecColumn::from(&vals), &mut buffer, &[codec_type])?;
        let buffer = OwnedBytes::new(buffer);
        let column = crate::open::<u64>(buffer.clone())?;
        assert_eq!(column.get_val(0), 1000u64);
        assert_eq!(column.get_val(1), 2000u64);
        assert_eq!(column.get_val(2), 3000u64);
        assert_eq!(column.max_value(), num_vals as u64 * 1000);
        assert_eq!(column.min_value(), 1000u64);

        // Can't apply gcd
        let mut buffer_without_gcd = Vec::new();
        vals.pop();
        vals.push(1001u64);
        crate::serialize(
            VecColumn::from(&vals),
            &mut buffer_without_gcd,
            &[codec_type],
        )?;
        let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
        assert!(buffer_without_gcd.len() > buffer.len());
        Ok(())
    }

    #[test]
    fn test_fastfield_gcd_u64() -> io::Result<()> {
        for &codec_type in &[
            FastFieldCodecType::Bitpacked,
            FastFieldCodecType::BlockwiseLinear,
            FastFieldCodecType::Linear,
        ] {
            test_fastfield_gcd_u64_with_codec(codec_type, 5500)?;
        }
        Ok(())
    }

    #[test]
    pub fn test_fastfield2() {
        let test_fastfield = crate::serialize_and_load(&[100u64, 200u64, 300u64]);
        assert_eq!(test_fastfield.get_val(0), 100);
        assert_eq!(test_fastfield.get_val(1), 200);
        assert_eq!(test_fastfield.get_val(2), 300);
    }

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
