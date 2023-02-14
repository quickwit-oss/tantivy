#![allow(clippy::len_without_is_empty)]

use std::ops::Deref;

pub use byteorder::LittleEndian as Endianness;

mod bitset;
mod datetime;
pub mod file_slice;
mod group_by;
mod serialize;
mod vint;
mod writer;
pub use bitset::*;
pub use datetime::{DatePrecision, DateTime};
pub use group_by::GroupByIteratorExtended;
pub use ownedbytes::{OwnedBytes, StableDeref};
pub use serialize::{BinarySerializable, DeserializeFrom, FixedSize};
pub use vint::{
    deserialize_vint_u128, read_u32_vint, read_u32_vint_no_advance, serialize_vint_u128,
    serialize_vint_u32, write_u32_vint, VInt, VIntU128,
};
pub use writer::{AntiCallToken, CountingWriter, TerminatingWrite};

/// Has length trait
pub trait HasLen {
    /// Return length
    fn len(&self) -> usize;

    /// Returns true iff empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Deref<Target = [u8]>> HasLen for T {
    fn len(&self) -> usize {
        self.deref().len()
    }
}

const HIGHEST_BIT_64: u64 = 1 << 63;
const HIGHEST_BIT_32: u32 = 1 << 31;

/// Maps a `i64` to `u64`
///
/// For simplicity, tantivy internally handles `i64` as `u64`.
/// The mapping is defined by this function.
///
/// Maps `i64` to `u64` so that
/// `-2^63 .. 2^63-1` is mapped
///     to
/// `0 .. 2^64-1`
/// in that order.
///
/// This is more suited than simply casting (`val as u64`)
/// because of bitpacking.
///
/// Imagine a list of `i64` ranging from -10 to 10.
/// When casting negative values, the negative values are projected
/// to values over 2^63, and all values end up requiring 64 bits.
///
/// # See also
/// The reverse mapping is [`u64_to_i64()`].
#[inline]
pub fn i64_to_u64(val: i64) -> u64 {
    (val as u64) ^ HIGHEST_BIT_64
}

/// Reverse the mapping given by [`i64_to_u64()`].
#[inline]
pub fn u64_to_i64(val: u64) -> i64 {
    (val ^ HIGHEST_BIT_64) as i64
}

/// Maps a `f64` to `u64`
///
/// For simplicity, tantivy internally handles `f64` as `u64`.
/// The mapping is defined by this function.
///
/// Maps `f64` to `u64` in a monotonic manner, so that bytes lexical order is preserved.
///
/// This is more suited than simply casting (`val as u64`)
/// which would truncate the result
///
/// # Reference
///
/// Daniel Lemire's [blog post](https://lemire.me/blog/2020/12/14/converting-floating-point-numbers-to-integers-while-preserving-order/)
/// explains the mapping in a clear manner.
///
/// # See also
/// The reverse mapping is [`u64_to_f64()`].
#[inline]
pub fn f64_to_u64(val: f64) -> u64 {
    let bits = val.to_bits();
    if val.is_sign_positive() {
        bits ^ HIGHEST_BIT_64
    } else {
        !bits
    }
}

/// Reverse the mapping given by [`f64_to_u64()`].
#[inline]
pub fn u64_to_f64(val: u64) -> f64 {
    f64::from_bits(if val & HIGHEST_BIT_64 != 0 {
        val ^ HIGHEST_BIT_64
    } else {
        !val
    })
}

/// Maps a `f32` to `u64`
///
/// # See also
/// Similar mapping for f64 [`u64_to_f64()`].
#[inline]
pub fn f32_to_u64(val: f32) -> u64 {
    let bits = val.to_bits();
    let res32 = if val.is_sign_positive() {
        bits ^ HIGHEST_BIT_32
    } else {
        !bits
    };
    res32 as u64
}

/// Reverse the mapping given by [`f32_to_u64()`].
#[inline]
pub fn u64_to_f32(val: u64) -> f32 {
    debug_assert!(val <= 1 << 32);
    let val = val as u32;
    f32::from_bits(if val & HIGHEST_BIT_32 != 0 {
        val ^ HIGHEST_BIT_32
    } else {
        !val
    })
}

/// Maps a `f64` to a fixed point representation.
/// Lower bound is inclusive, upper bound is exclusive.
/// `precision` is the number of bits used to represent the number.
///
/// This is a lossy, affine transformation. All provided values must be finite and non-NaN.
/// Care should be taken to not provide values which would cause loss of precision such as values
/// low enough to get sub-normal numbers, value high enough rounding would cause ±Inf to appear, or
/// a precision larger than 50b.
///
/// # See also
/// The reverse mapping is [`fixed_point_to_f64()`].
#[inline]
pub fn f64_to_fixed_point(val: f64, min: f64, max: f64, precision: u8) -> u64 {
    debug_assert!((1..53).contains(&precision));
    debug_assert!(min < max);

    let delta = max - min;
    let mult = (1u64 << precision) as f64;
    let bucket_size = delta / mult;
    let upper_bound = f64_next_down(max).min(max - bucket_size);

    // due to different cases of rounding error, we need to enforce upper_bound to be
    // max-bucket_size, but also that upper_bound < max, which is not given for small enough
    // bucket_size.
    let val = val.clamp(min, upper_bound);

    let res = (val - min) / bucket_size;
    if res.fract() == 0.5 {
        res as u64
    } else {
        // round down when getting x.5
        res.round() as u64
    }
}

/// Reverse the mapping given by [`f64_to_fixed_point()`].
#[inline]
pub fn fixed_point_to_f64(val: u64, min: f64, max: f64, precision: u8) -> f64 {
    let delta = max - min;
    let mult = (1u64 << precision) as f64;
    let bucket_size = delta / mult;

    bucket_size.mul_add(val as f64, min)
}

// taken from rfc/3173-float-next-up-down, commented out part about nan in infinity as it is not
// needed.
fn f64_next_down(this: f64) -> f64 {
    const NEG_TINY_BITS: u64 = 0x8000_0000_0000_0001;
    const CLEAR_SIGN_MASK: u64 = 0x7fff_ffff_ffff_ffff;

    let bits = this.to_bits();
    // if this.is_nan() || bits == f64::NEG_INFINITY.to_bits() {
    // return this;
    // }
    let abs = bits & CLEAR_SIGN_MASK;
    let next_bits = if abs == 0 {
        NEG_TINY_BITS
    } else if bits == abs {
        bits - 1
    } else {
        bits + 1
    };
    f64::from_bits(next_bits)
}

#[cfg(test)]
pub mod test {
    use std::cmp::Ordering;

    use proptest::prelude::*;

    use super::{
        f32_to_u64, f64_to_fixed_point, f64_to_u64, fixed_point_to_f64, i64_to_u64, u64_to_f32,
        u64_to_f64, u64_to_i64, BinarySerializable, FixedSize,
    };

    fn test_i64_converter_helper(val: i64) {
        assert_eq!(u64_to_i64(i64_to_u64(val)), val);
    }

    fn test_f64_converter_helper(val: f64) {
        assert_eq!(u64_to_f64(f64_to_u64(val)).total_cmp(&val), Ordering::Equal);
    }

    fn test_f32_converter_helper(val: f32) {
        assert_eq!(u64_to_f32(f32_to_u64(val)).total_cmp(&val), Ordering::Equal);
    }

    fn test_fixed_point_converter_helper(val: f64, min: f64, max: f64, precision: u8) {
        let bucket_count = 1 << precision;

        let packed = f64_to_fixed_point(val, min, max, precision);

        assert!(packed < bucket_count, "used to much bits");

        let depacked = fixed_point_to_f64(packed, min, max, precision);
        let repacked = f64_to_fixed_point(depacked, min, max, precision);

        assert_eq!(packed, repacked, "generational loss");

        let error = (val.clamp(min, crate::f64_next_down(max)) - depacked).abs();

        let expected = (max - min) / (bucket_count as f64);
        assert!(
            error <= (max - min) / (bucket_count as f64) * 2.0,
            "error larger than expected"
        );
    }

    pub fn fixed_size_test<O: BinarySerializable + FixedSize + Default>() {
        let mut buffer = Vec::new();
        O::default().serialize(&mut buffer).unwrap();
        assert_eq!(buffer.len(), O::SIZE_IN_BYTES);
    }

    fn fixed_point_bound() -> proptest::num::f64::Any {
        proptest::num::f64::POSITIVE
            | proptest::num::f64::NEGATIVE
            | proptest::num::f64::NORMAL
            | proptest::num::f64::ZERO
    }

    proptest! {
        #[test]
        fn test_f64_converter_monotonicity_proptest((left, right) in (proptest::num::f64::ANY, proptest::num::f64::ANY)) {
            test_f64_converter_helper(left);
            test_f64_converter_helper(right);

            let left_u64 = f64_to_u64(left);
            let right_u64 = f64_to_u64(right);

            assert_eq!(left_u64.cmp(&right_u64),  left.total_cmp(&right));
        }

        #[test]
        fn test_f32_converter_monotonicity_proptest((left, right) in (proptest::num::f32::ANY, proptest::num::f32::ANY)) {
            test_f32_converter_helper(left);
            test_f32_converter_helper(right);

            let left_u64 = f32_to_u64(left);
            let right_u64 = f32_to_u64(right);
            assert_eq!(left_u64.cmp(&right_u64),  left.total_cmp(&right));
        }

        #[test]
        fn test_fixed_point_converter_proptest((left, right, min, max, precision) in
                (fixed_point_bound(), fixed_point_bound(),
                fixed_point_bound(), fixed_point_bound(),
                proptest::num::u8::ANY)) {
            // convert so all input are legal
            let (min, max) = if min < max {
                (min, max)
            } else if min > max {
                (max, min)
            } else {
                return Ok(()); // equals
            };
            if 1 > precision || precision >= 50 {
                return Ok(());
            }

            let max_full_precision = 53.0 - precision as f64;
            if (max / min).abs().log2().abs() > max_full_precision {
                return Ok(());
            }
            // we will go in subnormal territories => loss of precision
            if (((max - min).log2() - precision as f64) as i32) < f64::MIN_EXP {
                return Ok(());
            }

            if (max - min).is_infinite() {
                return Ok(());
            }

            test_fixed_point_converter_helper(left, min, max, precision);
            test_fixed_point_converter_helper(right, min, max, precision);

            let left_u64 = f64_to_fixed_point(left, min, max, precision);
            let right_u64 = f64_to_fixed_point(right, min, max, precision);
            if left < right {
                assert!(left_u64 <= right_u64);
            } else if left > right {
                assert!(left_u64 >= right_u64)
            }
        }
    }

    #[test]
    fn test_i64_converter() {
        assert_eq!(i64_to_u64(i64::MIN), u64::MIN);
        assert_eq!(i64_to_u64(i64::MAX), u64::MAX);
        test_i64_converter_helper(0i64);
        test_i64_converter_helper(i64::MIN);
        test_i64_converter_helper(i64::MAX);
        for i in -1000i64..1000i64 {
            test_i64_converter_helper(i);
        }
    }

    #[test]
    fn test_f64_converter() {
        test_f64_converter_helper(f64::INFINITY);
        test_f64_converter_helper(f64::NEG_INFINITY);
        test_f64_converter_helper(0.0);
        test_f64_converter_helper(-0.0);
        test_f64_converter_helper(1.0);
        test_f64_converter_helper(-1.0);
    }

    #[test]
    fn test_f64_order() {
        assert!(!(f64_to_u64(f64::NEG_INFINITY)..f64_to_u64(f64::INFINITY))
            .contains(&f64_to_u64(f64::NAN))); // nan is not a number
        assert!(f64_to_u64(1.5) > f64_to_u64(1.0)); // same exponent, different mantissa
        assert!(f64_to_u64(2.0) > f64_to_u64(1.0)); // same mantissa, different exponent
        assert!(f64_to_u64(2.0) > f64_to_u64(1.5)); // different exponent and mantissa
        assert!(f64_to_u64(1.0) > f64_to_u64(-1.0)); // pos > neg
        assert!(f64_to_u64(-1.5) < f64_to_u64(-1.0));
        assert!(f64_to_u64(-2.0) < f64_to_u64(1.0));
        assert!(f64_to_u64(-2.0) < f64_to_u64(-1.5));
    }

    #[test]
    fn test_f32_converter() {
        test_f32_converter_helper(f32::INFINITY);
        test_f32_converter_helper(f32::NEG_INFINITY);
        test_f32_converter_helper(0.0);
        test_f32_converter_helper(-0.0);
        test_f32_converter_helper(1.0);
        test_f32_converter_helper(-1.0);
    }

    #[test]
    fn test_f32_order() {
        assert!(!(f32_to_u64(f32::NEG_INFINITY)..f32_to_u64(f32::INFINITY))
            .contains(&f32_to_u64(f32::NAN))); // nan is not a number
        assert!(f32_to_u64(1.5) > f32_to_u64(1.0)); // same exponent, different mantissa
        assert!(f32_to_u64(2.0) > f32_to_u64(1.0)); // same mantissa, different exponent
        assert!(f32_to_u64(2.0) > f32_to_u64(1.5)); // different exponent and mantissa
        assert!(f32_to_u64(1.0) > f32_to_u64(-1.0)); // pos > neg
        assert!(f32_to_u64(-1.5) < f32_to_u64(-1.0));
        assert!(f32_to_u64(-2.0) < f32_to_u64(1.0));
        assert!(f32_to_u64(-2.0) < f32_to_u64(-1.5));
    }
}
