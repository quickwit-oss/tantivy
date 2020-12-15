pub mod bitpacker;
mod bitset;
mod composite_file;
mod counting_writer;
mod serialize;
mod vint;

pub use self::bitset::BitSet;
pub(crate) use self::bitset::TinySet;
pub(crate) use self::composite_file::{CompositeFile, CompositeWrite};
pub use self::counting_writer::CountingWriter;
pub use self::serialize::{BinarySerializable, FixedSize};
pub use self::vint::{
    read_u32_vint, read_u32_vint_no_advance, serialize_vint_u32, write_u32_vint, VInt,
};
pub use byteorder::LittleEndian as Endianness;

/// Segment's max doc must be `< MAX_DOC_LIMIT`.
///
/// We do not allow segments with more than
pub const MAX_DOC_LIMIT: u32 = 1 << 31;

pub fn minmax<I, T>(mut vals: I) -> Option<(T, T)>
where
    I: Iterator<Item = T>,
    T: Copy + Ord,
{
    if let Some(first_el) = vals.next() {
        return Some(vals.fold((first_el, first_el), |(min_val, max_val), el| {
            (min_val.min(el), max_val.max(el))
        }));
    }
    None
}

/// Computes the number of bits that will be used for bitpacking.
///
/// In general the target is the minimum number of bits
/// required to express the amplitude given in argument.
///
/// e.g. If the amplitude is 10, we can store all ints on simply 4bits.
///
/// The logic is slightly more convoluted here as for optimization
/// reasons, we want to ensure that a value spawns over at most 8 bytes
/// of aligns bytes.
///
/// Spanning over 9 bytes is possible for instance, if we do
/// bitpacking with an amplitude of 63 bits.
/// In this case, the second int will start on bit
/// 63 (which belongs to byte 7) and ends at byte 15;
/// Hence 9 bytes (from byte 7 to byte 15 included).
///
/// To avoid this, we force the number of bits to 64bits
/// when the result is greater than `64-8 = 56 bits`.
///
/// Note that this only affects rare use cases spawning over
/// a very large range of values. Even in this case, it results
/// in an extra cost of at most 12% compared to the optimal
/// number of bits.
pub(crate) fn compute_num_bits(n: u64) -> u8 {
    let amplitude = (64u32 - n.leading_zeros()) as u8;
    if amplitude <= 64 - 8 {
        amplitude
    } else {
        64
    }
}

/// Has length trait
pub trait HasLen {
    /// Return length
    fn len(&self) -> usize;

    /// Returns true iff empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

const HIGHEST_BIT: u64 = 1 << 63;

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
/// The [reverse mapping is `u64_to_i64`](./fn.u64_to_i64.html).
#[inline(always)]
pub fn i64_to_u64(val: i64) -> u64 {
    (val as u64) ^ HIGHEST_BIT
}

/// Reverse the mapping given by [`i64_to_u64`](./fn.i64_to_u64.html).
#[inline(always)]
pub fn u64_to_i64(val: u64) -> i64 {
    (val ^ HIGHEST_BIT) as i64
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
/// The [reverse mapping is `u64_to_f64`](./fn.u64_to_f64.html).
#[inline(always)]
pub fn f64_to_u64(val: f64) -> u64 {
    let bits = val.to_bits();
    if val.is_sign_positive() {
        bits ^ HIGHEST_BIT
    } else {
        !bits
    }
}

/// Reverse the mapping given by [`i64_to_u64`](./fn.i64_to_u64.html).
#[inline(always)]
pub fn u64_to_f64(val: u64) -> f64 {
    f64::from_bits(if val & HIGHEST_BIT != 0 {
        val ^ HIGHEST_BIT
    } else {
        !val
    })
}

#[cfg(test)]
pub(crate) mod test {

    pub use super::minmax;
    pub use super::serialize::test::fixed_size_test;
    use super::{compute_num_bits, f64_to_u64, i64_to_u64, u64_to_f64, u64_to_i64};
    use proptest::prelude::*;
    use std::f64;

    fn test_i64_converter_helper(val: i64) {
        assert_eq!(u64_to_i64(i64_to_u64(val)), val);
    }

    fn test_f64_converter_helper(val: f64) {
        assert_eq!(u64_to_f64(f64_to_u64(val)), val);
    }

    proptest! {
        #[test]
        fn test_f64_converter_monotonicity_proptest((left, right) in (proptest::num::f64::NORMAL, proptest::num::f64::NORMAL)) {
            let left_u64 = f64_to_u64(left);
            let right_u64 = f64_to_u64(right);
            assert_eq!(left_u64 < right_u64,  left < right);
        }
    }

    #[test]
    fn test_i64_converter() {
        assert_eq!(i64_to_u64(i64::min_value()), u64::min_value());
        assert_eq!(i64_to_u64(i64::max_value()), u64::max_value());
        test_i64_converter_helper(0i64);
        test_i64_converter_helper(i64::min_value());
        test_i64_converter_helper(i64::max_value());
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
            .contains(&f64_to_u64(f64::NAN))); //nan is not a number
        assert!(f64_to_u64(1.5) > f64_to_u64(1.0)); //same exponent, different mantissa
        assert!(f64_to_u64(2.0) > f64_to_u64(1.0)); //same mantissa, different exponent
        assert!(f64_to_u64(2.0) > f64_to_u64(1.5)); //different exponent and mantissa
        assert!(f64_to_u64(1.0) > f64_to_u64(-1.0)); // pos > neg
        assert!(f64_to_u64(-1.5) < f64_to_u64(-1.0));
        assert!(f64_to_u64(-2.0) < f64_to_u64(1.0));
        assert!(f64_to_u64(-2.0) < f64_to_u64(-1.5));
    }

    #[test]
    fn test_compute_num_bits() {
        assert_eq!(compute_num_bits(1), 1u8);
        assert_eq!(compute_num_bits(0), 0u8);
        assert_eq!(compute_num_bits(2), 2u8);
        assert_eq!(compute_num_bits(3), 2u8);
        assert_eq!(compute_num_bits(4), 3u8);
        assert_eq!(compute_num_bits(255), 8u8);
        assert_eq!(compute_num_bits(256), 9u8);
        assert_eq!(compute_num_bits(5_000_000_000), 33u8);
    }

    #[test]
    fn test_max_doc() {
        // this is the first time I write a unit test for a constant.
        assert!(((super::MAX_DOC_LIMIT - 1) as i32) >= 0);
        assert!((super::MAX_DOC_LIMIT as i32) < 0);
    }

    #[test]
    fn test_minmax_empty() {
        let vals: Vec<u32> = vec![];
        assert_eq!(minmax(vals.into_iter()), None);
    }

    #[test]
    fn test_minmax_one() {
        assert_eq!(minmax(vec![1].into_iter()), Some((1, 1)));
    }

    #[test]
    fn test_minmax_two() {
        assert_eq!(minmax(vec![1, 2].into_iter()), Some((1, 2)));
        assert_eq!(minmax(vec![2, 1].into_iter()), Some((1, 2)));
    }
}
