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
pub use self::vint::{read_u32_vint, serialize_vint_u32, write_u32_vint, VInt};
pub use byteorder::LittleEndian as Endianness;

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

pub(crate) fn is_power_of_2(n: usize) -> bool {
    (n > 0) && (n & (n - 1) == 0)
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

#[cfg(test)]
pub(crate) mod test {

    pub use super::serialize::test::fixed_size_test;
    use super::{compute_num_bits, i64_to_u64, u64_to_i64};

    fn test_i64_converter_helper(val: i64) {
        assert_eq!(u64_to_i64(i64_to_u64(val)), val);
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
}
