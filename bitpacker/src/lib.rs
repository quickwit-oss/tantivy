mod bitpacker;
mod blocked_bitpacker;

pub use crate::bitpacker::BitPacker;
pub use crate::bitpacker::BitUnpacker;
pub use crate::blocked_bitpacker::BlockedBitpacker;

/// Computes the number of bits that will be used for bitpacking.
///
/// In general the target is the minimum number of bits
/// required to express the amplitude given in argument.
///
/// e.g. If the amplitude is 10, we can store all ints on simply 4bits.
///
/// The logic is slightly more convoluted here as for optimization
/// reasons, we want to ensure that a value spawns over at most 8 bytes
/// of aligned bytes.
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
pub fn compute_num_bits(n: u64) -> u8 {
    let amplitude = (64u32 - n.leading_zeros()) as u8;
    if amplitude <= 64 - 8 {
        amplitude
    } else {
        64
    }
}

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
