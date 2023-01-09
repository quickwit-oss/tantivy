use std::fmt;
use std::marker::PhantomData;
use std::ops::RangeInclusive;

use fastdivide::DividerU64;

use crate::MonotonicallyMappableToU128;

/// Monotonic maps a value to u64 value space.
/// Monotonic mapping enables `PartialOrd` on u64 space without conversion to original space.
pub trait MonotonicallyMappableToU64:
    'static + PartialOrd + Copy + Send + Sync + fmt::Debug
{
    /// Converts a value to u64.
    ///
    /// Internally all fast field values are encoded as u64.
    fn to_u64(self) -> u64;

    /// Converts a value from u64
    ///
    /// Internally all fast field values are encoded as u64.
    /// **Note: To be used for converting encoded Term, Posting values.**
    fn from_u64(val: u64) -> Self;
}

/// Values need to be strictly monotonic mapped to a `Internal` value (u64 or u128) that can be
/// used in fast field codecs.
///
/// The monotonic mapping is required so that `PartialOrd` can be used on `Internal` without
/// converting to `External`.
///
/// All strictly monotonic functions are invertible because they are guaranteed to have a one-to-one
/// mapping from their range to their domain. The `inverse` method is required when opening a codec,
/// so a value can be converted back to its original domain (e.g. ip address or f64) from its
/// internal representation.
pub trait StrictlyMonotonicFn<External: Copy, Internal: Copy> {
    /// Strictly monotonically maps the value from External to Internal.
    fn mapping(&self, inp: External) -> Internal;
    /// Inverse of `mapping`. Maps the value from Internal to External.
    fn inverse(&self, out: Internal) -> External;

    /// Maps a user provded value from External to Internal.
    /// It may be necessary to coerce the value if it is outside the value space.
    /// In that case it tries to find the next greater value in the value space.
    ///
    /// Returns a bool to mark if a value was outside the value space and had to be coerced _up_.
    /// With that information we can detect if two values in a range both map outside the same value
    /// space.
    ///
    /// coerce_up means the next valid upper value in the value space will be chosen if the value
    /// has to be coerced.
    fn mapping_coerce(&self, inp: RangeInclusive<External>) -> RangeInclusive<Internal> {
        self.mapping(*inp.start())..=self.mapping(*inp.end())
    }
    /// Inverse of `mapping_coerce`.
    fn inverse_coerce(&self, out: RangeInclusive<Internal>) -> RangeInclusive<External> {
        self.inverse(*out.start())..=self.inverse(*out.end())
    }
}

/// Inverts a strictly monotonic mapping from `StrictlyMonotonicFn<A, B>` to
/// `StrictlyMonotonicFn<B, A>`.
///
/// # Warning
///
/// This type comes with a footgun. A type being strictly monotonic does not impose that the inverse
/// mapping is strictly monotonic over the entire space External. e.g. a -> a * 2. Use at your own
/// risks.
pub(crate) struct StrictlyMonotonicMappingInverter<T> {
    orig_mapping: T,
}
impl<T> From<T> for StrictlyMonotonicMappingInverter<T> {
    fn from(orig_mapping: T) -> Self {
        Self { orig_mapping }
    }
}

impl<From, To, T> StrictlyMonotonicFn<To, From> for StrictlyMonotonicMappingInverter<T>
where
    T: StrictlyMonotonicFn<From, To>,
    From: Copy,
    To: Copy,
{
    #[inline(always)]
    fn mapping(&self, val: To) -> From {
        self.orig_mapping.inverse(val)
    }

    #[inline(always)]
    fn inverse(&self, val: From) -> To {
        self.orig_mapping.mapping(val)
    }

    #[inline]
    fn mapping_coerce(&self, inp: RangeInclusive<To>) -> RangeInclusive<From> {
        self.orig_mapping.inverse_coerce(inp)
    }
    #[inline]
    fn inverse_coerce(&self, out: RangeInclusive<From>) -> RangeInclusive<To> {
        self.orig_mapping.mapping_coerce(out)
    }
}

/// Applies the strictly monotonic mapping from `T` without any additional changes.
pub(crate) struct StrictlyMonotonicMappingToInternal<T> {
    _phantom: PhantomData<T>,
}

impl<T> StrictlyMonotonicMappingToInternal<T> {
    pub(crate) fn new() -> StrictlyMonotonicMappingToInternal<T> {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<External: MonotonicallyMappableToU128, T: MonotonicallyMappableToU128>
    StrictlyMonotonicFn<External, u128> for StrictlyMonotonicMappingToInternal<T>
where T: MonotonicallyMappableToU128
{
    #[inline(always)]
    fn mapping(&self, inp: External) -> u128 {
        External::to_u128(inp)
    }

    #[inline(always)]
    fn inverse(&self, out: u128) -> External {
        External::from_u128(out)
    }
}

impl<External: MonotonicallyMappableToU64, T: MonotonicallyMappableToU64>
    StrictlyMonotonicFn<External, u64> for StrictlyMonotonicMappingToInternal<T>
where T: MonotonicallyMappableToU64
{
    #[inline(always)]
    fn mapping(&self, inp: External) -> u64 {
        External::to_u64(inp)
    }

    #[inline(always)]
    fn inverse(&self, out: u64) -> External {
        External::from_u64(out)
    }
}

/// Mapping dividing by  gcd and a base value.
///
/// The function is assumed to be only called on values divided by passed
/// gcd value. (It is necessary for the function to be monotonic.)
pub(crate) struct StrictlyMonotonicMappingToInternalGCDBaseval {
    gcd_divider: DividerU64,
    gcd: u64,
    min_value: u64,
}
impl StrictlyMonotonicMappingToInternalGCDBaseval {
    pub(crate) fn new(gcd: u64, min_value: u64) -> Self {
        let gcd_divider = DividerU64::divide_by(gcd);
        Self {
            gcd_divider,
            gcd,
            min_value,
        }
    }
}
impl<External: MonotonicallyMappableToU64> StrictlyMonotonicFn<External, u64>
    for StrictlyMonotonicMappingToInternalGCDBaseval
{
    #[inline(always)]
    fn mapping(&self, inp: External) -> u64 {
        self.gcd_divider
            .divide(External::to_u64(inp) - self.min_value)
    }

    #[inline(always)]
    fn inverse(&self, out: u64) -> External {
        External::from_u64(self.min_value + out * self.gcd)
    }

    #[inline]
    #[allow(clippy::reversed_empty_ranges)]
    fn mapping_coerce(&self, inp: RangeInclusive<External>) -> RangeInclusive<u64> {
        let end = External::to_u64(*inp.end());
        if end < self.min_value || inp.end() < inp.start() {
            return 1..=0;
        }
        let map_coerce = |mut inp, coerce_up| {
            let inp_lower_bound = self.inverse(0);
            if inp < inp_lower_bound {
                inp = inp_lower_bound;
            }
            let val = External::to_u64(inp);
            let need_coercion = coerce_up && (val - self.min_value) % self.gcd != 0;
            let mut mapped_val = self.mapping(inp);
            if need_coercion {
                mapped_val += 1;
            }
            mapped_val
        };
        let start = map_coerce(*inp.start(), true);
        let end = map_coerce(*inp.end(), false);
        start..=end
    }
}

/// Strictly monotonic mapping with a base value.
pub(crate) struct StrictlyMonotonicMappingToInternalBaseval {
    min_value: u64,
}
impl StrictlyMonotonicMappingToInternalBaseval {
    #[inline(always)]
    pub(crate) fn new(min_value: u64) -> Self {
        Self { min_value }
    }
}

impl<External: MonotonicallyMappableToU64> StrictlyMonotonicFn<External, u64>
    for StrictlyMonotonicMappingToInternalBaseval
{
    #[inline]
    #[allow(clippy::reversed_empty_ranges)]
    fn mapping_coerce(&self, inp: RangeInclusive<External>) -> RangeInclusive<u64> {
        if External::to_u64(*inp.end()) < self.min_value {
            return 1..=0;
        }
        let start = self.mapping(External::to_u64(*inp.start()).max(self.min_value));
        let end = self.mapping(External::to_u64(*inp.end()));
        start..=end
    }

    #[inline(always)]
    fn mapping(&self, val: External) -> u64 {
        External::to_u64(val) - self.min_value
    }

    #[inline(always)]
    fn inverse(&self, val: u64) -> External {
        External::from_u64(self.min_value + val)
    }
}

impl MonotonicallyMappableToU64 for u64 {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        self
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        val
    }
}

impl MonotonicallyMappableToU64 for i64 {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        common::i64_to_u64(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        common::u64_to_i64(val)
    }
}

impl MonotonicallyMappableToU64 for bool {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        u64::from(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        val > 0
    }
}

// TODO remove me.
// Tantivy should refuse NaN values and work with NotNaN internally.
impl MonotonicallyMappableToU64 for f64 {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        common::f64_to_u64(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        common::u64_to_f64(val)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn strictly_monotonic_test() {
        // identity mapping
        test_round_trip(&StrictlyMonotonicMappingToInternal::<u64>::new(), 100u64);
        // round trip to i64
        test_round_trip(&StrictlyMonotonicMappingToInternal::<i64>::new(), 100u64);
        // identity mapping
        test_round_trip(&StrictlyMonotonicMappingToInternal::<u128>::new(), 100u128);

        // base value to i64 round trip
        let mapping = StrictlyMonotonicMappingToInternalBaseval::new(100);
        test_round_trip::<_, _, u64>(&mapping, 100i64);
        // base value and gcd to u64 round trip
        let mapping = StrictlyMonotonicMappingToInternalGCDBaseval::new(10, 100);
        test_round_trip::<_, _, u64>(&mapping, 100u64);
    }

    fn test_round_trip<T: StrictlyMonotonicFn<K, L>, K: std::fmt::Debug + Eq + Copy, L: Copy>(
        mapping: &T,
        test_val: K,
    ) {
        assert_eq!(mapping.inverse(mapping.mapping(test_val)), test_val);
    }
}
