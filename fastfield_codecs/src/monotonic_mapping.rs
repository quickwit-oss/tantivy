use std::marker::PhantomData;

use fastdivide::DividerU64;
use ordered_float::NotNan;

use crate::MonotonicallyMappableToU128;

/// Monotonic maps a value to u64 value space.
/// Monotonic mapping enables `PartialOrd` on u64 space without conversion to original space.
pub trait MonotonicallyMappableToU64: 'static + PartialOrd + Copy + Send + Sync {
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
pub trait StrictlyMonotonicFn<External, Internal> {
    /// Strictly monotonically maps the value from External to Internal.
    fn mapping(&self, inp: External) -> Internal;
    /// Inverse of `mapping`. Maps the value from Internal to External.
    fn inverse(&self, out: Internal) -> External;
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
where T: StrictlyMonotonicFn<From, To>
{
    fn mapping(&self, val: To) -> From {
        self.orig_mapping.inverse(val)
    }

    fn inverse(&self, val: From) -> To {
        self.orig_mapping.mapping(val)
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
    fn mapping(&self, inp: External) -> u128 {
        External::to_u128(inp)
    }

    fn inverse(&self, out: u128) -> External {
        External::from_u128(out)
    }
}

impl<External: MonotonicallyMappableToU64, T: MonotonicallyMappableToU64>
    StrictlyMonotonicFn<External, u64> for StrictlyMonotonicMappingToInternal<T>
where T: MonotonicallyMappableToU64
{
    fn mapping(&self, inp: External) -> u64 {
        External::to_u64(inp)
    }

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
    fn mapping(&self, inp: External) -> u64 {
        self.gcd_divider
            .divide(External::to_u64(inp) - self.min_value)
    }

    fn inverse(&self, out: u64) -> External {
        External::from_u64(self.min_value + out * self.gcd)
    }
}

/// Strictly monotonic mapping with a base value.
pub(crate) struct StrictlyMonotonicMappingToInternalBaseval {
    min_value: u64,
}
impl StrictlyMonotonicMappingToInternalBaseval {
    pub(crate) fn new(min_value: u64) -> Self {
        Self { min_value }
    }
}

impl<External: MonotonicallyMappableToU64> StrictlyMonotonicFn<External, u64>
    for StrictlyMonotonicMappingToInternalBaseval
{
    fn mapping(&self, val: External) -> u64 {
        External::to_u64(val) - self.min_value
    }

    fn inverse(&self, val: u64) -> External {
        External::from_u64(self.min_value + val)
    }
}

impl MonotonicallyMappableToU64 for u64 {
    fn to_u64(self) -> u64 {
        self
    }

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
    fn to_u64(self) -> u64 {
        common::f64_to_u64(self)
    }

    fn from_u64(val: u64) -> Self {
        common::u64_to_f64(val)
    }
}

impl MonotonicallyMappableToU64 for ordered_float::NotNan<f64> {
    fn to_u64(self) -> u64 {
        common::f64_to_u64(self.into_inner())
    }

    fn from_u64(val: u64) -> Self {
        NotNan::new(common::u64_to_f64(val)).expect("Invalid NotNaN f64 value.")
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_from_u64_pos_inf() {
        let inf_as_u64 = common::f64_to_u64(f64::INFINITY);
        let inf_back_to_f64 = NotNan::from_u64(inf_as_u64);
        assert_eq!(inf_back_to_f64, NotNan::new(f64::INFINITY).unwrap());
    }

    #[test]
    fn test_from_u64_neg_inf() {
        let inf_as_u64 = common::f64_to_u64(-f64::INFINITY);
        let inf_back_to_f64 = NotNan::from_u64(inf_as_u64);
        assert_eq!(inf_back_to_f64, NotNan::new(-f64::INFINITY).unwrap());
    }

    #[test]
    #[should_panic(expected = "Invalid NotNaN")]
    fn test_from_u64_nan_panics() {
        let nan_as_u64 = common::f64_to_u64(f64::NAN);
        NotNan::from_u64(nan_as_u64);
    }

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

    fn test_round_trip<T: StrictlyMonotonicFn<K, L>, K: std::fmt::Debug + Eq + Copy, L>(
        mapping: &T,
        test_val: K,
    ) {
        assert_eq!(mapping.inverse(mapping.mapping(test_val)), test_val);
    }
}
