use fastdivide::DividerU64;

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

// Mapping pairs for the case we subtract the min_value and apply a gcd (greatest common divisor)
pub mod gcd_min_val_mapping_pairs {

    use super::*;
    pub fn from_gcd_normalized_u64<Item: MonotonicallyMappableToU64>(
        val: u64,
        min_value: u64,
        gcd: u64,
    ) -> Item {
        Item::from_u64(min_value + val * gcd)
    }

    pub fn normalize_with_gcd<Item: MonotonicallyMappableToU64>(
        val: Item,
        min_value: u64,
        gcd_divider: &DividerU64,
    ) -> u64 {
        gcd_divider.divide(Item::to_u64(val) - min_value)
    }

    #[test]
    fn monotonic_mapping_roundtrip_test() {
        let gcd = std::num::NonZeroU64::new(10).unwrap();
        let divider = DividerU64::divide_by(gcd.get());

        let orig_value: u64 = 500;
        let normalized_val: u64 = normalize_with_gcd(orig_value, 100, &divider);
        assert_eq!(normalized_val, 40);
        assert_eq!(
            from_gcd_normalized_u64::<u64>(normalized_val, 100, gcd.get()),
            500
        );
    }
}

// Mapping pairs for the case we subtract the min_value
pub mod min_val_mapping_pairs {
    use super::*;

    pub fn from_normalized_u64<Item: MonotonicallyMappableToU64>(val: u64, min_value: u64) -> Item {
        Item::from_u64(min_value + val)
    }

    pub fn normalize<Item: MonotonicallyMappableToU64>(val: Item, min_value: u64) -> u64 {
        Item::to_u64(val) - min_value
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

impl MonotonicallyMappableToU64 for f64 {
    fn to_u64(self) -> u64 {
        common::f64_to_u64(self)
    }

    fn from_u64(val: u64) -> Self {
        common::u64_to_f64(val)
    }
}
