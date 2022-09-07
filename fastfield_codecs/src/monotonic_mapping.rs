pub trait MonotonicallyMappableToU64: 'static + PartialOrd + Copy {
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
        if self {
            1
        } else {
            0
        }
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
