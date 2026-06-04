#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
use std::arch::is_aarch64_feature_detected;
use std::ops::RangeInclusive;

#[cfg(target_arch = "x86_64")]
mod avx2;

#[cfg(target_arch = "aarch64")]
mod neon;

// SVE intrinsics are not exposed on aarch64-apple-darwin.
#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
mod sve;

mod scalar;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
enum FilterImplPerInstructionSet {
    #[cfg(target_arch = "x86_64")]
    AVX2 = 0u8,
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    SVE = 3u8,
    #[cfg(target_arch = "aarch64")]
    Neon = 2u8,
    Scalar = 1u8,
}

impl FilterImplPerInstructionSet {
    #[inline]
    pub fn is_available(&self) -> bool {
        match *self {
            #[cfg(target_arch = "x86_64")]
            FilterImplPerInstructionSet::AVX2 => is_x86_feature_detected!("avx2"),
            #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
            FilterImplPerInstructionSet::SVE => is_aarch64_feature_detected!("sve"),
            // TIL Neon is required on aarch 64.
            #[cfg(target_arch = "aarch64")]
            FilterImplPerInstructionSet::Neon => true,
            FilterImplPerInstructionSet::Scalar => true,
        }
    }
}

// List of available implementations in preferred order.
#[cfg(target_arch = "x86_64")]
const IMPLS: [FilterImplPerInstructionSet; 2] = [
    FilterImplPerInstructionSet::AVX2,
    FilterImplPerInstructionSet::Scalar,
];

// Non-Apple aarch64: try SVE, NEON, Scalar.
#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
const IMPLS: [FilterImplPerInstructionSet; 3] = [
    FilterImplPerInstructionSet::SVE,
    FilterImplPerInstructionSet::Neon,
    FilterImplPerInstructionSet::Scalar,
];

// Apple aarch64 (M-series): SVE not available; use NEON or Scalar.
#[cfg(all(target_arch = "aarch64", target_vendor = "apple"))]
const IMPLS: [FilterImplPerInstructionSet; 2] = [
    FilterImplPerInstructionSet::Neon,
    FilterImplPerInstructionSet::Scalar,
];

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const IMPLS: [FilterImplPerInstructionSet; 1] = [FilterImplPerInstructionSet::Scalar];

impl FilterImplPerInstructionSet {
    #[inline]
    #[allow(unused_variables)]
    fn from(code: u8) -> FilterImplPerInstructionSet {
        #[cfg(target_arch = "x86_64")]
        if code == FilterImplPerInstructionSet::AVX2 as u8 {
            return FilterImplPerInstructionSet::AVX2;
        }
        #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
        if code == FilterImplPerInstructionSet::SVE as u8 {
            return FilterImplPerInstructionSet::SVE;
        }
        #[cfg(target_arch = "aarch64")]
        if code == FilterImplPerInstructionSet::Neon as u8 {
            return FilterImplPerInstructionSet::Neon;
        }
        FilterImplPerInstructionSet::Scalar
    }

    #[inline]
    fn filter_vec_in_place(self, range: RangeInclusive<u32>, offset: u32, output: &mut Vec<u32>) {
        match self {
            #[cfg(target_arch = "x86_64")]
            FilterImplPerInstructionSet::AVX2 => avx2::filter_vec_in_place(range, offset, output),
            #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
            FilterImplPerInstructionSet::SVE => sve::filter_vec_in_place(range, offset, output),
            #[cfg(target_arch = "aarch64")]
            FilterImplPerInstructionSet::Neon => neon::filter_vec_in_place(range, offset, output),
            FilterImplPerInstructionSet::Scalar => {
                scalar::filter_vec_in_place(range, offset, output)
            }
        }
    }
}

fn available_impls() -> impl Iterator<Item = FilterImplPerInstructionSet> {
    IMPLS
        .into_iter()
        .filter(FilterImplPerInstructionSet::is_available)
}

#[inline]
fn get_best_available_instruction_set() -> FilterImplPerInstructionSet {
    use std::sync::atomic::{AtomicU8, Ordering};
    static INSTRUCTION_SET_BYTE: AtomicU8 = AtomicU8::new(u8::MAX);
    let instruction_set_byte: u8 = INSTRUCTION_SET_BYTE.load(Ordering::Relaxed);
    if instruction_set_byte == u8::MAX {
        // Let's initialize the instruction set and cache it.
        let instruction_set = available_impls().next().unwrap();
        INSTRUCTION_SET_BYTE.store(instruction_set as u8, Ordering::Relaxed);
        return instruction_set;
    }
    FilterImplPerInstructionSet::from(instruction_set_byte)
}

pub fn filter_vec_in_place(range: RangeInclusive<u32>, offset: u32, output: &mut Vec<u32>) {
    get_best_available_instruction_set().filter_vec_in_place(range, offset, output)
}

#[cfg(test)]
mod tests {
    use proptest::strategy::Strategy;

    use super::*;

    #[test]
    fn test_get_best_available_instruction_set() {
        let instruction_set = get_best_available_instruction_set();
        assert_eq!(get_best_available_instruction_set(), instruction_set);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_instruction_set_to_code_from_code() {
        for instruction_set in [
            FilterImplPerInstructionSet::AVX2,
            FilterImplPerInstructionSet::Scalar,
        ] {
            let code = instruction_set as u8;
            assert_eq!(instruction_set, FilterImplPerInstructionSet::from(code));
        }
    }

    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    #[test]
    fn test_instruction_set_to_code_from_code() {
        for instruction_set in [
            FilterImplPerInstructionSet::SVE,
            FilterImplPerInstructionSet::Neon,
            FilterImplPerInstructionSet::Scalar,
        ] {
            let code = instruction_set as u8;
            assert_eq!(instruction_set, FilterImplPerInstructionSet::from(code));
        }
    }

    #[cfg(all(target_arch = "aarch64", target_vendor = "apple"))]
    #[test]
    fn test_instruction_set_to_code_from_code() {
        for instruction_set in [
            FilterImplPerInstructionSet::Neon,
            FilterImplPerInstructionSet::Scalar,
        ] {
            let code = instruction_set as u8;
            assert_eq!(instruction_set, FilterImplPerInstructionSet::from(code));
        }
    }

    fn test_filter_impl_empty_aux(filter_impl: FilterImplPerInstructionSet) {
        let mut output = vec![];
        filter_impl.filter_vec_in_place(0..=u32::MAX, 0, &mut output);
        assert_eq!(&output, &[]);
    }

    fn test_filter_impl_simple_aux(filter_impl: FilterImplPerInstructionSet) {
        let mut output = vec![3, 2, 1, 5, 11, 2, 5, 10, 2];
        filter_impl.filter_vec_in_place(3..=10, 0, &mut output);
        assert_eq!(&output, &[0, 3, 6, 7]);
    }

    fn test_filter_impl_simple_aux_shifted(filter_impl: FilterImplPerInstructionSet) {
        let mut output = vec![3, 2, 1, 5, 11, 2, 5, 10, 2];
        filter_impl.filter_vec_in_place(3..=10, 10, &mut output);
        assert_eq!(&output, &[10, 13, 16, 17]);
    }

    fn test_filter_impl_simple_outside_i32_range(filter_impl: FilterImplPerInstructionSet) {
        let mut output = vec![u32::MAX, i32::MAX as u32 + 1, 0, 1, 3, 1, 1, 1, 1];
        filter_impl.filter_vec_in_place(1..=i32::MAX as u32 + 1u32, 0, &mut output);
        assert_eq!(&output, &[1, 3, 4, 5, 6, 7, 8]);
    }

    fn test_filter_impl_empty_range_aux(filter_impl: FilterImplPerInstructionSet) {
        // start > end: RangeInclusive::contains always returns false; output must be empty.
        // The SVE path's wrapping_sub would otherwise produce a huge range_width.
        let mut output = vec![3, 2, 1, 5, 11, 2, 5, 10, 2];
        filter_impl.filter_vec_in_place(10..=5, 0, &mut output);
        assert_eq!(&output, &[]);
    }

    fn test_filter_impl_test_suite(filter_impl: FilterImplPerInstructionSet) {
        test_filter_impl_empty_aux(filter_impl);
        test_filter_impl_simple_aux(filter_impl);
        test_filter_impl_simple_aux_shifted(filter_impl);
        test_filter_impl_simple_outside_i32_range(filter_impl);
        test_filter_impl_empty_range_aux(filter_impl);
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_filter_implementation_avx2() {
        if FilterImplPerInstructionSet::AVX2.is_available() {
            test_filter_impl_test_suite(FilterImplPerInstructionSet::AVX2);
        }
    }

    #[test]
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    fn test_filter_implementation_sve() {
        if FilterImplPerInstructionSet::SVE.is_available() {
            test_filter_impl_test_suite(FilterImplPerInstructionSet::SVE);
        }
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn test_filter_implementation_neon() {
        test_filter_impl_test_suite(FilterImplPerInstructionSet::Neon);
    }

    #[test]
    fn test_filter_implementation_scalar() {
        test_filter_impl_test_suite(FilterImplPerInstructionSet::Scalar);
    }

    fn max_val_strategy() -> impl proptest::strategy::Strategy<Value = u32> {
        proptest::prop_oneof![
            0u32..10u32,
            255u32..258u32,
            proptest::prelude::Just(1u32 << 25),
            proptest::prelude::Just(u32::MAX - 1),
            proptest::prelude::Just(u32::MAX),
        ]
    }

    fn vals_strategy() -> impl proptest::strategy::Strategy<Value = Vec<u32>> {
        proptest::prop_oneof![
            proptest::collection::vec(proptest::prelude::any::<u32>(), 0..300),
            max_val_strategy()
                .prop_flat_map(|max_val| { proptest::collection::vec(0..=max_val, 0..300) })
        ]
    }

    proptest::proptest! {
        #[test]
        fn test_filter_compare_scalar_and_impls_impl_proptest(
            start in 0u32..400u32,
            end in 0u32..400u32,
            offset in 0u32..2u32,
            mut vals in vals_strategy()) {
                for implementation in available_impls() {
                    if implementation == FilterImplPerInstructionSet::Scalar {
                        continue;
                    }
                    let mut vals_clone = vals.clone();
                    implementation.filter_vec_in_place(start..=end, offset, &mut vals);
                    FilterImplPerInstructionSet::Scalar.filter_vec_in_place(start..=end, offset, &mut vals_clone);
                    assert_eq!(&vals, &vals_clone);
                }
       }
    }
}
