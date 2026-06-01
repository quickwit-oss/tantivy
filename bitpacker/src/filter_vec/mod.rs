#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
use std::arch::is_aarch64_feature_detected;
use std::ops::RangeInclusive;

#[cfg(target_arch = "x86_64")]
mod avx2;

#[cfg(target_arch = "aarch64")]
mod neon;

// SVE intrinsics are nightly-only and not exposed on aarch64-apple-darwin.
#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
mod sve;

mod scalar;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
enum FilterImplPerInstructionSet {
    #[cfg(target_arch = "x86_64")]
    AVX2 = 0u8,
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
    Sve = 3u8,
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
            #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
            FilterImplPerInstructionSet::Sve => is_aarch64_feature_detected!("sve"),
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

// Non-Apple aarch64 with nightly: try SVE, NEON, Scalar.
#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
const IMPLS: [FilterImplPerInstructionSet; 3] = [
    FilterImplPerInstructionSet::Sve,
    FilterImplPerInstructionSet::Neon,
    FilterImplPerInstructionSet::Scalar,
];

// Non-Apple aarch64 without nightly: SVE unavailable; use NEON or Scalar.
#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), not(nightly)))]
const IMPLS: [FilterImplPerInstructionSet; 2] = [
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
        #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
        if code == FilterImplPerInstructionSet::Sve as u8 {
            return FilterImplPerInstructionSet::Sve;
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
            #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
            FilterImplPerInstructionSet::Sve => sve::filter_vec_in_place(range, offset, output),
            #[cfg(target_arch = "aarch64")]
            FilterImplPerInstructionSet::Neon => neon::filter_vec_in_place(range, offset, output),
            FilterImplPerInstructionSet::Scalar => {
                scalar::filter_vec_in_place(range, offset, output)
            }
        }
    }
}

#[inline]
fn get_best_available_instruction_set() -> FilterImplPerInstructionSet {
    use std::sync::atomic::{AtomicU8, Ordering};
    static INSTRUCTION_SET_BYTE: AtomicU8 = AtomicU8::new(u8::MAX);
    let instruction_set_byte: u8 = INSTRUCTION_SET_BYTE.load(Ordering::Relaxed);
    if instruction_set_byte == u8::MAX {
        let instruction_set = IMPLS
            .into_iter()
            .find(FilterImplPerInstructionSet::is_available)
            .unwrap();
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

    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
    #[test]
    fn test_instruction_set_to_code_from_code() {
        for instruction_set in [
            FilterImplPerInstructionSet::Sve,
            FilterImplPerInstructionSet::Neon,
            FilterImplPerInstructionSet::Scalar,
        ] {
            let code = instruction_set as u8;
            assert_eq!(instruction_set, FilterImplPerInstructionSet::from(code));
        }
    }

    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), not(nightly)))]
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
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple"), nightly))]
    fn test_filter_implementation_sve() {
        if FilterImplPerInstructionSet::Sve.is_available() {
            test_filter_impl_test_suite(FilterImplPerInstructionSet::Sve);
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

    #[cfg(target_arch = "x86_64")]
    proptest::proptest! {
        #[test]
        fn test_filter_compare_scalar_and_avx2_impl_proptest(
            start in proptest::prelude::any::<u32>(),
            end in proptest::prelude::any::<u32>(),
            offset in 0u32..2u32,
            mut vals in proptest::collection::vec(0..u32::MAX, 0..30)) {
            if FilterImplPerInstructionSet::AVX2.is_available() {
                let mut vals_clone = vals.clone();
                FilterImplPerInstructionSet::AVX2.filter_vec_in_place(start..=end, offset, &mut vals);
                FilterImplPerInstructionSet::Scalar.filter_vec_in_place(start..=end, offset, &mut vals_clone);
                assert_eq!(&vals, &vals_clone);
            }
       }
    }

    #[cfg(target_arch = "aarch64")]
    proptest::proptest! {
        #[test]
        fn test_filter_compare_scalar_and_neon_impl_proptest(
            start in proptest::prelude::any::<u32>(),
            end in proptest::prelude::any::<u32>(),
            offset in 0u32..2u32,
            mut vals in proptest::collection::vec(0..u32::MAX, 0..30)) {
            let mut vals_clone = vals.clone();
            FilterImplPerInstructionSet::Neon.filter_vec_in_place(start..=end, offset, &mut vals);
            FilterImplPerInstructionSet::Scalar.filter_vec_in_place(start..=end, offset, &mut vals_clone);
            assert_eq!(&vals, &vals_clone);
       }
    }
}
