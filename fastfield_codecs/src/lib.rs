#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(test)]
#[macro_use]
extern crate more_asserts;

#[cfg(all(test, feature = "unstable"))]
extern crate test;

use std::io;
use std::io::Write;

use common::BinarySerializable;
use ownedbytes::OwnedBytes;

pub mod bitpacked;
pub mod blockwise_linear;
pub(crate) mod line;
pub mod linear;

mod column;
mod gcd;
mod serialize;

pub use self::column::{monotonic_map_column, Column, VecColumn};
pub use self::serialize::{open, serialize, serialize_and_load};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
pub enum FastFieldCodecType {
    Bitpacked = 1,
    Linear = 2,
    BlockwiseLinear = 3,
    Gcd = 4,
}

impl BinarySerializable for FastFieldCodecType {
    fn serialize<W: Write>(&self, wrt: &mut W) -> io::Result<()> {
        self.to_code().serialize(wrt)
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let code = u8::deserialize(reader)?;
        let codec_type: Self = Self::from_code(code)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Unknown code `{code}.`"))?;
        Ok(codec_type)
    }
}

impl FastFieldCodecType {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Bitpacked),
            2 => Some(Self::Linear),
            3 => Some(Self::BlockwiseLinear),
            4 => Some(Self::Gcd),
            _ => None,
        }
    }
}

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

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldCodec: 'static {
    /// A codex needs to provide a unique name and id, which is
    /// used for debugging and de/serialization.
    const CODEC_TYPE: FastFieldCodecType;

    type Reader: Column<u64> + 'static;

    /// Reads the metadata and returns the CodecReader
    fn open_from_bytes(bytes: OwnedBytes) -> io::Result<Self::Reader>;

    /// Serializes the data using the serializer into write.
    ///
    /// The fastfield_accessor iterator should be preferred over using fastfield_accessor for
    /// performance reasons.
    fn serialize(write: &mut impl Write, fastfield_accessor: &dyn Column<u64>) -> io::Result<()>;

    /// Returns an estimate of the compression ratio.
    /// If the codec is not applicable, returns `None`.
    ///
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(fastfield_accessor: &impl Column) -> Option<f32>;
}

pub const ALL_CODEC_TYPES: [FastFieldCodecType; 4] = [
    FastFieldCodecType::Bitpacked,
    FastFieldCodecType::BlockwiseLinear,
    FastFieldCodecType::Gcd,
    FastFieldCodecType::Linear,
];

#[derive(Debug, Clone)]
/// Statistics are used in codec detection and stored in the fast field footer.
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use crate::bitpacked::BitpackedCodec;
    use crate::blockwise_linear::BlockwiseLinearCodec;
    use crate::linear::LinearCodec;

    pub fn create_and_validate<Codec: FastFieldCodec>(
        data: &[u64],
        name: &str,
    ) -> Option<(f32, f32)> {
        let estimation = Codec::estimate(&VecColumn::from(data))?;

        let mut out: Vec<u8> = Vec::new();
        Codec::serialize(&mut out, &VecColumn::from(data)).unwrap();

        let actual_compression = out.len() as f32 / (data.len() as f32 * 8.0);

        let reader = Codec::open_from_bytes(OwnedBytes::new(out)).unwrap();
        assert_eq!(reader.num_vals(), data.len() as u64);
        for (doc, orig_val) in data.iter().copied().enumerate() {
            let val = reader.get_val(doc as u64);
            assert_eq!(
                val, orig_val,
                "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data \
                 `{data:?}`",
            );
        }
        Some((estimation, actual_compression))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn test_proptest_small(data in proptest::collection::vec(num_strategy(), 1..10)) {
            create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
            create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
            create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_proptest_large(data in proptest::collection::vec(num_strategy(), 1..6000)) {
            create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
            create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
            create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
        }

    }
    fn num_strategy() -> impl Strategy<Value = u64> {
        prop_oneof![
            1 => prop::num::u64::ANY.prop_map(|num| u64::MAX - (num % 10) ),
            1 => prop::num::u64::ANY.prop_map(|num| num % 10 ),
            20 => prop::num::u64::ANY,
        ]
    }

    pub fn get_codec_test_datasets() -> Vec<(Vec<u64>, &'static str)> {
        let mut data_and_names = vec![];

        let data = (10..=10_000_u64).collect::<Vec<_>>();
        data_and_names.push((data, "simple monotonically increasing"));

        data_and_names.push((
            vec![5, 6, 7, 8, 9, 10, 99, 100],
            "offset in linear interpol",
        ));
        data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
        data_and_names.push((vec![10], "single value"));

        data_and_names.push((
            vec![1572656989877777, 1170935903116329, 720575940379279, 0],
            "overflow error",
        ));

        data_and_names
    }

    fn test_codec<C: FastFieldCodec>() {
        let codec_name = format!("{:?}", C::CODEC_TYPE);
        for (data, dataset_name) in get_codec_test_datasets() {
            let estimate_actual_opt: Option<(f32, f32)> =
                crate::tests::create_and_validate::<C>(&data, dataset_name);
            let result = if let Some((estimate, actual)) = estimate_actual_opt {
                format!("Estimate `{estimate}` Actual `{actual}`")
            } else {
                "Disabled".to_string()
            };
            println!("Codec {codec_name}, DataSet {dataset_name}, {result}");
        }
    }
    #[test]
    fn test_codec_bitpacking() {
        test_codec::<BitpackedCodec>();
    }
    #[test]
    fn test_codec_interpolation() {
        test_codec::<LinearCodec>();
    }
    #[test]
    fn test_codec_multi_interpolation() {
        test_codec::<BlockwiseLinearCodec>();
    }

    use super::*;

    #[test]
    fn estimation_good_interpolation_case() {
        let data = (10..=20000_u64).collect::<Vec<_>>();
        let data: VecColumn = data.as_slice().into();

        let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
        assert_le!(linear_interpol_estimation, 0.01);

        let multi_linear_interpol_estimation = BlockwiseLinearCodec::estimate(&data).unwrap();
        assert_le!(multi_linear_interpol_estimation, 0.2);
        assert_le!(linear_interpol_estimation, multi_linear_interpol_estimation);

        let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
        assert_le!(linear_interpol_estimation, bitpacked_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case() {
        let data: &[u64] = &[200, 10, 10, 10, 10, 1000, 20];

        let data: VecColumn = data.into();
        let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
        assert_le!(linear_interpol_estimation, 0.32);

        let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case_monotonically_increasing() {
        let mut data: Vec<u64> = (200..=20000_u64).collect();
        data.push(1_000_000);
        let data: VecColumn = data.as_slice().into();

        // in this case the linear interpolation can't in fact not be worse than bitpacking,
        // but the estimator adds some threshold, which leads to estimated worse behavior
        let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
        assert_le!(linear_interpol_estimation, 0.35);

        let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
        assert_le!(bitpacked_estimation, 0.32);
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }

    #[test]
    fn test_fast_field_codec_type_to_code() {
        let mut count_codec = 0;
        for code in 0..=255 {
            if let Some(codec_type) = FastFieldCodecType::from_code(code) {
                assert_eq!(codec_type.to_code(), code);
                count_codec += 1;
            }
        }
        assert_eq!(count_codec, 4);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::sync::Arc;

    use rand::prelude::*;
    use test::{self, Bencher};

    use crate::Column;

    // Warning: this generates the same permutation at each call
    fn generate_permutation() -> Vec<u64> {
        let mut permutation: Vec<u64> = (0u64..100_000u64).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    // Warning: this generates the same permutation at each call
    fn generate_permutation_gcd() -> Vec<u64> {
        let mut permutation: Vec<u64> = (1u64..100_000u64).map(|el| el * 1000).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    #[bench]
    fn bench_intfastfield_jumpy_veclookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        let n = permutation.len();
        b.iter(|| {
            let mut a = 0u64;
            for _ in 0..n {
                a = permutation[a as usize];
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_jumpy_fflookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        let n = permutation.len();
        let column: Arc<dyn Column<u64>> = crate::serialize_and_load(&permutation);
        b.iter(|| {
            let mut a = 0u64;
            for _ in 0..n {
                a = column.get_val(a as u64);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_stride7_vec(b: &mut Bencher) {
        let permutation = generate_permutation();
        let n = permutation.len();
        b.iter(|| {
            let mut a = 0u64;
            for i in (0..n / 7).map(|val| val * 7) {
                a += permutation[i as usize];
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_stride7_fflookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        let n = permutation.len();
        let column: Arc<dyn Column<u64>> = crate::serialize_and_load(&permutation);
        b.iter(|| {
            let mut a = 0u64;
            for i in (0..n / 7).map(|val| val * 7) {
                a += column.get_val(i as u64);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_scan_all_fflookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        let n = permutation.len();
        let column: Arc<dyn Column<u64>> = crate::serialize_and_load(&permutation);
        b.iter(|| {
            let mut a = 0u64;
            for i in 0u64..n as u64 {
                a += column.get_val(i);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_scan_all_fflookup_gcd(b: &mut Bencher) {
        let permutation = generate_permutation_gcd();
        let n = permutation.len();
        let column: Arc<dyn Column<u64>> = crate::serialize_and_load(&permutation);
        b.iter(|| {
            let mut a = 0u64;
            for i in 0..n as u64 {
                a += column.get_val(i);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_scan_all_vec(b: &mut Bencher) {
        let permutation = generate_permutation();
        b.iter(|| {
            let mut a = 0u64;
            for i in 0..permutation.len() {
                a += permutation[i as usize] as u64;
            }
            a
        });
    }
}
