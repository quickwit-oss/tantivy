#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(test)]
#[macro_use]
extern crate more_asserts;

#[cfg(all(test, feature = "unstable"))]
extern crate test;

use std::io;
use std::io::Write;
use std::sync::Arc;

use column::ColumnExt;
use common::BinarySerializable;
use compact_space::CompactSpaceDecompressor;
use ownedbytes::OwnedBytes;
use serialize::Header;

mod bitpacked;
mod blockwise_linear;
mod compact_space;
mod line;
mod linear;
mod monotonic_mapping;

mod column;
mod gcd;
mod serialize;

use self::bitpacked::BitpackedCodec;
use self::blockwise_linear::BlockwiseLinearCodec;
pub use self::column::{monotonic_map_column, Column, VecColumn};
pub use self::compact_space::ip_to_u128;
use self::linear::LinearCodec;
pub use self::monotonic_mapping::MonotonicallyMappableToU64;
pub use self::serialize::{
    estimate, serialize, serialize_and_load, serialize_u128, NormalizedHeader,
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
pub enum FastFieldCodecType {
    Bitpacked = 1,
    Linear = 2,
    BlockwiseLinear = 3,
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
            _ => None,
        }
    }
}

/// Returns the correct codec reader wrapped in the `Arc` for the data.
pub fn open_u128(bytes: OwnedBytes) -> io::Result<Arc<dyn ColumnExt<u128>>> {
    Ok(Arc::new(CompactSpaceDecompressor::open(bytes)?))
}

/// Returns the correct codec reader wrapped in the `Arc` for the data.
pub fn open<T: MonotonicallyMappableToU64>(
    mut bytes: OwnedBytes,
) -> io::Result<Arc<dyn Column<T>>> {
    let header = Header::deserialize(&mut bytes)?;
    match header.codec_type {
        FastFieldCodecType::Bitpacked => open_specific_codec::<BitpackedCodec, _>(bytes, &header),
        FastFieldCodecType::Linear => open_specific_codec::<LinearCodec, _>(bytes, &header),
        FastFieldCodecType::BlockwiseLinear => {
            open_specific_codec::<BlockwiseLinearCodec, _>(bytes, &header)
        }
    }
}

fn open_specific_codec<C: FastFieldCodec, Item: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
    header: &Header,
) -> io::Result<Arc<dyn Column<Item>>> {
    let normalized_header = header.normalized();
    let reader = C::open_from_bytes(bytes, normalized_header)?;
    let min_value = header.min_value;
    if let Some(gcd) = header.gcd {
        let monotonic_mapping = move |val: u64| Item::from_u64(min_value + val * gcd.get());
        Ok(Arc::new(monotonic_map_column(reader, monotonic_mapping)))
    } else {
        let monotonic_mapping = move |val: u64| Item::from_u64(min_value + val);
        Ok(Arc::new(monotonic_map_column(reader, monotonic_mapping)))
    }
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
trait FastFieldCodec: 'static {
    /// A codex needs to provide a unique name and id, which is
    /// used for debugging and de/serialization.
    const CODEC_TYPE: FastFieldCodecType;

    type Reader: Column<u64> + 'static;

    /// Reads the metadata and returns the CodecReader
    fn open_from_bytes(bytes: OwnedBytes, header: NormalizedHeader) -> io::Result<Self::Reader>;

    /// Serializes the data using the serializer into write.
    ///
    /// The column iterator should be preferred over using column `get_val` method for
    /// performance reasons.
    fn serialize(column: &dyn Column<u64>, write: &mut impl Write) -> io::Result<()>;

    /// Returns an estimate of the compression ratio.
    /// If the codec is not applicable, returns `None`.
    ///
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(column: &impl Column) -> Option<f32>;
}

pub const ALL_CODEC_TYPES: [FastFieldCodecType; 3] = [
    FastFieldCodecType::Bitpacked,
    FastFieldCodecType::BlockwiseLinear,
    FastFieldCodecType::Linear,
];

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use crate::bitpacked::BitpackedCodec;
    use crate::blockwise_linear::BlockwiseLinearCodec;
    use crate::linear::LinearCodec;
    use crate::serialize::Header;

    pub(crate) fn create_and_validate<Codec: FastFieldCodec>(
        data: &[u64],
        name: &str,
    ) -> Option<(f32, f32)> {
        let col = &VecColumn::from(data);
        let header = Header::compute_header(&col, &[Codec::CODEC_TYPE])?;
        let normalized_col = header.normalize_column(col);
        let estimation = Codec::estimate(&normalized_col)?;

        let mut out = Vec::new();
        let col = VecColumn::from(data);
        serialize(col, &mut out, &[Codec::CODEC_TYPE]).unwrap();

        let actual_compression = out.len() as f32 / (data.len() as f32 * 8.0);

        let reader = crate::open::<u64>(OwnedBytes::new(out)).unwrap();
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
        fn test_proptest_small_bitpacked(data in proptest::collection::vec(num_strategy(), 1..10)) {
            create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
        }

        #[test]
        fn test_proptest_small_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
            create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
        }

        #[test]
        fn test_proptest_small_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
            create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        #[test]
        fn test_proptest_large_bitpacked(data in proptest::collection::vec(num_strategy(), 1..6000)) {
            create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
        }

        #[test]
        fn test_proptest_large_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
            create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
        }

        #[test]
        fn test_proptest_large_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
            create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
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
        assert_lt!(linear_interpol_estimation, multi_linear_interpol_estimation);

        let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
        assert_lt!(linear_interpol_estimation, bitpacked_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case() {
        let data: &[u64] = &[200, 10, 10, 10, 10, 1000, 20];

        let data: VecColumn = data.into();
        let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
        assert_le!(linear_interpol_estimation, 0.34);

        let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
        assert_lt!(bitpacked_estimation, linear_interpol_estimation);
    }

    #[test]
    fn estimation_prefer_bitpacked() {
        let data = VecColumn::from(&[10, 10, 10, 10]);
        let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
        let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
        assert_lt!(bitpacked_estimation, linear_interpol_estimation);
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
        assert_eq!(count_codec, 3);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::iter;
    use std::sync::Arc;

    use column::ColumnExt;
    use rand::prelude::*;
    use test::{self, Bencher};

    use super::*;
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

    fn get_exp_data() -> Vec<u64> {
        let mut data = vec![];
        for i in 0..100 {
            let num = i * i;
            data.extend(iter::repeat(i as u64).take(num));
        }
        data.shuffle(&mut StdRng::from_seed([1u8; 32]));

        // lengt = 328350
        data
    }

    fn get_u128_column_permutation() -> Arc<dyn ColumnExt<u128>> {
        let permutation = generate_permutation();
        let permutation = permutation.iter().map(|el| *el as u128).collect::<Vec<_>>();
        get_u128_column(&permutation)
    }
    fn get_data_50percent_item() -> (u128, u128, Vec<u128>) {
        let mut permutation = get_exp_data();
        let major_item = 20;
        let minor_item = 10;
        permutation.extend(iter::repeat(major_item).take(permutation.len()));
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        let permutation = permutation.iter().map(|el| *el as u128).collect::<Vec<_>>();
        (major_item as u128, minor_item as u128, permutation)
    }
    fn get_u128_column(data: &[u128]) -> Arc<dyn ColumnExt<u128>> {
        let mut out = vec![];
        serialize_u128(VecColumn::from(&data), &mut out).unwrap();
        let out = OwnedBytes::new(out);
        open_u128(out).unwrap()
    }

    #[bench]
    fn bench_intfastfield_getrange_u128_50percent_hit(b: &mut Bencher) {
        let (major_item, _minor_item, data) = get_data_50percent_item();
        let column = get_u128_column(&data);

        b.iter(|| column.get_between_vals(major_item..=major_item));
    }

    #[bench]
    fn bench_intfastfield_getrange_u128_single_hit(b: &mut Bencher) {
        let (_major_item, minor_item, data) = get_data_50percent_item();
        let column = get_u128_column(&data);

        b.iter(|| column.get_between_vals(minor_item..=minor_item));
    }

    #[bench]
    fn bench_intfastfield_getrange_u128_hit_all(b: &mut Bencher) {
        let (_major_item, _minor_item, data) = get_data_50percent_item();
        let column = get_u128_column(&data);

        b.iter(|| column.get_between_vals(0..=u128::MAX));
    }

    #[bench]
    fn bench_intfastfield_jumpy_fflookup_u128(b: &mut Bencher) {
        let column = get_u128_column_permutation();

        b.iter(|| {
            let mut a = 0u128;
            for _ in 0..column.num_vals() {
                a = column.get_val(a as u64);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_jumpy_stride5_u128(b: &mut Bencher) {
        let column = get_u128_column_permutation();

        b.iter(|| {
            let n = column.num_vals();
            let mut a = 0u128;
            for i in (0..n / 5).map(|val| val * 5) {
                a += column.get_val(i as u64);
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
