#![warn(missing_docs)]
#![cfg_attr(all(feature = "unstable", test), feature(test))]

//! # `fastfield_codecs`
//!
//! - Columnar storage of data for tantivy [`Column`].
//! - Encode data in different codecs.
//! - Monotonically map values to u64/u128

#[cfg(test)]
#[macro_use]
extern crate more_asserts;

#[cfg(all(test, feature = "unstable"))]
extern crate test;

use std::io;
use std::io::Write;
use std::sync::Arc;

use common::BinarySerializable;
use compact_space::CompactSpaceDecompressor;
use format_version::read_format_version;
use monotonic_mapping::{
    StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
    StrictlyMonotonicMappingToInternalBaseval, StrictlyMonotonicMappingToInternalGCDBaseval,
};
use null_index_footer::read_null_index_footer;
use ownedbytes::OwnedBytes;
use serialize::{Header, U128Header};

mod bitpacked;
mod blockwise_linear;
mod compact_space;
mod format_version;
mod line;
mod linear;
mod monotonic_mapping;
mod monotonic_mapping_u128;
#[allow(dead_code)]
mod null_index;
mod null_index_footer;

mod column;
mod gcd;
pub mod serialize;

pub use ordered_float;

use self::bitpacked::BitpackedCodec;
use self::blockwise_linear::BlockwiseLinearCodec;
pub use self::column::{monotonic_map_column, Column, IterColumn, VecColumn};
use self::linear::LinearCodec;
pub use self::monotonic_mapping::{MonotonicallyMappableToU64, StrictlyMonotonicFn};
pub use self::monotonic_mapping_u128::MonotonicallyMappableToU128;
pub use self::serialize::{
    estimate, serialize, serialize_and_load, serialize_u128, NormalizedHeader,
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
/// Available codecs to use to encode the u64 (via [`MonotonicallyMappableToU64`]) converted data.
pub enum FastFieldCodecType {
    /// Bitpack all values in the value range. The number of bits is defined by the amplitude
    /// `column.max_value() - column.min_value()`
    Bitpacked = 1,
    /// Linear interpolation puts a line between the first and last value and then bitpacks the
    /// values by the offset from the line. The number of bits is defined by the max deviation from
    /// the line.
    Linear = 2,
    /// Same as [`FastFieldCodecType::Linear`], but encodes in blocks of 512 elements.
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
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Bitpacked),
            2 => Some(Self::Linear),
            3 => Some(Self::BlockwiseLinear),
            _ => None,
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
/// Available codecs to use to encode the u128 (via [`MonotonicallyMappableToU128`]) converted data.
pub enum U128FastFieldCodecType {
    /// This codec takes a large number space (u128) and reduces it to a compact number space, by
    /// removing the holes.
    CompactSpace = 1,
}

impl BinarySerializable for U128FastFieldCodecType {
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

impl U128FastFieldCodecType {
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::CompactSpace),
            _ => None,
        }
    }
}

/// Returns the correct codec reader wrapped in the `Arc` for the data.
pub fn open_u128<Item: MonotonicallyMappableToU128>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn Column<Item>>> {
    let (bytes, _format_version) = read_format_version(bytes)?;
    let (mut bytes, _null_index_footer) = read_null_index_footer(bytes)?;
    let header = U128Header::deserialize(&mut bytes)?;
    assert_eq!(header.codec_type, U128FastFieldCodecType::CompactSpace);
    let reader = CompactSpaceDecompressor::open(bytes)?;
    let inverted: StrictlyMonotonicMappingInverter<StrictlyMonotonicMappingToInternal<Item>> =
        StrictlyMonotonicMappingToInternal::<Item>::new().into();
    Ok(Arc::new(monotonic_map_column(reader, inverted)))
}

/// Returns the correct codec reader wrapped in the `Arc` for the data.
pub fn open<T: MonotonicallyMappableToU64>(bytes: OwnedBytes) -> io::Result<Arc<dyn Column<T>>> {
    let (bytes, _format_version) = read_format_version(bytes)?;
    let (mut bytes, _null_index_footer) = read_null_index_footer(bytes)?;
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
        let mapping = StrictlyMonotonicMappingInverter::from(
            StrictlyMonotonicMappingToInternalGCDBaseval::new(gcd.get(), min_value),
        );
        Ok(Arc::new(monotonic_map_column(reader, mapping)))
    } else {
        let mapping = StrictlyMonotonicMappingInverter::from(
            StrictlyMonotonicMappingToInternalBaseval::new(min_value),
        );
        Ok(Arc::new(monotonic_map_column(reader, mapping)))
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
    fn serialize(column: &dyn Column, write: &mut impl Write) -> io::Result<()>;

    /// Returns an estimate of the compression ratio.
    /// If the codec is not applicable, returns `None`.
    ///
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(column: &dyn Column) -> Option<f32>;
}

/// The list of all available codecs for u64 convertible data.
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
        let header = Header::compute_header(col, &[Codec::CODEC_TYPE])?;
        let normalized_col = header.normalize_column(col);
        let estimation = Codec::estimate(&normalized_col)?;

        let mut out = Vec::new();
        let col = VecColumn::from(data);
        serialize(col, &mut out, &[Codec::CODEC_TYPE]).unwrap();

        let actual_compression = out.len() as f32 / (data.len() as f32 * 8.0);

        let reader = crate::open::<u64>(OwnedBytes::new(out)).unwrap();
        assert_eq!(reader.num_vals(), data.len() as u32);
        for (doc, orig_val) in data.iter().copied().enumerate() {
            let val = reader.get_val(doc as u32);
            assert_eq!(
                val, orig_val,
                "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data \
                 `{data:?}`",
            );
        }

        if !data.is_empty() {
            let test_rand_idx = rand::thread_rng().gen_range(0..=data.len() - 1);
            let expected_positions: Vec<u32> = data
                .iter()
                .enumerate()
                .filter(|(_, el)| **el == data[test_rand_idx])
                .map(|(pos, _)| pos as u32)
                .collect();
            let mut positions = Vec::new();
            reader.get_docids_for_value_range(
                data[test_rand_idx]..=data[test_rand_idx],
                0..data.len() as u32,
                &mut positions,
            );
            assert_eq!(expected_positions, positions);
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
        let mut data: Vec<u64> = (201..=20000_u64).collect();
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
    use std::sync::Arc;

    use ownedbytes::OwnedBytes;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::{self, Bencher};

    use super::*;
    use crate::Column;

    fn get_data() -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(2u64);
        let mut data: Vec<_> = (100..55000_u64)
            .map(|num| num + rng.gen::<u8>() as u64)
            .collect();
        data.push(99_000);
        data.insert(1000, 2000);
        data.insert(2000, 100);
        data.insert(3000, 4100);
        data.insert(4000, 100);
        data.insert(5000, 800);
        data
    }

    #[inline(never)]
    fn value_iter() -> impl Iterator<Item = u64> {
        0..20_000
    }
    fn get_reader_for_bench<Codec: FastFieldCodec>(data: &[u64]) -> Codec::Reader {
        let mut bytes = Vec::new();
        let min_value = *data.iter().min().unwrap();
        let data = data.iter().map(|el| *el - min_value).collect::<Vec<_>>();
        let col = VecColumn::from(&data);
        let normalized_header = crate::NormalizedHeader {
            num_vals: col.num_vals(),
            max_value: col.max_value(),
        };
        Codec::serialize(&VecColumn::from(&data), &mut bytes).unwrap();
        Codec::open_from_bytes(OwnedBytes::new(bytes), normalized_header).unwrap()
    }
    fn bench_get<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let col = get_reader_for_bench::<Codec>(data);
        b.iter(|| {
            let mut sum = 0u64;
            for pos in value_iter() {
                let val = col.get_val(pos as u32);
                sum = sum.wrapping_add(val);
            }
            sum
        });
    }

    #[inline(never)]
    fn bench_get_dynamic_helper(b: &mut Bencher, col: Arc<dyn Column>) {
        b.iter(|| {
            let mut sum = 0u64;
            for pos in value_iter() {
                let val = col.get_val(pos as u32);
                sum = sum.wrapping_add(val);
            }
            sum
        });
    }

    fn bench_get_dynamic<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let col = Arc::new(get_reader_for_bench::<Codec>(data));
        bench_get_dynamic_helper(b, col);
    }
    fn bench_create<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let min_value = *data.iter().min().unwrap();
        let data = data.iter().map(|el| *el - min_value).collect::<Vec<_>>();

        let mut bytes = Vec::new();
        b.iter(|| {
            bytes.clear();
            Codec::serialize(&VecColumn::from(&data), &mut bytes).unwrap();
        });
    }

    #[bench]
    fn bench_fastfield_bitpack_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<BitpackedCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<LinearCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<BlockwiseLinearCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_bitpack_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<BitpackedCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_bitpack_get_dynamic(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get_dynamic::<BitpackedCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<LinearCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_get_dynamic(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get_dynamic::<LinearCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<BlockwiseLinearCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_get_dynamic(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get_dynamic::<BlockwiseLinearCodec>(b, &data);
    }
}
