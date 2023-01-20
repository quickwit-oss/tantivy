#![warn(missing_docs)]
#![cfg_attr(all(feature = "unstable", test), feature(test))]

//! # `fastfield_codecs`
//!
//! - Columnar storage of data for tantivy [`Column`].
//! - Encode data in different codecs.
//! - Monotonically map values to u64/u128

#[cfg(test)]
mod tests;

use std::fmt::Debug;
use std::io;
use std::io::Write;
use std::sync::Arc;

use common::{BinarySerializable, OwnedBytes};
use compact_space::CompactSpaceDecompressor;
pub use monotonic_mapping::{MonotonicallyMappableToU64, StrictlyMonotonicFn};
use monotonic_mapping::{
    StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
    StrictlyMonotonicMappingToInternalBaseval, StrictlyMonotonicMappingToInternalGCDBaseval,
};
pub use monotonic_mapping_u128::MonotonicallyMappableToU128;
use serialize::{Header, U128Header};

mod bitpacked;
mod blockwise_linear;
mod compact_space;
mod line;
mod linear;
pub(crate) mod monotonic_mapping;
pub(crate) mod monotonic_mapping_u128;

mod column;
mod gcd;
pub mod serialize;

pub use self::column::{monotonic_map_column, ColumnValues, IterColumn, VecColumn};
#[cfg(test)]
pub use self::serialize::tests::serialize_and_load;
pub use self::serialize::{serialize_column_values, NormalizedHeader};
use crate::column_values::bitpacked::BitpackedCodec;
use crate::column_values::blockwise_linear::BlockwiseLinearCodec;
use crate::column_values::linear::LinearCodec;

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
pub fn open_u128_mapped<T: MonotonicallyMappableToU128 + Debug>(
    mut bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let header = U128Header::deserialize(&mut bytes)?;
    assert_eq!(header.codec_type, U128FastFieldCodecType::CompactSpace);
    let reader = CompactSpaceDecompressor::open(bytes)?;

    let inverted: StrictlyMonotonicMappingInverter<StrictlyMonotonicMappingToInternal<T>> =
        StrictlyMonotonicMappingToInternal::<T>::new().into();
    Ok(Arc::new(monotonic_map_column(reader, inverted)))
}

/// Returns the correct codec reader wrapped in the `Arc` for the data.
pub fn open_u64_mapped<T: MonotonicallyMappableToU64 + Debug>(
    mut bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let header = Header::deserialize(&mut bytes)?;
    match header.codec_type {
        FastFieldCodecType::Bitpacked => open_specific_codec::<BitpackedCodec, _>(bytes, &header),
        FastFieldCodecType::Linear => open_specific_codec::<LinearCodec, _>(bytes, &header),
        FastFieldCodecType::BlockwiseLinear => {
            open_specific_codec::<BlockwiseLinearCodec, _>(bytes, &header)
        }
    }
}

fn open_specific_codec<C: FastFieldCodec, Item: MonotonicallyMappableToU64 + Debug>(
    bytes: OwnedBytes,
    header: &Header,
) -> io::Result<Arc<dyn ColumnValues<Item>>> {
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
pub(crate) trait FastFieldCodec: 'static {
    /// A codex needs to provide a unique name and id, which is
    /// used for debugging and de/serialization.
    const CODEC_TYPE: FastFieldCodecType;

    type Reader: ColumnValues<u64> + 'static;

    /// Reads the metadata and returns the CodecReader
    fn open_from_bytes(bytes: OwnedBytes, header: NormalizedHeader) -> io::Result<Self::Reader>;

    /// Serializes the data using the serializer into write.
    ///
    /// The column iterator should be preferred over using column `get_val` method for
    /// performance reasons.
    fn serialize(column: &dyn ColumnValues, write: &mut impl Write) -> io::Result<()>;

    /// Returns an estimate of the compression ratio.
    /// If the codec is not applicable, returns `None`.
    ///
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(column: &dyn ColumnValues) -> Option<f32>;
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::sync::Arc;

    use common::OwnedBytes;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::{self, Bencher};

    use super::*;

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
        let normalized_header = NormalizedHeader {
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
    fn bench_get_dynamic_helper(b: &mut Bencher, col: Arc<dyn ColumnValues>) {
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
