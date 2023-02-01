#![warn(missing_docs)]
#![cfg_attr(all(feature = "unstable", test), feature(test))]

//! # `fastfield_codecs`
//!
//! - Columnar storage of data for tantivy [`Column`].
//! - Encode data in different codecs.
//! - Monotonically map values to u64/u128

use std::fmt::Debug;
use std::io;
use std::io::Write;
use std::sync::Arc;

use common::{BinarySerializable, OwnedBytes};
use compact_space::CompactSpaceDecompressor;
pub use monotonic_mapping::{MonotonicallyMappableToU64, StrictlyMonotonicFn};
use monotonic_mapping::{StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal};
pub use monotonic_mapping_u128::MonotonicallyMappableToU128;
use serialize::U128Header;

mod compact_space;
pub(crate) mod monotonic_mapping;
pub(crate) mod monotonic_mapping_u128;
mod stats;
pub(crate) mod u64_based;

mod column;
pub mod serialize;

pub use serialize::serialize_column_values_u128;
pub use stats::Stats;
pub use u64_based::{
    load_u64_based_column_values, serialize_and_load_u64_based_column_values,
    serialize_u64_based_column_values, CodecType, ALL_U64_CODEC_TYPES,
};

pub use self::column::{monotonic_map_column, ColumnValues, IterColumn, VecColumn};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
/// Available codecs to use to encode the u128 (via [`MonotonicallyMappableToU128`]) converted data.
pub enum U128FastFieldCodecType {
    /// This codec takes a large number space (u128) and reduces it to a compact number space, by
    /// removing the holes.
    CompactSpace = 1,
}

impl BinarySerializable for U128FastFieldCodecType {
    fn serialize<W: Write + ?Sized>(&self, wrt: &mut W) -> io::Result<()> {
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
