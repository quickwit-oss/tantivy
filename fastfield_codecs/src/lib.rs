#[cfg(test)]
#[macro_use]
extern crate more_asserts;

use std::io;
use std::io::Write;

pub mod bitpacked;
pub mod linearinterpol;
pub mod multilinearinterpol;

pub trait FastFieldCodecReader: Sized {
    /// reads the metadata and returns the CodecReader
    fn open_from_bytes(bytes: &[u8]) -> std::io::Result<Self>;

    fn get_u64(&self, doc: u64, data: &[u8]) -> u64;

    fn min_value(&self) -> u64;
    fn max_value(&self) -> u64;
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldCodecSerializer {
    /// A codex needs to provide a unique name and id, which is
    /// used for debugging and de/serialization.
    const NAME: &'static str;
    const ID: u8;

    /// Check if the Codec is able to compress the data
    fn is_applicable(fastfield_accessor: &impl FastFieldDataAccess, stats: FastFieldStats) -> bool;

    /// Returns an estimate of the compression ratio.
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess, stats: FastFieldStats) -> f32;

    /// Serializes the data using the serializer into write.
    /// There are multiple iterators, in case the codec needs to read the data multiple times.
    /// The iterators should be preferred over using fastfield_accessor for performance reasons.
    fn serialize(
        write: &mut impl Write,
        fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        data_iter1: impl Iterator<Item = u64>,
    ) -> io::Result<()>;
}

/// FastFieldDataAccess is the trait to access fast field data during serialization and estimation.
pub trait FastFieldDataAccess {
    /// Return the value associated to the given position.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance reasons.
    ///
    /// # Panics
    ///
    /// May panic if `position` is greater than the index.
    fn get_val(&self, position: u64) -> u64;
}

#[derive(Debug, Clone)]
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
}

impl<'a> FastFieldDataAccess for &'a [u64] {
    fn get_val(&self, position: u64) -> u64 {
        self[position as usize]
    }
}

impl FastFieldDataAccess for Vec<u64> {
    fn get_val(&self, position: u64) -> u64 {
        self[position as usize]
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bitpacked::{BitpackedFastFieldReader, BitpackedFastFieldSerializer},
        linearinterpol::{LinearInterpolFastFieldReader, LinearInterpolFastFieldSerializer},
        multilinearinterpol::{
            MultiLinearInterpolFastFieldReader, MultiLinearInterpolFastFieldSerializer,
        },
    };

    pub fn create_and_validate<S: FastFieldCodecSerializer, R: FastFieldCodecReader>(
        data: &[u64],
        name: &str,
    ) -> (f32, f32) {
        if !S::is_applicable(&data, crate::tests::stats_from_vec(data)) {
            return (f32::MAX, 0.0);
        }
        let estimation = S::estimate(&data, crate::tests::stats_from_vec(data));
        let mut out = vec![];
        S::serialize(
            &mut out,
            &data,
            crate::tests::stats_from_vec(data),
            data.iter().cloned(),
            data.iter().cloned(),
        )
        .unwrap();

        let reader = R::open_from_bytes(&out).unwrap();
        for (doc, orig_val) in data.iter().enumerate() {
            let val = reader.get_u64(doc as u64, &out);
            if val != *orig_val {
                panic!(
                    "val {:?} does not match orig_val {:?}, in data set {}, data {:?}",
                    val, orig_val, name, data
                );
            }
        }
        let actual_compression = data.len() as f32 / out.len() as f32;
        (estimation, actual_compression)
    }
    pub fn get_codec_test_data_sets() -> Vec<(Vec<u64>, &'static str)> {
        let mut data_and_names = vec![];

        let data = (10..=20_u64).collect::<Vec<_>>();
        data_and_names.push((data, "simple monotonically increasing"));

        data_and_names.push((
            vec![5, 6, 7, 8, 9, 10, 99, 100],
            "offset in linear interpol",
        ));
        data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
        data_and_names.push((vec![10], "single value"));

        data_and_names
    }

    fn test_codec<S: FastFieldCodecSerializer, R: FastFieldCodecReader>() {
        let codec_name = S::NAME;
        for (data, data_set_name) in get_codec_test_data_sets() {
            let (estimate, actual) =
                crate::tests::create_and_validate::<S, R>(&data, data_set_name);
            let result = if estimate == f32::MAX {
                "Disabled".to_string()
            } else {
                format!("Estimate {:?} Actual {:?} ", estimate, actual)
            };
            println!(
                "Codec {}, DataSet {}, {}",
                codec_name, data_set_name, result
            );
        }
    }
    #[test]
    fn test_codec_bitpacking() {
        test_codec::<BitpackedFastFieldSerializer, BitpackedFastFieldReader>();
    }
    #[test]
    fn test_codec_interpolation() {
        test_codec::<LinearInterpolFastFieldSerializer, LinearInterpolFastFieldReader>();
    }
    #[test]
    fn test_codec_multi_interpolation() {
        test_codec::<MultiLinearInterpolFastFieldSerializer, MultiLinearInterpolFastFieldReader>();
    }

    use super::*;
    pub fn stats_from_vec(data: &[u64]) -> FastFieldStats {
        let min_value = data.iter().cloned().min().unwrap_or(0);
        let max_value = data.iter().cloned().max().unwrap_or(0);
        FastFieldStats {
            min_value,
            max_value,
            num_vals: data.len() as u64,
        }
    }

    #[test]
    fn estimation_good_interpolation_case() {
        let data = (10..=20000_u64).collect::<Vec<_>>();

        let linear_interpol_estimation =
            LinearInterpolFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.01);

        let multi_linear_interpol_estimation =
            MultiLinearInterpolFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(multi_linear_interpol_estimation, 0.2);
        assert_le!(linear_interpol_estimation, multi_linear_interpol_estimation);

        let bitpacked_estimation =
            BitpackedFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, bitpacked_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case() {
        let data = vec![200, 10, 10, 10, 10, 1000, 20];

        let linear_interpol_estimation =
            LinearInterpolFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.32);

        let bitpacked_estimation =
            BitpackedFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case_monotonically_increasing() {
        let mut data = (200..=20000_u64).collect::<Vec<_>>();
        data.push(1_000_000);

        // in this case the linear interpolation can't in fact not be worse than bitpacking,
        // but the estimator adds some threshold, which leads to estimated worse behavior
        let linear_interpol_estimation =
            LinearInterpolFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.35);

        let bitpacked_estimation =
            BitpackedFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(bitpacked_estimation, 0.32);
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
}
