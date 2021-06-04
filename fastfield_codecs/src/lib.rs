#[cfg(test)]
#[macro_use]
extern crate more_asserts;

pub mod bitpacked;
pub mod linearinterpol;

pub trait CodecReader: Sized {
    /// reads the metadata and returns the CodecReader
    fn open_from_bytes(bytes: &[u8]) -> std::io::Result<Self>;

    fn get_u64(&self, doc: u64, data: &[u8]) -> u64;

    fn min_value(&self) -> u64;
    fn max_value(&self) -> u64;
}

/// FastFieldDataAccess is the trait to access fast field data during serialization and estimation.
pub trait FastFieldDataAccess: Clone {
    /// Return the value associated to the given document.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance reasons.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    fn get(&self, doc: u32) -> u64;
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldSerializerEstimate {
    /// returns an estimate of the compression ratio.
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess, stats: FastFieldStats) -> f32;
}

/// `CodecId` is required by each Codec.
///
/// It needs to provide a unique name and id, which is
/// used for debugging and de/serialization.
pub trait CodecId {
    const NAME: &'static str;
    const ID: u8;
}

#[derive(Debug, Clone)]
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
}

impl<'a> FastFieldDataAccess for &'a [u64] {
    fn get(&self, doc: u32) -> u64 {
        self[doc as usize]
    }
}

impl FastFieldDataAccess for Vec<u64> {
    fn get(&self, doc: u32) -> u64 {
        self[doc as usize]
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bitpacked::BitpackedFastFieldSerializer, linearinterpol::LinearInterpolFastFieldSerializer,
    };

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
        let data = (10..=20_u64).collect::<Vec<_>>();

        let linear_interpol_estimation =
            LinearInterpolFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.1);

        let bitpacked_estimation =
            BitpackedFastFieldSerializer::<Vec<u8>>::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, bitpacked_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case() {
        let data = vec![200, 10, 10, 10, 10, 1000, 20];

        let linear_interpol_estimation =
            LinearInterpolFastFieldSerializer::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.3);

        let bitpacked_estimation =
            BitpackedFastFieldSerializer::<Vec<u8>>::estimate(&data, stats_from_vec(&data));
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
            BitpackedFastFieldSerializer::<Vec<u8>>::estimate(&data, stats_from_vec(&data));
        assert_le!(bitpacked_estimation, 0.32);
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
}
