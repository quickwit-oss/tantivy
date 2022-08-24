#[cfg(test)]
#[macro_use]
extern crate more_asserts;

use std::io;
use std::io::Write;

use common::BinarySerializable;
use ownedbytes::OwnedBytes;

pub mod bitpacked;
pub mod blockwise_linear;
pub mod linear;

pub trait FastFieldCodecReader: Sized {
    /// reads the metadata and returns the CodecReader
    fn open_from_bytes(bytes: OwnedBytes) -> std::io::Result<Self>;
    fn get_u64(&self, doc: u64) -> u64;
    fn min_value(&self) -> u64;
    fn max_value(&self) -> u64;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
pub enum FastFieldCodecType {
    Bitpacked = 1,
    LinearInterpol = 2,
    BlockwiseLinearInterpol = 3,
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
            2 => Some(Self::LinearInterpol),
            3 => Some(Self::BlockwiseLinearInterpol),
            4 => Some(Self::Gcd),
            _ => None,
        }
    }
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldCodecSerializer {
    /// A codex needs to provide a unique name and id, which is
    /// used for debugging and de/serialization.
    const CODEC_TYPE: FastFieldCodecType;

    /// Check if the Codec is able to compress the data
    fn is_applicable(fastfield_accessor: &impl FastFieldDataAccess) -> bool;

    /// Returns an estimate of the compression ratio.
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess) -> f32;

    /// Serializes the data using the serializer into write.
    ///
    /// The fastfield_accessor iterator should be preferred over using fastfield_accessor for
    /// performance reasons.
    fn serialize(
        write: &mut impl Write,
        fastfield_accessor: &dyn FastFieldDataAccess,
    ) -> io::Result<()>;
}

/// FastFieldDataAccess is the trait to access fast field data during serialization and estimation.
pub trait FastFieldDataAccess {
    /// Return the value associated to the given position.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance
    /// reasons.
    ///
    /// # Panics
    ///
    /// May panic if `position` is greater than the index.
    fn get_val(&self, position: u64) -> u64;

    /// Returns a iterator over the data
    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_>;

    /// min value of the data
    fn min_value(&self) -> u64;

    /// max value of the data
    fn max_value(&self) -> u64;

    /// num vals
    fn num_vals(&self) -> u64;
}

#[derive(Debug, Clone)]
/// Statistics are used in codec detection and stored in the fast field footer.
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
}

impl<'a> FastFieldDataAccess for &'a [u64] {
    fn get_val(&self, position: u64) -> u64 {
        self[position as usize]
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = u64> + 'b> {
        Box::new((self as &[u64]).iter().cloned())
    }

    fn min_value(&self) -> u64 {
        self.iter().min().unwrap_or(0)
    }

    fn max_value(&self) -> u64 {
        self.iter().max().unwrap_or(0)
    }

    fn num_vals(&self) -> u64 {
        self.len() as u64
    }
}

impl FastFieldDataAccess for Vec<u64> {
    fn get_val(&self, position: u64) -> u64 {
        self[position as usize]
    }
    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = u64> + 'b> {
        Box::new((self as &[u64]).iter().cloned())
    }
    fn min_value(&self) -> u64 {
        self.iter().min().unwrap_or(0)
    }

    fn max_value(&self) -> u64 {
        self.iter().max().unwrap_or(0)
    }

    fn num_vals(&self) -> u64 {
        self.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use proptest::arbitrary::any;
    use proptest::proptest;

    use crate::bitpacked::{BitpackedFastFieldReader, BitpackedFastFieldSerializer};
    use crate::blockwise_linear::{
        BlockwiseLinearInterpolFastFieldSerializer, MultiLinearInterpolFastFieldReader,
    };
    use crate::linear::{LinearInterpolFastFieldReader, LinearInterpolFastFieldSerializer};

    pub fn create_and_validate<S: FastFieldCodecSerializer, R: FastFieldCodecReader>(
        data: &[u64],
        name: &str,
    ) -> (f32, f32) {
        if !S::is_applicable(&data) {
            return (f32::MAX, 0.0);
        }
        let estimation = S::estimate(&data);
        let mut out: Vec<u8> = Vec::new();
        S::serialize(&mut out, &data).unwrap();

        let actual_compression = out.len() as f32 / (data.len() as f32 * 8.0);

        let reader = R::open_from_bytes(OwnedBytes::new(out)).unwrap();
        for (doc, orig_val) in data.iter().enumerate() {
            let val = reader.get_u64(doc as u64);
            if val != *orig_val {
                panic!(
                    "val {val:?} does not match orig_val {orig_val:?}, in data set {name}, data \
                     {data:?}",
                );
            }
        }
        (estimation, actual_compression)
    }

    proptest! {
        #[test]
        fn test_proptest_small(data in proptest::collection::vec(any::<u64>(), 1..10)) {
            create_and_validate::<LinearInterpolFastFieldSerializer, LinearInterpolFastFieldReader>(&data, "proptest linearinterpol");
            create_and_validate::<BlockwiseLinearInterpolFastFieldSerializer, MultiLinearInterpolFastFieldReader>(&data, "proptest multilinearinterpol");
            create_and_validate::<BitpackedFastFieldSerializer, BitpackedFastFieldReader>(&data, "proptest bitpacked");
        }

        #[test]
        fn test_proptest_large(data in proptest::collection::vec(any::<u64>(), 1..6000)) {
            create_and_validate::<LinearInterpolFastFieldSerializer, LinearInterpolFastFieldReader>(&data, "proptest linearinterpol");
            create_and_validate::<BlockwiseLinearInterpolFastFieldSerializer, MultiLinearInterpolFastFieldReader>(&data, "proptest multilinearinterpol");
            create_and_validate::<BitpackedFastFieldSerializer, BitpackedFastFieldReader>(&data, "proptest bitpacked");
        }

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

        data_and_names.push((
            vec![1572656989877777, 1170935903116329, 720575940379279, 0],
            "overflow error",
        ));

        data_and_names
    }

    fn test_codec<S: FastFieldCodecSerializer, R: FastFieldCodecReader>() {
        let codec_name = format!("{:?}", S::CODEC_TYPE);
        for (data, dataset_name) in get_codec_test_data_sets() {
            let (estimate, actual) = crate::tests::create_and_validate::<S, R>(&data, dataset_name);
            let result = if estimate == f32::MAX {
                "Disabled".to_string()
            } else {
                format!("Estimate `{estimate}` Actual `{actual}`")
            };
            println!("Codec {codec_name}, DataSet {dataset_name}, {result}");
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
        test_codec::<BlockwiseLinearInterpolFastFieldSerializer, MultiLinearInterpolFastFieldReader>(
        );
    }

    use super::*;

    #[test]
    fn estimation_good_interpolation_case() {
        let data = (10..=20000_u64).collect::<Vec<_>>();

        let linear_interpol_estimation = LinearInterpolFastFieldSerializer::estimate(&data);
        assert_le!(linear_interpol_estimation, 0.01);

        let multi_linear_interpol_estimation =
            BlockwiseLinearInterpolFastFieldSerializer::estimate(&data);
        assert_le!(multi_linear_interpol_estimation, 0.2);
        assert_le!(linear_interpol_estimation, multi_linear_interpol_estimation);

        let bitpacked_estimation = BitpackedFastFieldSerializer::estimate(&data);
        assert_le!(linear_interpol_estimation, bitpacked_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case() {
        let data = vec![200, 10, 10, 10, 10, 1000, 20];

        let linear_interpol_estimation = LinearInterpolFastFieldSerializer::estimate(&data);
        assert_le!(linear_interpol_estimation, 0.32);

        let bitpacked_estimation = BitpackedFastFieldSerializer::estimate(&data);
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case_monotonically_increasing() {
        let mut data = (200..=20000_u64).collect::<Vec<_>>();
        data.push(1_000_000);

        // in this case the linear interpolation can't in fact not be worse than bitpacking,
        // but the estimator adds some threshold, which leads to estimated worse behavior
        let linear_interpol_estimation = LinearInterpolFastFieldSerializer::estimate(&data);
        assert_le!(linear_interpol_estimation, 0.35);

        let bitpacked_estimation = BitpackedFastFieldSerializer::estimate(&data);
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
