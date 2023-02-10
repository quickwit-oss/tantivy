mod bitpacked;
mod blockwise_linear;
mod line;
mod linear;
mod stats_collector;

use std::io;
use std::io::Write;
use std::sync::Arc;

use common::{BinarySerializable, OwnedBytes};

use crate::column_values::monotonic_mapping::{
    StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
};
pub use crate::column_values::u64_based::bitpacked::BitpackedCodec;
pub use crate::column_values::u64_based::blockwise_linear::BlockwiseLinearCodec;
pub use crate::column_values::u64_based::linear::LinearCodec;
pub use crate::column_values::u64_based::stats_collector::StatsCollector;
use crate::column_values::{monotonic_map_column, Stats};
use crate::iterable::Iterable;
use crate::{ColumnValues, MonotonicallyMappableToU64};

pub trait ColumnCodecEstimator<T = u64>: 'static {
    fn collect(&mut self, value: u64);
    fn estimate(&self, stats: &Stats) -> Option<u64>;
    fn finalize(&mut self) {}
    fn serialize(
        &self,
        stats: &Stats,
        vals: &mut dyn Iterator<Item = T>,
        wrt: &mut dyn io::Write,
    ) -> io::Result<()>;
}

pub trait ColumnCodec<T: PartialOrd = u64> {
    type Reader: ColumnValues<T> + 'static;
    type Estimator: ColumnCodecEstimator + Default;

    fn load(bytes: OwnedBytes) -> io::Result<Self::Reader>;

    fn estimator() -> Self::Estimator {
        Self::Estimator::default()
    }
    fn boxed_estimator() -> Box<dyn ColumnCodecEstimator> {
        Box::new(Self::estimator())
    }
}

/// Available codecs to use to encode the u64 (via [`MonotonicallyMappableToU64`]) converted data.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
pub enum CodecType {
    /// Bitpack all values in the value range. The number of bits is defined by the amplitude
    /// `column.max_value() - column.min_value()`
    Bitpacked = 0u8,
    /// Linear interpolation puts a line between the first and last value and then bitpacks the
    /// values by the offset from the line. The number of bits is defined by the max deviation from
    /// the line.
    Linear = 1u8,
    /// Same as [`CodecType::Linear`], but encodes in blocks of 512 elements.
    BlockwiseLinear = 2u8,
}

pub const ALL_U64_CODEC_TYPES: [CodecType; 3] = [
    CodecType::Bitpacked,
    CodecType::Linear,
    CodecType::BlockwiseLinear,
];

impl CodecType {
    fn to_code(self) -> u8 {
        self as u8
    }

    fn try_from_code(code: u8) -> Option<CodecType> {
        match code {
            0u8 => Some(CodecType::Bitpacked),
            1u8 => Some(CodecType::Linear),
            2u8 => Some(CodecType::BlockwiseLinear),
            _ => None,
        }
    }

    fn load<T: MonotonicallyMappableToU64>(
        &self,
        bytes: OwnedBytes,
    ) -> io::Result<Arc<dyn ColumnValues<T>>> {
        match self {
            CodecType::Bitpacked => load_specific_codec::<BitpackedCodec, T>(bytes),
            CodecType::Linear => load_specific_codec::<LinearCodec, T>(bytes),
            CodecType::BlockwiseLinear => load_specific_codec::<BlockwiseLinearCodec, T>(bytes),
        }
    }
}

fn load_specific_codec<C: ColumnCodec, T: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let reader = C::load(bytes)?;
    let reader_typed = monotonic_map_column(
        reader,
        StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<T>::new()),
    );
    Ok(Arc::new(reader_typed))
}

impl CodecType {
    pub fn estimator(&self) -> Box<dyn ColumnCodecEstimator> {
        match self {
            CodecType::Bitpacked => BitpackedCodec::boxed_estimator(),
            CodecType::Linear => LinearCodec::boxed_estimator(),
            CodecType::BlockwiseLinear => BlockwiseLinearCodec::boxed_estimator(),
        }
    }
}

pub fn serialize_u64_based_column_values<'a, T: MonotonicallyMappableToU64>(
    vals: &dyn Iterable<T>,
    codec_types: &[CodecType],
    wrt: &mut dyn Write,
) -> io::Result<()> {
    let mut stats_collector = StatsCollector::default();
    let mut estimators: Vec<(CodecType, Box<dyn ColumnCodecEstimator>)> =
        Vec::with_capacity(codec_types.len());
    for &codec_type in codec_types {
        estimators.push((codec_type, codec_type.estimator()));
    }
    for val in vals.boxed_iter() {
        let val_u64 = val.to_u64();
        stats_collector.collect(val_u64);
        for (_, estimator) in &mut estimators {
            estimator.collect(val_u64);
        }
    }
    for (_, estimator) in &mut estimators {
        estimator.finalize();
    }
    let stats = stats_collector.stats();
    let (_, best_codec, best_codec_estimator) = estimators
        .into_iter()
        .flat_map(|(codec_type, estimator)| {
            let num_bytes = estimator.estimate(&stats)?;
            Some((num_bytes, codec_type, estimator))
        })
        .min_by_key(|(num_bytes, _, _)| *num_bytes)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "No available applicable codec.")
        })?;
    best_codec.to_code().serialize(wrt)?;
    best_codec_estimator.serialize(
        &stats,
        &mut vals.boxed_iter().map(MonotonicallyMappableToU64::to_u64),
        wrt,
    )?;
    Ok(())
}

pub fn load_u64_based_column_values<T: MonotonicallyMappableToU64>(
    mut bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let codec_type: CodecType = bytes
        .get(0)
        .copied()
        .and_then(CodecType::try_from_code)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Failed to read codec type"))?;
    bytes.advance(1);
    codec_type.load(bytes)
}

/// Helper function to serialize a column (autodetect from all codecs) and then open it
pub fn serialize_and_load_u64_based_column_values<T: MonotonicallyMappableToU64>(
    vals: &dyn Iterable,
    codec_types: &[CodecType],
) -> Arc<dyn ColumnValues<T>> {
    let mut buffer = Vec::new();
    serialize_u64_based_column_values(vals, codec_types, &mut buffer).unwrap();
    load_u64_based_column_values::<T>(OwnedBytes::new(buffer)).unwrap()
}

#[cfg(test)]
mod tests;
