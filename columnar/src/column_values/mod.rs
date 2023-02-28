#![warn(missing_docs)]

//! # `fastfield_codecs`
//!
//! - Columnar storage of data for tantivy [`Column`].
//! - Encode data in different codecs.
//! - Monotonically map values to u64/u128

use std::fmt::Debug;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

pub use monotonic_mapping::{MonotonicallyMappableToU64, StrictlyMonotonicFn};
pub use monotonic_mapping_u128::MonotonicallyMappableToU128;

mod merge;
pub(crate) mod monotonic_mapping;
pub(crate) mod monotonic_mapping_u128;
mod stats;
mod u128_based;
mod u64_based;
mod vec_column;

mod monotonic_column;

pub(crate) use merge::MergedColumnValues;
pub use stats::ColumnStats;
pub use u128_based::{open_u128_mapped, serialize_column_values_u128};
pub use u64_based::{
    load_u64_based_column_values, serialize_and_load_u64_based_column_values,
    serialize_u64_based_column_values, CodecType, ALL_U64_CODEC_TYPES,
};
pub use vec_column::VecColumn;

pub use self::monotonic_column::monotonic_map_column;
use crate::RowId;

/// `ColumnValues` provides access to a dense field column.
///
/// `Column` are just a wrapper over `ColumnValues` and a `ColumnIndex`.
///
/// Any methods with a default and specialized implementation need to be called in the
/// wrappers that implement the trait: Arc and MonotonicMappingColumn
pub trait ColumnValues<T: PartialOrd = u64>: Send + Sync {
    /// Return the value associated with the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_val(&self, idx: u32) -> T;

    /// Allows to push down multiple fetch calls, to avoid dynamic dispatch overhead.
    ///
    /// idx and output should have the same length
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_vals(&self, idx: &[u32], output: &mut [T]) {
        assert!(idx.len() == output.len());
        for (out, idx) in output.iter_mut().zip(idx.iter()) {
            *out = self.get_val(*idx as u32);
        }
    }

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// # Panics
    ///
    /// Must panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    #[inline(always)]
    fn get_range(&self, start: u64, output: &mut [T]) {
        for (out, idx) in output.iter_mut().zip(start..) {
            *out = self.get_val(idx as u32);
        }
    }

    /// Get the row ids of values which are in the provided value range.
    ///
    /// Note that position == docid for single value fast fields
    #[inline(always)]
    fn get_row_ids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        row_id_range: Range<RowId>,
        row_id_hits: &mut Vec<RowId>,
    ) {
        let row_id_range = row_id_range.start..row_id_range.end.min(self.num_vals());
        for idx in row_id_range.start..row_id_range.end {
            let val = self.get_val(idx);
            if value_range.contains(&val) {
                row_id_hits.push(idx);
            }
        }
    }

    /// Returns the minimum value for this fast field.
    ///
    /// This min_value may not be exact.
    /// For instance, the min value does not take in account of possible
    /// deleted document. All values are however guaranteed to be higher than
    /// `.min_value()`.
    fn min_value(&self) -> T;

    /// Returns the maximum value for this fast field.
    ///
    /// This max_value may not be exact.
    /// For instance, the max value does not take in account of possible
    /// deleted document. All values are however guaranteed to be higher than
    /// `.max_value()`.
    fn max_value(&self) -> T;

    /// The number of values in the column.
    fn num_vals(&self) -> u32;

    /// Returns a iterator over the data
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = T> + 'a> {
        Box::new((0..self.num_vals()).map(|idx| self.get_val(idx)))
    }
}

impl<T: Copy + PartialOrd + Debug> ColumnValues<T> for Arc<dyn ColumnValues<T>> {
    #[inline(always)]
    fn get_val(&self, idx: u32) -> T {
        self.as_ref().get_val(idx)
    }

    #[inline(always)]
    fn min_value(&self) -> T {
        self.as_ref().min_value()
    }

    #[inline(always)]
    fn max_value(&self) -> T {
        self.as_ref().max_value()
    }

    #[inline(always)]
    fn num_vals(&self) -> u32 {
        self.as_ref().num_vals()
    }

    #[inline(always)]
    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = T> + 'b> {
        self.as_ref().iter()
    }

    #[inline(always)]
    fn get_range(&self, start: u64, output: &mut [T]) {
        self.as_ref().get_range(start, output)
    }

    #[inline(always)]
    fn get_row_ids_for_value_range(
        &self,
        range: RangeInclusive<T>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        self.as_ref()
            .get_row_ids_for_value_range(range, doc_id_range, positions)
    }
}

/// Wraps an cloneable iterator into a `Column`.
pub struct IterColumn<T>(T);

impl<T> From<T> for IterColumn<T>
where T: Iterator + Clone + ExactSizeIterator
{
    fn from(iter: T) -> Self {
        IterColumn(iter)
    }
}

impl<T> ColumnValues<T::Item> for IterColumn<T>
where
    T: Iterator + Clone + ExactSizeIterator + Send + Sync,
    T::Item: PartialOrd + Debug,
{
    fn get_val(&self, idx: u32) -> T::Item {
        self.0.clone().nth(idx as usize).unwrap()
    }

    fn min_value(&self) -> T::Item {
        self.0.clone().next().unwrap()
    }

    fn max_value(&self) -> T::Item {
        self.0.clone().last().unwrap()
    }

    fn num_vals(&self) -> u32 {
        self.0.len() as u32
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T::Item> + '_> {
        Box::new(self.0.clone())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_as_col() {
        let col = IterColumn::from(10..100);
        assert_eq!(col.num_vals(), 90);
        assert_eq!(col.max_value(), 99);
    }
}
