use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use crate::Column;

/// `OptionalColumn` provides columnar access on a field.
pub trait OptionalColumn<T: PartialOrd = u64>: Send + Sync {
    /// Return the value associated with the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_val(&self, idx: u32) -> Option<T>;

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// # Panics
    ///
    /// Must panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.

    fn get_range(&self, start: u64, output: &mut [Option<T>]) {
        for (out, idx) in output.iter_mut().zip(start..) {
            *out = self.get_val(idx as u32);
        }
    }

    /// Return the positions of values which are in the provided range.
    fn get_docids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        let doc_id_range = doc_id_range.start..doc_id_range.end.min(self.num_vals());

        for idx in doc_id_range.start..doc_id_range.end {
            let val = self.get_val(idx);
            if let Some(val) = val {
                if value_range.contains(&val) {
                    positions.push(idx);
                }
            }
        }
    }

    /// Returns the minimum value for this fast field.
    ///
    /// This min_value may not be exact.
    /// For instance, the min value does not take in account of possible
    /// deleted document. All values are however guaranteed to be higher than
    /// `.min_value()`.
    fn min_value(&self) -> Option<T>;

    /// Returns the maximum value for this fast field.
    ///
    /// This max_value may not be exact.
    /// For instance, the max value does not take in account of possible
    /// deleted document. All values are however guaranteed to be higher than
    /// `.max_value()`.
    fn max_value(&self) -> Option<T>;

    /// The number of values in the column.
    fn num_vals(&self) -> u32;

    /// Returns a iterator over the data
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Option<T>> + 'a> {
        Box::new((0..self.num_vals()).map(|idx| self.get_val(idx)))
    }

    /// return full column if all values are set and is not empty
    fn to_full(&self) -> Option<Arc<dyn Column<T>>> {
        None
    }
}

/// Temporary wrapper to migrate to optional column
pub(crate) struct ToOptionalColumn<T> {
    column: Arc<dyn Column<T>>,
}

impl<T: PartialOrd> ToOptionalColumn<T> {
    pub(crate) fn new(column: Arc<dyn Column<T>>) -> Self {
        Self { column }
    }
}

impl<T: PartialOrd> OptionalColumn<T> for ToOptionalColumn<T> {
    #[inline]
    fn get_val(&self, idx: u32) -> Option<T> {
        let val = self.column.get_val(idx);
        Some(val)
    }

    fn min_value(&self) -> Option<T> {
        let min_value = self.column.min_value();
        Some(min_value)
    }

    fn max_value(&self) -> Option<T> {
        let max_value = self.column.max_value();
        Some(max_value)
    }

    fn num_vals(&self) -> u32 {
        self.column.num_vals()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Option<T>> + '_> {
        Box::new(self.column.iter().map(|el| Some(el)))
    }
    /// return full column if all values are set and is not empty
    fn to_full(&self) -> Option<Arc<dyn Column<T>>> {
        Some(self.column.clone())
    }
}
