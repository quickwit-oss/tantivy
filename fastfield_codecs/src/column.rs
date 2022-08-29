use std::ops::Range;

pub trait Column<T = u64> {
    /// Return the value associated to the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_val(&self, idx: u64) -> T;

    /// Returns an iterator over given doc range.
    ///
    /// # Panics
    ///
    /// May panic if `range.end()` is greater than
    /// the segment's `maxdoc`.
    fn get_range(&self, range: Range<u64>) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(range.map(|idx| self.get_val(idx)))
    }

    /// Returns the minimum value for this fast field.
    ///
    /// The min value does not take in account of possible
    /// deleted document, and should be considered as a lower bound
    /// of the actual minimum value.
    fn min_value(&self) -> T;

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value
    fn max_value(&self) -> T;

    fn num_vals(&self) -> u64;
    /// Returns a iterator over the data
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = T> + 'a> {
        Box::new((0..self.num_vals()).map(|idx| self.get_val(idx)))
    }
}
