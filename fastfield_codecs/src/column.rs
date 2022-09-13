use std::marker::PhantomData;
use std::sync::Mutex;

use tantivy_bitpacker::minmax;

pub trait Column<T = u64>: Send + Sync {
    /// Return the value associated to the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_val(&self, idx: u64) -> T;

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// Regardless of the type of `Item`, this method works
    /// - transmuting the output array
    /// - extracting the `Item`s as if they were `u64`
    /// - possibly converting the `u64` value to the right type.
    ///
    /// # Panics
    ///
    /// May panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    fn get_range(&self, start: u64, output: &mut [T]) {
        for (out, idx) in output.iter_mut().zip(start..) {
            *out = self.get_val(idx);
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

    fn num_vals(&self) -> u64;

    /// Returns a iterator over the data
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = T> + 'a> {
        Box::new((0..self.num_vals()).map(|idx| self.get_val(idx)))
    }
}

pub struct VecColumn<'a, T = u64> {
    values: &'a [T],
    min_value: T,
    max_value: T,
}

impl<'a, C: Column<T>, T: Copy + PartialOrd> Column<T> for &'a C {
    fn get_val(&self, idx: u64) -> T {
        (*self).get_val(idx)
    }

    fn min_value(&self) -> T {
        (*self).min_value()
    }

    fn max_value(&self) -> T {
        (*self).max_value()
    }

    fn num_vals(&self) -> u64 {
        (*self).num_vals()
    }
}

impl<'a, T: Copy + PartialOrd + Send + Sync> Column<T> for VecColumn<'a, T> {
    fn get_val(&self, position: u64) -> T {
        self.values[position as usize]
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = T> + 'b> {
        Box::new(self.values.iter().copied())
    }

    fn min_value(&self) -> T {
        self.min_value
    }

    fn max_value(&self) -> T {
        self.max_value
    }

    fn num_vals(&self) -> u64 {
        self.values.len() as u64
    }
}

impl<'a, T: Copy + Ord + Default, V> From<&'a V> for VecColumn<'a, T>
where V: AsRef<[T]> + ?Sized
{
    fn from(values: &'a V) -> Self {
        let values = values.as_ref();
        let (min_value, max_value) = minmax(values.iter().copied()).unwrap_or_default();
        Self {
            values,
            min_value,
            max_value,
        }
    }
}

struct MonotonicMappingColumn<C, T, Input> {
    from_column: C,
    monotonic_mapping: T,
    _phantom: PhantomData<Input>,
}

/// Creates a view of a column transformed by a monotonic mapping.
pub fn monotonic_map_column<C, T, Input, Output>(
    from_column: C,
    monotonic_mapping: T,
) -> impl Column<Output>
where
    C: Column<Input>,
    T: Fn(Input) -> Output + Send + Sync,
    Input: Send + Sync,
    Output: Send + Sync,
{
    MonotonicMappingColumn {
        from_column,
        monotonic_mapping,
        _phantom: PhantomData,
    }
}

impl<C, T, Input, Output> Column<Output> for MonotonicMappingColumn<C, T, Input>
where
    C: Column<Input>,
    T: Fn(Input) -> Output + Send + Sync,
    Input: Send + Sync,
    Output: Send + Sync,
{
    fn get_val(&self, idx: u64) -> Output {
        let from_val = self.from_column.get_val(idx);
        (self.monotonic_mapping)(from_val)
    }

    fn min_value(&self) -> Output {
        let from_min_value = self.from_column.min_value();
        (self.monotonic_mapping)(from_min_value)
    }

    fn max_value(&self) -> Output {
        let from_max_value = self.from_column.max_value();
        (self.monotonic_mapping)(from_max_value)
    }

    fn num_vals(&self) -> u64 {
        self.from_column.num_vals()
    }
}

pub struct RemappedColumn<T, M, C> {
    column: C,
    new_to_old_id_mapping: M,
    min_max_cache: Mutex<Option<(T, T)>>,
}

impl<T, M, C> RemappedColumn<T, M, C>
where
    C: Column<T>,
    M: Column<u32>,
    T: Copy + Ord + Default + Send + Sync,
{
    fn min_max(&self) -> (T, T) {
        if let Some((min, max)) = *self.min_max_cache.lock().unwrap() {
            return (min, max);
        }
        let (min, max) =
            tantivy_bitpacker::minmax(self.iter()).unwrap_or((T::default(), T::default()));
        *self.min_max_cache.lock().unwrap() = Some((min, max));
        (min, max)
    }
}

pub struct IterColumn<T>(T);

impl<T> From<T> for IterColumn<T>
where T: Iterator + Clone + ExactSizeIterator
{
    fn from(iter: T) -> Self {
        IterColumn(iter)
    }
}

impl<T> Column<T::Item> for IterColumn<T>
where T: Iterator + Clone + ExactSizeIterator + Send + Sync
{
    fn get_val(&self, idx: u64) -> T::Item {
        self.0.clone().nth(idx as usize).unwrap()
    }

    fn min_value(&self) -> T::Item {
        self.0.clone().next().unwrap()
    }

    fn max_value(&self) -> T::Item {
        self.0.clone().last().unwrap()
    }

    fn num_vals(&self) -> u64 {
        self.0.len() as u64
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = T::Item> + 'a> {
        Box::new(self.0.clone())
    }
}

impl<T, M, C> Column<T> for RemappedColumn<T, M, C>
where
    C: Column<T>,
    M: Column<u32>,
    T: Copy + Ord + Default + Send + Sync,
{
    fn get_val(&self, idx: u64) -> T {
        let old_id = self.new_to_old_id_mapping.get_val(idx);
        self.column.get_val(old_id as u64)
    }

    fn min_value(&self) -> T {
        self.min_max().0
    }

    fn max_value(&self) -> T {
        self.min_max().1
    }

    fn num_vals(&self) -> u64 {
        self.new_to_old_id_mapping.num_vals() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_mapping() {
        let vals = &[1u64, 3u64][..];
        let col = VecColumn::from(vals);
        let mapped = monotonic_map_column(col, |el| el + 4);
        assert_eq!(mapped.min_value(), 5u64);
        assert_eq!(mapped.max_value(), 7u64);
        assert_eq!(mapped.num_vals(), 2);
        assert_eq!(mapped.num_vals(), 2);
        assert_eq!(mapped.get_val(0), 5);
        assert_eq!(mapped.get_val(1), 7);
    }

    #[test]
    fn test_range_as_col() {
        let col = IterColumn::from(10..100);
        assert_eq!(col.num_vals(), 90);
        assert_eq!(col.max_value(), 99);
    }
}
