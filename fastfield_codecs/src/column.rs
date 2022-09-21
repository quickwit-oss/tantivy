use std::marker::PhantomData;
use std::ops::RangeInclusive;

use tantivy_bitpacker::minmax;

pub trait Column<T: PartialOrd + Copy + 'static = u64>: Send + Sync {
    /// Return a `ColumnReader`.
    fn reader(&self) -> Box<dyn ColumnReader<T> + '_> {
        // Box::new(ColumnReaderAdapter { column: self, idx: 0,  })
        Box::new(ColumnReaderAdapter::from(self))
    }

    /// Return the value associated to the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    ///
    /// TODO remove to force people to use `.reader()`.
    fn get_val(&self, idx: u64) -> T;

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// # Panics
    ///
    /// Must panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    #[inline]
    fn get_range(&self, start: u64, output: &mut [T]) {
        for (out, idx) in output.iter_mut().zip(start..) {
            *out = self.get_val(idx);
        }
    }

    /// Return the positions of values which are in the provided range.
    #[inline]
    fn get_between_vals(&self, range: RangeInclusive<T>) -> Vec<u64> {
        let mut vals = Vec::new();
        for idx in 0..self.num_vals() {
            let val = self.get_val(idx);
            if range.contains(&val) {
                vals.push(idx);
            }
        }
        vals
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
}

/// `ColumnReader` makes it possible to read forward through a column.
pub trait ColumnReader<T = u64> {
    /// Advance the reader to the target_idx.
    ///
    /// After a successful call to seek,
    /// `.get()` should returns `column.get_val(target_idx)`.
    fn seek(&mut self, target_idx: u64) -> T;

    fn advance(&mut self) -> bool;

    /// Get the current value without advancing the reader
    fn get(&self) -> T;
}

pub fn iter_from_reader<'a, T: 'static>(
    mut column_reader: Box<dyn ColumnReader<T> + 'a>,
) -> impl Iterator<Item = T> + 'a {
    std::iter::from_fn(move || {
        if !column_reader.advance() {
            return None;
        }
        Some(column_reader.get())
    })
}

pub(crate) struct ColumnReaderAdapter<'a, C: ?Sized, T> {
    column: &'a C,
    idx: u64,
    len: u64,
    _phantom: PhantomData<T>,
}

impl<'a, C: Column<T> + ?Sized, T: Copy + PartialOrd + 'static> From<&'a C>
    for ColumnReaderAdapter<'a, C, T>
{
    fn from(column: &'a C) -> Self {
        ColumnReaderAdapter {
            column,
            idx: u64::MAX,
            len: column.num_vals(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, T, C: ?Sized> ColumnReader<T> for ColumnReaderAdapter<'a, C, T>
where
    C: Column<T>,
    T: PartialOrd<T> + Copy + 'static,
{
    fn seek(&mut self, idx: u64) -> T {
        self.idx = idx;
        self.get()
    }

    fn advance(&mut self) -> bool {
        self.idx = self.idx.wrapping_add(1);
        self.idx < self.len
    }

    fn get(&self) -> T {
        self.column.get_val(self.idx)
    }
}

pub struct VecColumn<'a, T = u64> {
    values: &'a [T],
    min_value: T,
    max_value: T,
}

impl<'a, C: Column<T>, T> Column<T> for &'a C
where T: Copy + PartialOrd + 'static
{
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

    fn reader(&self) -> Box<dyn ColumnReader<T> + '_> {
        (*self).reader()
    }

    fn get_range(&self, start: u64, output: &mut [T]) {
        (*self).get_range(start, output)
    }
}

impl<'a, T: Copy + PartialOrd + Send + Sync + 'static> Column<T> for VecColumn<'a, T> {
    fn get_val(&self, position: u64) -> T {
        self.values[position as usize]
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

    fn get_range(&self, start: u64, output: &mut [T]) {
        output.copy_from_slice(&self.values[start as usize..][..output.len()])
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
pub fn monotonic_map_column<C, T, Input: PartialOrd + Copy, Output: PartialOrd + Copy>(
    from_column: C,
    monotonic_mapping: T,
) -> impl Column<Output>
where
    C: Column<Input>,
    T: Fn(Input) -> Output + Send + Sync,
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    MonotonicMappingColumn {
        from_column,
        monotonic_mapping,
        _phantom: PhantomData,
    }
}

impl<C, T, Input: PartialOrd + Copy, Output: PartialOrd + Copy> Column<Output>
    for MonotonicMappingColumn<C, T, Input>
where
    C: Column<Input>,
    T: Fn(Input) -> Output + Send + Sync,
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    #[inline]
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

    fn reader(&self) -> Box<dyn ColumnReader<Output> + '_> {
        Box::new(MonotonicMappingColumnReader {
            col_reader: self.from_column.reader(),
            monotonic_mapping: &self.monotonic_mapping,
            intermdiary_type: PhantomData,
        })
    }

    // We voluntarily do not implement get_range as it yields a regression,
    // and we do not have any specialized implementation anyway.
}

struct MonotonicMappingColumnReader<'a, Transform, U> {
    col_reader: Box<dyn ColumnReader<U> + 'a>,
    monotonic_mapping: &'a Transform,
    intermdiary_type: PhantomData<U>,
}

impl<'a, U, V, Transform> ColumnReader<V> for MonotonicMappingColumnReader<'a, Transform, U>
where
    U: Copy,
    V: Copy,
    Transform: Fn(U) -> V,
{
    fn seek(&mut self, idx: u64) -> V {
        let intermediary_value = self.col_reader.seek(idx);
        (*self.monotonic_mapping)(intermediary_value)
    }

    fn advance(&mut self) -> bool {
        self.col_reader.advance()
    }

    fn get(&self) -> V {
        (*self.monotonic_mapping)(self.col_reader.get())
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
where
    T: Iterator + Clone + ExactSizeIterator + Send + Sync,
    T::Item: PartialOrd + Copy + 'static,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MonotonicallyMappableToU64;

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

    #[test]
    fn test_monotonic_mapping_iter() {
        let vals: Vec<u64> = (-1..99).map(i64::to_u64).collect();
        let col = VecColumn::from(&vals);
        let mapped = monotonic_map_column(col, |el| i64::from_u64(el) * 10i64);
        let val_i64s: Vec<i64> = crate::iter_from_reader(mapped.reader()).collect();
        for i in 0..100 {
            assert_eq!(val_i64s[i as usize], mapped.get_val(i));
        }
    }

    #[test]
    fn test_monotonic_mapping_get_range() {
        let vals: Vec<u64> = (-1..99).map(i64::to_u64).collect();
        let col = VecColumn::from(&vals);
        let mapped = monotonic_map_column(col, |el| i64::from_u64(el) * 10i64);
        assert_eq!(mapped.min_value(), -10i64);
        assert_eq!(mapped.max_value(), 980i64);
        assert_eq!(mapped.num_vals(), 100);
        let val_i64s: Vec<i64> = crate::iter_from_reader(mapped.reader()).collect();
        assert_eq!(val_i64s.len(), 100);
        for i in 0..100 {
            assert_eq!(val_i64s[i as usize], mapped.get_val(i));
            assert_eq!(val_i64s[i as usize], i64::from_u64(vals[i as usize]) * 10);
        }
        let mut buf = [0i64; 20];
        mapped.get_range(7, &mut buf[..]);
        assert_eq!(&val_i64s[7..][..20], &buf);
    }
}
