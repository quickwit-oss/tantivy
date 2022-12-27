use std::marker::PhantomData;
use std::ops::{Range, RangeInclusive};

use tantivy_bitpacker::minmax;

use crate::monotonic_mapping::StrictlyMonotonicFn;

/// `Column` provides columnar access on a field.
pub trait Column<T: PartialOrd = u64>: Send + Sync {
    /// Return the value associated with the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_val(&self, idx: u32) -> T;

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
            *out = self.get_val(idx as u32);
        }
    }

    /// Get the positions of values which are in the provided value range.
    ///
    /// Note that position == docid for single value fast fields
    #[inline]
    fn get_docids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        let doc_id_range = doc_id_range.start..doc_id_range.end.min(self.num_vals());
        for idx in doc_id_range {
            let val = self.get_val(idx);
            if value_range.contains(&val) {
                positions.push(idx);
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

/// VecColumn provides `Column` over a slice.
pub struct VecColumn<'a, T = u64> {
    values: &'a [T],
    min_value: T,
    max_value: T,
}

impl<'a, C: Column<T>, T: Copy + PartialOrd> Column<T> for &'a C {
    fn get_val(&self, idx: u32) -> T {
        (*self).get_val(idx)
    }

    fn min_value(&self) -> T {
        (*self).min_value()
    }

    fn max_value(&self) -> T {
        (*self).max_value()
    }

    fn num_vals(&self) -> u32 {
        (*self).num_vals()
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = T> + 'b> {
        (*self).iter()
    }

    fn get_range(&self, start: u64, output: &mut [T]) {
        (*self).get_range(start, output)
    }
}

impl<'a, T: Copy + PartialOrd + Send + Sync> Column<T> for VecColumn<'a, T> {
    fn get_val(&self, position: u32) -> T {
        self.values[position as usize]
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(self.values.iter().copied())
    }

    fn min_value(&self) -> T {
        self.min_value
    }

    fn max_value(&self) -> T {
        self.max_value
    }

    fn num_vals(&self) -> u32 {
        self.values.len() as u32
    }

    fn get_range(&self, start: u64, output: &mut [T]) {
        output.copy_from_slice(&self.values[start as usize..][..output.len()])
    }
}

impl<'a, T: Copy + PartialOrd + Default, V> From<&'a V> for VecColumn<'a, T>
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

/// Creates a view of a column transformed by a strictly monotonic mapping. See
/// [`StrictlyMonotonicFn`].
///
/// E.g. apply a gcd monotonic_mapping([100, 200, 300]) == [1, 2, 3]
/// monotonic_mapping.mapping() is expected to be injective, and we should always have
/// monotonic_mapping.inverse(monotonic_mapping.mapping(el)) == el
///
/// The inverse of the mapping is required for:
/// `fn get_positions_for_value_range(&self, range: RangeInclusive<T>) -> Vec<u64> `
/// The user provides the original value range and we need to monotonic map them in the same way the
/// serialization does before calling the underlying column.
///
/// Note that when opening a codec, the monotonic_mapping should be the inverse of the mapping
/// during serialization. And therefore the monotonic_mapping_inv when opening is the same as
/// monotonic_mapping during serialization.
pub fn monotonic_map_column<C, T, Input, Output>(
    from_column: C,
    monotonic_mapping: T,
) -> impl Column<Output>
where
    C: Column<Input>,
    T: StrictlyMonotonicFn<Input, Output> + Send + Sync,
    Input: PartialOrd + Send + Sync + Clone,
    Output: PartialOrd + Send + Sync + Clone,
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
    T: StrictlyMonotonicFn<Input, Output> + Send + Sync,
    Input: PartialOrd + Send + Sync + Clone,
    Output: PartialOrd + Send + Sync + Clone,
{
    #[inline]
    fn get_val(&self, idx: u32) -> Output {
        let from_val = self.from_column.get_val(idx);
        self.monotonic_mapping.mapping(from_val)
    }

    fn min_value(&self) -> Output {
        let from_min_value = self.from_column.min_value();
        self.monotonic_mapping.mapping(from_min_value)
    }

    fn max_value(&self) -> Output {
        let from_max_value = self.from_column.max_value();
        self.monotonic_mapping.mapping(from_max_value)
    }

    fn num_vals(&self) -> u32 {
        self.from_column.num_vals()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Output> + '_> {
        Box::new(
            self.from_column
                .iter()
                .map(|el| self.monotonic_mapping.mapping(el)),
        )
    }

    fn get_docids_for_value_range(
        &self,
        range: RangeInclusive<Output>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        self.from_column.get_docids_for_value_range(
            self.monotonic_mapping.inverse(range.start().clone())
                ..=self.monotonic_mapping.inverse(range.end().clone()),
            doc_id_range,
            positions,
        )
    }

    // We voluntarily do not implement get_range as it yields a regression,
    // and we do not have any specialized implementation anyway.
}

/// Wraps an iterator into a `Column`.
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
    T::Item: PartialOrd,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monotonic_mapping::{
        StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternalBaseval,
        StrictlyMonotonicMappingToInternalGCDBaseval,
    };

    #[test]
    fn test_monotonic_mapping() {
        let vals = &[3u64, 5u64][..];
        let col = VecColumn::from(vals);
        let mapped = monotonic_map_column(col, StrictlyMonotonicMappingToInternalBaseval::new(2));
        assert_eq!(mapped.min_value(), 1u64);
        assert_eq!(mapped.max_value(), 3u64);
        assert_eq!(mapped.num_vals(), 2);
        assert_eq!(mapped.num_vals(), 2);
        assert_eq!(mapped.get_val(0), 1);
        assert_eq!(mapped.get_val(1), 3);
    }

    #[test]
    fn test_range_as_col() {
        let col = IterColumn::from(10..100);
        assert_eq!(col.num_vals(), 90);
        assert_eq!(col.max_value(), 99);
    }

    #[test]
    fn test_monotonic_mapping_iter() {
        let vals: Vec<u64> = (10..110u64).map(|el| el * 10).collect();
        let col = VecColumn::from(&vals);
        let mapped = monotonic_map_column(
            col,
            StrictlyMonotonicMappingInverter::from(
                StrictlyMonotonicMappingToInternalGCDBaseval::new(10, 100),
            ),
        );
        let val_i64s: Vec<u64> = mapped.iter().collect();
        for i in 0..100 {
            assert_eq!(val_i64s[i as usize], mapped.get_val(i));
        }
    }

    #[test]
    fn test_monotonic_mapping_get_range() {
        let vals: Vec<u64> = (0..100u64).map(|el| el * 10).collect();
        let col = VecColumn::from(&vals);
        let mapped = monotonic_map_column(
            col,
            StrictlyMonotonicMappingInverter::from(
                StrictlyMonotonicMappingToInternalGCDBaseval::new(10, 0),
            ),
        );

        assert_eq!(mapped.min_value(), 0u64);
        assert_eq!(mapped.max_value(), 9900u64);
        assert_eq!(mapped.num_vals(), 100);
        let val_u64s: Vec<u64> = mapped.iter().collect();
        assert_eq!(val_u64s.len(), 100);
        for i in 0..100 {
            assert_eq!(val_u64s[i as usize], mapped.get_val(i));
            assert_eq!(val_u64s[i as usize], vals[i as usize] * 10);
        }
        let mut buf = [0u64; 20];
        mapped.get_range(7, &mut buf[..]);
        assert_eq!(&val_u64s[7..][..20], &buf);
    }
}
