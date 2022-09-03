use std::marker::PhantomData;

use tantivy_bitpacker::minmax;

pub trait Column<T = u64> {
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

pub struct VecColumn<'a, T = u64> {
    values: &'a [T],
    min_value: T,
    max_value: T,
}

impl<'a, T: Copy + PartialOrd> Column<T> for VecColumn<'a, T> {
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
    T: Fn(Input) -> Output,
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
    T: Fn(Input) -> Output,
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
}
