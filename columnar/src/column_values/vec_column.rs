use std::fmt::Debug;

use tantivy_bitpacker::minmax;

use crate::ColumnValues;

/// VecColumn provides `Column` over a slice.
pub struct VecColumn<'a, T = u64> {
    pub(crate) values: &'a [T],
    pub(crate) min_value: T,
    pub(crate) max_value: T,
}

impl<'a, T: Copy + PartialOrd + Send + Sync + Debug> ColumnValues<T> for VecColumn<'a, T> {
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
