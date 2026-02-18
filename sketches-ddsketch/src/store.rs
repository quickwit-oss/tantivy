#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

const CHUNK_SIZE: i32 = 128;

// Divide the `dividend` by the `divisor`, rounding towards positive infinity.
//
// Similar to the nightly only `std::i32::div_ceil`.
fn div_ceil(dividend: i32, divisor: i32) -> i32 {
    (dividend + divisor - 1) / divisor
}

/// CollapsingLowestDenseStore
#[derive(Clone, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct Store {
    pub(crate) bins: Vec<u64>,
    pub(crate) count: u64,
    pub(crate) min_key: i32,
    pub(crate) max_key: i32,
    pub(crate) offset: i32,
    pub(crate) bin_limit: usize,
    is_collapsed: bool,
}

impl Store {
    pub fn new(bin_limit: usize) -> Self {
        Store {
            bins: Vec::new(),
            count: 0,
            min_key: i32::MAX,
            max_key: i32::MIN,
            offset: 0,
            bin_limit,
            is_collapsed: false,
        }
    }

    /// Return the number of bins.
    pub fn length(&self) -> i32 {
        self.bins.len() as i32
    }

    pub fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }

    pub fn add(&mut self, key: i32) {
        let idx = self.get_index(key);
        self.bins[idx] += 1;
        self.count += 1;
    }

    /// See Java: https://github.com/DataDog/sketches-java/blob/master/src/main/java/com/datadoghq/sketch/ddsketch/store/DenseStore.java  (add(int index, double count) method)
    pub(crate) fn add_count(&mut self, key: i32, count: u64) {
        let idx = self.get_index(key);
        self.bins[idx] += count;
        self.count += count;
    }

    fn get_index(&mut self, key: i32) -> usize {
        if key < self.min_key {
            if self.is_collapsed {
                return 0;
            }

            self.extend_range(key, None);
            if self.is_collapsed {
                return 0;
            }
        } else if key > self.max_key {
            self.extend_range(key, None);
        }

        (key - self.offset) as usize
    }

    fn extend_range(&mut self, key: i32, second_key: Option<i32>) {
        let second_key = second_key.unwrap_or(key);
        let new_min_key = i32::min(key, i32::min(second_key, self.min_key));
        let new_max_key = i32::max(key, i32::max(second_key, self.max_key));

        if self.is_empty() {
            let new_len = self.get_new_length(new_min_key, new_max_key);
            self.bins.resize(new_len, 0);
            self.offset = new_min_key;
            self.adjust(new_min_key, new_max_key);
        } else if new_min_key >= self.min_key && new_max_key < self.offset + self.length() {
            self.min_key = new_min_key;
            self.max_key = new_max_key;
        } else {
            // Grow bins
            let new_length = self.get_new_length(new_min_key, new_max_key);
            if new_length > self.length() as usize {
                self.bins.resize(new_length, 0);
            }
            self.adjust(new_min_key, new_max_key);
        }
    }

    fn get_new_length(&self, new_min_key: i32, new_max_key: i32) -> usize {
        let desired_length = new_max_key - new_min_key + 1;
        usize::min(
            (CHUNK_SIZE * div_ceil(desired_length, CHUNK_SIZE)) as usize,
            self.bin_limit,
        )
    }

    fn adjust(&mut self, new_min_key: i32, new_max_key: i32) {
        if new_max_key - new_min_key + 1 > self.length() {
            let new_min_key = new_max_key - self.length() + 1;

            if new_min_key >= self.max_key {
                // Put everything in the first bin.
                self.offset = new_min_key;
                self.min_key = new_min_key;
                self.bins.fill(0);
                self.bins[0] = self.count;
            } else {
                let shift = self.offset - new_min_key;
                if shift < 0 {
                    let collapse_start_index = (self.min_key - self.offset) as usize;
                    let collapse_end_index = (new_min_key - self.offset) as usize;
                    let collapsed_count: u64 = self.bins[collapse_start_index..collapse_end_index]
                        .iter()
                        .sum();
                    let zero_len = (new_min_key - self.min_key) as usize;
                    self.bins.splice(
                        collapse_start_index..collapse_end_index,
                        std::iter::repeat(0).take(zero_len),
                    );
                    self.bins[collapse_end_index] += collapsed_count;
                }
                self.min_key = new_min_key;
                self.shift_bins(shift);
            }

            self.max_key = new_max_key;
            self.is_collapsed = true;
        } else {
            self.center_bins(new_min_key, new_max_key);
            self.min_key = new_min_key;
            self.max_key = new_max_key;
        }
    }

    fn shift_bins(&mut self, shift: i32) {
        if shift > 0 {
            let shift = shift as usize;
            self.bins.rotate_right(shift);
            for idx in 0..shift {
                self.bins[idx] = 0;
            }
        } else {
            let shift = shift.abs() as usize;
            for idx in 0..shift {
                self.bins[idx] = 0;
            }
            self.bins.rotate_left(shift);
        }

        self.offset -= shift;
    }

    fn center_bins(&mut self, new_min_key: i32, new_max_key: i32) {
        let middle_key = new_min_key + (new_max_key - new_min_key + 1) / 2;
        let shift = self.offset + self.length() / 2 - middle_key;
        self.shift_bins(shift)
    }

    pub fn key_at_rank(&self, rank: u64) -> i32 {
        let mut n = 0;
        for (i, bin) in self.bins.iter().enumerate() {
            n += *bin;
            if n > rank {
                return i as i32 + self.offset;
            }
        }

        self.max_key
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn merge(&mut self, other: &Store) {
        if other.count == 0 {
            return;
        }

        if self.count == 0 {
            self.copy(other);
            return;
        }

        if other.min_key < self.min_key || other.max_key > self.max_key {
            self.extend_range(other.min_key, Some(other.max_key));
        }

        let collapse_start_index = other.min_key - other.offset;
        let mut collapse_end_index = i32::min(self.min_key, other.max_key + 1) - other.offset;
        if collapse_end_index > collapse_start_index {
            let collapsed_count: u64 = self.bins
                [collapse_start_index as usize..collapse_end_index as usize]
                .iter()
                .sum();
            self.bins[0] += collapsed_count;
        } else {
            collapse_end_index = collapse_start_index;
        }

        for key in (collapse_end_index + other.offset)..(other.max_key + 1) {
            self.bins[(key - self.offset) as usize] += other.bins[(key - other.offset) as usize]
        }

        self.count += other.count;
    }

    fn copy(&mut self, o: &Store) {
        self.bins = o.bins.clone();
        self.count = o.count;
        self.min_key = o.min_key;
        self.max_key = o.max_key;
        self.offset = o.offset;
        self.bin_limit = o.bin_limit;
        self.is_collapsed = o.is_collapsed;
    }
}

#[cfg(test)]
mod tests {
    use crate::store::Store;

    #[test]
    fn test_simple_store() {
        let mut s = Store::new(2048);

        for i in 0..2048 {
            s.add(i);
        }
    }

    #[test]
    fn test_simple_store_rev() {
        let mut s = Store::new(2048);

        for i in 2048..0 {
            s.add(i);
        }
    }
}
