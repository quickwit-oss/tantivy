//! Radix selection for block kd-tree tree partitioning.
//!
//! Implements byte-wise histogram selection to find median values without comparisons, enabling
//! efficient partitioning of spatial data during block kd-tree construction. Processes values
//! through multiple passes, building histograms for each byte position after a common prefix,
//! avoiding the need to sort or compare elements directly.

/// Performs radix selection to find the median value without comparisons by building byte-wise
/// histograms.
pub struct RadixSelect {
    histogram: [usize; 256],
    prefix: Vec<u8>,
    offset: usize,
    nth: usize,
}

impl RadixSelect {
    /// Creates a new radix selector for finding the nth element among values with a common prefix.
    ///
    /// The offset specifies how many matching elements appeared in previous buckets (from earlier
    /// passes). The nth parameter is 0-indexed, so pass 31 to find the 32nd element (median of
    /// 64).
    pub fn new(prefix: Vec<u8>, offset: usize, nth: usize) -> Self {
        RadixSelect {
            histogram: [0; 256],
            prefix,
            offset,
            nth,
        }
    }
    /// Updates the histogram with a value if it matches the current prefix.
    ///
    /// Values that don't start with the prefix are ignored. For matching values, increments the
    /// count for the byte at position `prefix.len()`.
    pub fn update(&mut self, value: i32) {
        let bytes = value.to_be_bytes();
        if !bytes.starts_with(&self.prefix) {
            return;
        }
        let byte = bytes[self.prefix.len()];
        self.histogram[byte as usize] += 1;
    }
    /// Finds which bucket contains the nth element and returns the bucket value and offset.
    ///
    /// Returns a tuple of `(bucket_byte, count_before)` where bucket_byte is the value of the byte
    /// that contains the nth element, and count_before is the number of elements in earlier
    /// buckets (becomes the offset for the next pass).
    pub fn nth(&self) -> (u8, usize) {
        let mut count = self.offset;
        for (bucket, &frequency) in self.histogram.iter().enumerate() {
            if count + frequency > self.nth as usize {
                return (bucket as u8, count);
            }
            count += frequency;
        }
        panic!("nth element {} not found in histogram", self.nth);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radix_selection() {
        let dimensions = [
            (
                vec![
                    0x10101010, 0x10101011, 0x10101012, 0x10101013, 0x10101014, 0x10101015,
                    0x10101016, 0x10101017, 0x10101018, 0x10101019, 0x1010101A, 0x1010101B,
                    0x1010101C, 0x1010101D, 0x1010101E, 0x1010101F, 0x10101020, 0x10101021,
                    0x10101022, 0x10101023, 0x10101024, 0x10101025, 0x10101026, 0x10101027,
                    0x10101028, 0x10101029, 0x1010102A, 0x1010102B, 0x1010102C, 0x1010102D,
                    0x1010102E, 0x1010102F, 0x10101030, 0x10101031, 0x10101032, 0x10101033,
                    0x10101034, 0x10101035, 0x10101036, 0x10101037, 0x10101038, 0x10101039,
                    0x1010103A, 0x1010103B, 0x1010103C, 0x1010103D, 0x1010103E, 0x1010103F,
                    0x10101040, 0x10101041, 0x10101042, 0x10101043, 0x10101044, 0x10101045,
                    0x10101046, 0x10101047, 0x10101048, 0x10101049, 0x1010104A, 0x1010104B,
                    0x1010104C, 0x1010104D, 0x1010104E, 0x1010104F,
                ],
                [(0x10, 0), (0x10, 0), (0x10, 0), (0x2F, 31)],
            ),
            (
                vec![
                    0x10101010, 0x10101011, 0x10101012, 0x10101013, 0x10101014, 0x10101015,
                    0x10101016, 0x10101017, 0x10101018, 0x10101019, 0x1010101A, 0x1010101B,
                    0x1010101C, 0x1010101D, 0x1010101E, 0x1010101F, 0x10101020, 0x10101021,
                    0x10101022, 0x10101023, 0x10101024, 0x20101025, 0x20201026, 0x20301027,
                    0x20401028, 0x20501029, 0x2060102A, 0x2070102B, 0x2080102C, 0x2090102D,
                    0x20A0102E, 0x20B0102F, 0x20C01030, 0x20D01031, 0x20E01032, 0x20F01033,
                    0x20F11034, 0x20F21035, 0x20F31036, 0x20F41037, 0x20F51038, 0x20F61039,
                    0x3010103A, 0x3010103B, 0x3010103C, 0x3010103D, 0x3010103E, 0x3010103F,
                    0x30101040, 0x30101041, 0x30101042, 0x30101043, 0x30101044, 0x30101045,
                    0x30101046, 0x30101047, 0x30101048, 0x30101049, 0x3010104A, 0x3010104B,
                    0x3010104C, 0x3010104D, 0x3010104E, 0x3010104F,
                ],
                [(0x20, 21), (0xB0, 31), (0x10, 31), (0x2F, 31)],
            ),
        ];
        for (numbers, expected) in dimensions {
            let mut offset = 0;
            let mut prefix = Vec::new();
            for i in 0..4 {
                let mut radix_select = RadixSelect::new(prefix.clone(), offset, 31);
                for &number in &numbers {
                    radix_select.update(number);
                }
                let (byte, count) = radix_select.nth();
                if i != 3 {
                    assert_eq!(expected[i].0, byte);
                    assert_eq!(expected[i].1, count);
                }
                prefix.push(byte);
                offset = count;
            }
            let mut sorted = numbers.clone();
            sorted.sort();
            let radix_result = i32::from_be_bytes(prefix.as_slice().try_into().unwrap());
            assert_eq!(radix_result, sorted[31]);
        }
    }
}
