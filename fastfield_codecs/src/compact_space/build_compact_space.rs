use std::collections::{BTreeSet, BinaryHeap};
use std::ops::RangeInclusive;

use itertools::Itertools;

use super::blank_range::BlankRange;
use super::CompactSpace;

/// Put the blanks for the sorted values into a binary heap
fn get_blanks(values_sorted: &BTreeSet<u128>) -> BinaryHeap<BlankRange> {
    let mut blanks: BinaryHeap<BlankRange> = BinaryHeap::new();
    let mut add_range = |blank_range: RangeInclusive<u128>| {
        let blank_range: Result<BlankRange, _> = blank_range.try_into();
        if let Ok(blank_range) = blank_range {
            blanks.push(blank_range);
        }
    };
    for (first, second) in values_sorted.iter().tuple_windows() {
        // Correctness Overflow: the values are deduped and sorted (BTreeSet property), that means
        // there's always space between two values.
        let blank_range = first + 1..=second - 1;
        add_range(blank_range);
    }

    // Replace after stabilization of https://github.com/rust-lang/rust/issues/62924
    // Add preceeding range if values don't start at 0
    if let Some(first_val) = values_sorted.iter().next() {
        if *first_val != 0 {
            let blank_range = 0..=first_val - 1;
            add_range(blank_range);
        }
    }

    // Add succeeding range if values don't end at u128::MAX
    if let Some(last_val) = values_sorted.iter().last() {
        if *last_val != u128::MAX {
            let blank_range = last_val + 1..=u128::MAX;
            add_range(blank_range);
        }
    }
    blanks
}

struct BlankCollector {
    blanks: Vec<BlankRange>,
    staged_blanks_sum: u128,
}
impl BlankCollector {
    fn new() -> Self {
        Self {
            blanks: vec![],
            staged_blanks_sum: 0,
        }
    }
    fn stage_blank(&mut self, blank: BlankRange) {
        self.staged_blanks_sum += blank.blank_size();
        self.blanks.push(blank);
    }
    fn drain(&mut self) -> impl Iterator<Item = BlankRange> + '_ {
        self.staged_blanks_sum = 0;
        self.blanks.drain(..)
    }
    fn staged_blanks_sum(&self) -> u128 {
        self.staged_blanks_sum
    }
    fn num_blanks(&self) -> usize {
        self.blanks.len()
    }
}
fn num_bits(val: u128) -> u8 {
    (128u32 - val.leading_zeros()) as u8
}

/// Will collect blanks and add them to compact space if more bits are saved than cost from
/// metadata.
pub fn get_compact_space(
    values_deduped_sorted: &BTreeSet<u128>,
    total_num_values: usize,
    cost_per_blank: usize,
) -> CompactSpace {
    let mut blanks: BinaryHeap<BlankRange> = get_blanks(values_deduped_sorted);
    let mut amplitude_compact_space = u128::MAX;
    let mut amplitude_bits: u8 = num_bits(amplitude_compact_space);

    let mut compact_space = CompactSpaceBuilder::new();
    if values_deduped_sorted.is_empty() {
        return compact_space.finish();
    }

    let mut blank_collector = BlankCollector::new();
    // We will stage blanks until they reduce the compact space by 1 bit.
    // Binary heap to process the gaps by their size
    while let Some(blank_range) = blanks.pop() {
        blank_collector.stage_blank(blank_range);

        let staged_spaces_sum: u128 = blank_collector.staged_blanks_sum();
        // +1 for later added null value
        let amplitude_new_compact_space = amplitude_compact_space - staged_spaces_sum + 1;
        let amplitude_new_bits = num_bits(amplitude_new_compact_space);
        if amplitude_bits == amplitude_new_bits {
            continue;
        }
        let saved_bits = (amplitude_bits - amplitude_new_bits) as usize * total_num_values;
        // TODO: Maybe calculate exact cost of blanks and run this more expensive computation only,
        // when amplitude_new_bits changes
        let cost = blank_collector.num_blanks() * cost_per_blank;
        if cost >= saved_bits {
            // Continue here, since although we walk over the blanks by size,
            // we can potentially save a lot at the last bits, which are smaller blanks
            //
            // E.g. if the first range reduces the compact space by 1000 from 2000 to 1000, which
            // saves 11-10=1 bit and the next range reduces the compact space by 950 to
            // 50, which saves 10-6=4 bit
            continue;
        }

        amplitude_compact_space = amplitude_new_compact_space;
        amplitude_bits = amplitude_new_bits;
        compact_space.add_blanks(blank_collector.drain().map(|blank| blank.blank_range()));
    }

    // special case, when we don't collected any blanks because:
    // * the data is empty (early exit)
    // * the algorithm did decide it's not worth the cost, which can be the case for single values
    //
    // We drain one collected blank unconditionally, so the empty case is reserved for empty
    // data, and therefore empty compact_space means the data is empty and no data is covered
    // (conversely to all data) and we can assign null to it.
    if compact_space.is_empty() {
        compact_space.add_blanks(
            blank_collector
                .drain()
                .map(|blank| blank.blank_range())
                .take(1),
        );
    }

    compact_space.finish()
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpaceBuilder {
    blanks: Vec<RangeInclusive<u128>>,
}

impl CompactSpaceBuilder {
    /// Creates a new compact space builder which will initially cover the whole space.
    fn new() -> Self {
        Self { blanks: vec![] }
    }

    /// Assumes that repeated add_blank calls don't overlap and are not adjacent,
    /// e.g. [3..=5, 5..=10] is not allowed
    ///
    /// Both of those assumptions are true when blanks are produced from sorted values.
    fn add_blanks(&mut self, blank: impl Iterator<Item = RangeInclusive<u128>>) {
        self.blanks.extend(blank);
    }

    fn is_empty(&self) -> bool {
        self.blanks.is_empty()
    }

    /// Convert blanks to covered space and assign null value
    fn finish(mut self) -> CompactSpace {
        // sort by start. ranges are not allowed to overlap
        self.blanks.sort_unstable_by_key(|blank| *blank.start());

        // Between the blanks
        let mut covered_space = self
            .blanks
            .iter()
            .tuple_windows()
            .map(|(left, right)| {
                assert!(
                    left.end() < right.start(),
                    "overlapping or adjacent ranges detected"
                );
                *left.end() + 1..=*right.start() - 1
            })
            .collect::<Vec<_>>();

        // Outside the blanks
        if let Some(first_blank_start) = self.blanks.first().map(RangeInclusive::start) {
            if *first_blank_start != 0 {
                covered_space.insert(0, 0..=first_blank_start - 1);
            }
        }

        if let Some(last_blank_end) = self.blanks.last().map(RangeInclusive::end) {
            if *last_blank_end != u128::MAX {
                covered_space.push(last_blank_end + 1..=u128::MAX);
            }
        }

        // Extend the first range and assign the null value to it.
        let null_value = if let Some(first_covered_space) = covered_space.first_mut() {
            // in case the first covered space ends at u128::MAX, assign null to the beginning
            if *first_covered_space.end() == u128::MAX {
                *first_covered_space = first_covered_space.start() - 1..=*first_covered_space.end();
                *first_covered_space.start()
            } else {
                *first_covered_space = *first_covered_space.start()..=first_covered_space.end() + 1;
                *first_covered_space.end()
            }
        } else {
            covered_space.push(0..=0); // empty data case
            0u128
        };

        let mut compact_start: u64 = 0;
        let mut ranges_mapping: Vec<RangeMapping> = Vec::with_capacity(covered_space.len());
        for cov in covered_space {
            let range_mapping = super::RangeMapping {
                value_range: cov,
                compact_start,
            };
            let covered_range_len = range_mapping.range_length();
            ranges_mapping.push(range_mapping);
            compact_start += covered_range_len as u64;
        }
        CompactSpace {
            ranges_mapping,
            null_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_heap_pop_order() {
        let mut blanks: BinaryHeap<BlankRange> = BinaryHeap::new();
        blanks.push((0..=10).try_into().unwrap());
        blanks.push((100..=200).try_into().unwrap());
        blanks.push((100..=110).try_into().unwrap());
        assert_eq!(blanks.pop().unwrap().blank_size(), 101);
        assert_eq!(blanks.pop().unwrap().blank_size(), 11);
    }
}
