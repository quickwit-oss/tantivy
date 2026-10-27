use std::ops::Range;

/// Minimizes requests for sorted ranges using at most `extra_bytes` beyond their union.
///
/// Empty and reversed ranges are ignored, and overlaps and touching ranges are
/// normalized in place. The cheapest positive gaps are then filled, minimizing
/// added bytes for the resulting request count. Equal gaps are filled earliest first.
/// All input ranges must be sorted by start. Uses O(n log n) time and O(n) scratch space.
pub fn merge_ranges_with_byte_budget(ranges: &mut Vec<Range<usize>>, extra_bytes: usize) {
    debug_assert!(ranges.windows(2).all(|pair| pair[0].start <= pair[1].start));
    ranges.retain(|range| !range.is_empty());
    ranges.dedup_by(|next, previous| {
        if next.start <= previous.end {
            previous.end = previous.end.max(next.end);
            true
        } else {
            false
        }
    });
    if ranges.len() <= 1 || extra_bytes == 0 {
        return;
    }

    let mut gaps: Vec<(usize, usize)> = ranges
        .windows(2)
        .enumerate()
        .map(|(index, pair)| (pair[1].start - pair[0].end, index))
        .collect();
    gaps.sort_unstable();
    let mut remaining = extra_bytes;
    let mut cutoff = None;
    for (gap, index) in gaps {
        if gap > remaining {
            break;
        }
        remaining -= gap;
        cutoff = Some((gap, index));
    }
    let Some(cutoff) = cutoff else {
        return;
    };

    let mut index = 0;
    ranges.dedup_by(|next, previous| {
        let merge = (next.start - previous.end, index) <= cutoff;
        index += 1;
        if merge {
            previous.end = next.end;
        }
        merge
    });
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::merge_ranges_with_byte_budget;

    fn ranges_from_gaps(gaps: &[usize]) -> Vec<Range<usize>> {
        let mut ranges = vec![0..1];
        for gap in gaps {
            let start = ranges.last().unwrap().end + gap;
            ranges.push(start..start + 1);
        }
        ranges
    }

    #[test]
    fn exact_budget_and_one_byte_over() {
        for (budget, expected) in [
            (0, vec![0..1, 4..5, 8..9]),
            (2, vec![0..1, 4..5, 8..9]),
            (3, vec![0..5, 8..9]),
            (5, vec![0..5, 8..9]),
            (6, vec![0..9]),
        ] {
            let mut ranges = ranges_from_gaps(&[3, 3]);
            merge_ranges_with_byte_budget(&mut ranges, budget);
            assert_eq!(ranges, expected, "budget={budget}");
        }
    }

    #[test]
    fn chooses_globally_cheapest_gaps() {
        let mut ranges = ranges_from_gaps(&[4, 2, 3]);
        merge_ranges_with_byte_budget(&mut ranges, 5);
        assert_eq!(ranges, vec![0..1, 5..13]);
    }

    #[test]
    fn equal_gaps_choose_earliest_after_cheaper_gaps() {
        let mut ranges = ranges_from_gaps(&[3, 1, 3, 1, 3]);
        merge_ranges_with_byte_budget(&mut ranges, 8);
        assert_eq!(ranges, vec![0..13, 16..17]);
    }

    #[test]
    fn budget_is_relative_to_input_union() {
        let mut ranges = Vec::new();
        merge_ranges_with_byte_budget(&mut ranges, usize::MAX);
        assert!(ranges.is_empty());
        ranges = vec![Range { start: 1, end: 0 }, 2..2];
        merge_ranges_with_byte_budget(&mut ranges, 0);
        assert!(ranges.is_empty());
        let original = vec![
            0..0,
            1..6,
            2..3,
            3..8,
            Range { start: 7, end: 4 },
            8..10,
            11..11,
            12..14,
        ];
        for (budget, expected) in [(0, vec![1..10, 12..14]), (2, vec![1..14])] {
            ranges = original.clone();
            merge_ranges_with_byte_budget(&mut ranges, budget);
            assert_eq!(ranges, expected);
        }
    }

    #[test]
    fn handles_usize_endpoints_and_budget() {
        let max = usize::MAX;
        let original = vec![0..1, max - 4..max - 3, max - 1..max];
        for (budget, expected) in [
            (2, vec![0..1, max - 4..max]),
            (max - 4, vec![0..1, max - 4..max]),
            (max - 3, vec![0..max]),
            (max, vec![0..max]),
        ] {
            let mut ranges = original.clone();
            merge_ranges_with_byte_budget(&mut ranges, budget);
            assert_eq!(ranges, expected, "budget={budget}");
        }
    }

    #[test]
    fn exhaustive_small_gaps_match_subset_optimum() {
        for gap_count in 0..=5 {
            for mut encoded in 0..4usize.pow(gap_count) {
                let gaps: Vec<usize> = (0..gap_count)
                    .map(|_| {
                        let gap = encoded % 4 + 1;
                        encoded /= 4;
                        gap
                    })
                    .collect();
                let original = ranges_from_gaps(&gaps);
                for budget in 0..=gaps.iter().sum::<usize>() + 1 {
                    let mut best = (original.len(), 0);
                    for subset in 0..1usize << gap_count {
                        let mut cost = 0;
                        let mut count = original.len();
                        for (index, &gap) in gaps.iter().enumerate() {
                            if subset & (1 << index) != 0 {
                                cost += gap;
                                count -= 1;
                            }
                        }
                        if cost <= budget {
                            best = best.min((count, cost));
                        }
                    }
                    let mut ranges = original.clone();
                    merge_ranges_with_byte_budget(&mut ranges, budget);
                    let bytes: usize = ranges.iter().map(|range| range.end - range.start).sum();
                    assert_eq!(
                        (ranges.len(), bytes - original.len()),
                        best,
                        "gaps={gaps:?}, budget={budget}"
                    );
                    assert!(ranges.windows(2).all(|pair| pair[0].end < pair[1].start));
                    for range in &original {
                        assert!(ranges.iter().any(|output| {
                            output.start <= range.start && range.end <= output.end
                        }));
                    }
                }
            }
        }
    }
}
