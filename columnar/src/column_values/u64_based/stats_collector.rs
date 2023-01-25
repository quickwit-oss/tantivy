use std::num::NonZeroU64;

use fastdivide::DividerU64;

use crate::column_values::u64_based::Stats;
use crate::RowId;

/// Compute the gcd of two non null numbers.
///
/// It is recommended, but not required, to feed values such that `large >= small`.
fn compute_gcd(mut large: NonZeroU64, mut small: NonZeroU64) -> NonZeroU64 {
    loop {
        let rem: u64 = large.get() % small;
        if let Some(new_small) = NonZeroU64::new(rem) {
            (large, small) = (small, new_small);
        } else {
            return small;
        }
    }
}

#[derive(Default)]
pub struct StatsCollector {
    pub(crate) min_max_opt: Option<(u64, u64)>,
    pub(crate) num_rows: RowId,
    pub(crate) gcd_opt: Option<(NonZeroU64, DividerU64)>,
}

impl StatsCollector {
    pub fn stats(&self) -> Stats {
        let (min_value, max_value) = self.min_max_opt.unwrap_or((0u64, 0u64));
        let gcd = if let Some((gcd, _)) = self.gcd_opt {
            gcd.get()
        } else {
            1u64
        };
        Stats {
            min_value,
            max_value,
            num_rows: self.num_rows,
            gcd,
        }
    }

    #[inline]
    fn update_gcd(&mut self, non_zero_value: NonZeroU64) {
        let Some((gcd, gcd_divider)) = self.gcd_opt else {
            self.set_gcd(non_zero_value);
            return;
        };
        if gcd.get() == 1 {
            return;
        }
        let remainder =
            non_zero_value.get() - (gcd_divider.divide(non_zero_value.get())) * gcd.get();
        if remainder == 0 {
            return;
        }
        let new_gcd = compute_gcd(non_zero_value, gcd);
        self.set_gcd(new_gcd);
    }

    fn set_gcd(&mut self, gcd: NonZeroU64) {
        let new_divider = DividerU64::divide_by(gcd.get());
        self.gcd_opt = Some((gcd, new_divider));
    }

    pub fn collect(&mut self, value: u64) {
        self.min_max_opt = Some(if let Some((min, max)) = self.min_max_opt {
            (min.min(value), max.max(value))
        } else {
            (value, value)
        });
        self.num_rows += 1;
        let Some(non_zero_value) = NonZeroU64::new(value) else {
            return;
        };
        self.update_gcd(non_zero_value);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::column_values::u64_based::stats_collector::{compute_gcd, StatsCollector};
    use crate::column_values::u64_based::Stats;

    fn compute_stats(vals: impl Iterator<Item = u64>) -> Stats {
        let mut stats_collector = StatsCollector::default();
        for val in vals {
            stats_collector.collect(val);
        }
        stats_collector.stats()
    }

    fn find_gcd(vals: impl Iterator<Item = u64>) -> u64 {
        compute_stats(vals).gcd
    }

    #[test]
    fn test_compute_gcd() {
        let test_compute_gcd_aux = |large, small, expected| {
            let large = NonZeroU64::new(large).unwrap();
            let small = NonZeroU64::new(small).unwrap();
            let expected = NonZeroU64::new(expected).unwrap();
            assert_eq!(compute_gcd(small, large), expected);
            assert_eq!(compute_gcd(large, small), expected);
        };
        test_compute_gcd_aux(1, 4, 1);
        test_compute_gcd_aux(2, 4, 2);
        test_compute_gcd_aux(10, 25, 5);
        test_compute_gcd_aux(25, 25, 25);
    }

    #[test]
    fn test_gcd() {
        assert_eq!(find_gcd([0].into_iter()), 1);
        assert_eq!(find_gcd([0, 10].into_iter()), 10);
        assert_eq!(find_gcd([10, 0].into_iter()), 10);
        assert_eq!(find_gcd([].into_iter()), 1);
        assert_eq!(find_gcd([15, 30, 5, 10].into_iter()), 5);
        assert_eq!(find_gcd([15, 16, 10].into_iter()), 1);
        assert_eq!(find_gcd([0, 5, 5, 5].into_iter()), 5);
        assert_eq!(find_gcd([0, 0].into_iter()), 1);
    }

    #[test]
    fn test_stats() {
        assert_eq!(
            compute_stats([].into_iter()),
            Stats {
                gcd: 1,
                min_value: 0,
                max_value: 0,
                num_rows: 0
            }
        );
        assert_eq!(
            compute_stats([0, 1].into_iter()),
            Stats {
                gcd: 1,
                min_value: 0,
                max_value: 1,
                num_rows: 2
            }
        );
        assert_eq!(
            compute_stats([0, 1].into_iter()),
            Stats {
                gcd: 1,
                min_value: 0,
                max_value: 1,
                num_rows: 2
            }
        );
        assert_eq!(
            compute_stats([10, 30].into_iter()),
            Stats {
                gcd: 10,
                min_value: 10,
                max_value: 30,
                num_rows: 2
            }
        );
        assert_eq!(
            compute_stats([10, 0, 30].into_iter()),
            Stats {
                gcd: 10,
                min_value: 0,
                max_value: 30,
                num_rows: 3
            }
        );
    }
}
