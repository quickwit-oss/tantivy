use std::collections::Bound;

mod fastfield_filter_query;
mod fastfield_filter_weight;
mod fastfield_filter_scorer;


pub use self::fastfield_filter_query::FastFieldFilterQuery;
use self::fastfield_filter_weight::FastFieldFilterWeight;
use self::fastfield_filter_scorer::FastFieldFilterScorer;

#[derive(Debug, Clone)]
pub(crate) struct RangeU64 {
    pub low: Bound<u64>,
    pub high: Bound<u64>,
}


impl RangeU64 {

    fn match_high(&self, val: u64) -> bool {
        match self.high {
            Bound::Excluded(bound) =>
                val < bound,
            Bound::Included(bound) =>
                val <= bound,
            Bound::Unbounded =>
                true
        }
    }

    fn match_low(&self, val: u64) -> bool {
        match self.high {
            Bound::Excluded(bound) =>
                bound < val,
            Bound::Included(bound) =>
                bound <= val,
            Bound::Unbounded =>
                true
        }
    }

    pub fn contains(&self, val: u64) -> bool {
        self.match_low(val) && self.match_high(val)
    }

}
