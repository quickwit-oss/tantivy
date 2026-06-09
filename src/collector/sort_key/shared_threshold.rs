use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::{Score, SegmentOrdinal};

pub type SharedThresholdArc<T> = Arc<dyn SharedThreshold<T>>;
pub type SharedThresholdArcOpt<T> = Option<SharedThresholdArc<T>>;

/// A trait for sharing a search threshold across multiple threads or segments.
///
/// Implementations of this trait must be thread-safe as they are typically wrapped in an [`Arc`].
/// The threshold is used to prune documents that cannot possibly compete with the top-K
/// documents already found in other segments.
pub trait SharedThreshold<T>: Send + Sync {
    /// Loads the current shared threshold and its associated segment ordinal.
    ///
    /// Returns `None` if the threshold is uninitialized (i.e. no segment has
    /// successfully pushed a document yet).
    ///
    /// Among documents with the same sort key, we favor those from segments with a lower ordinal.
    /// This is consistent with the tie-breaking behavior of [`DocAddress`], which ensures
    /// stable sorting across multiple segments.
    fn load(&self) -> Option<(T, SegmentOrdinal)>;

    /// Attempts to update the shared threshold to `new_threshold`.
    ///
    /// This method succeeds if the internal threshold exactly matches `expected_threshold`.
    ///
    /// Returns `Ok(())` on success, or `Err(current_threshold)` if the internal state was
    /// different, in which case the caller should evaluate the new `current_threshold` and retry
    /// if appropriate.
    fn try_update(
        &self,
        expected_threshold: &Option<(T, SegmentOrdinal)>,
        new_threshold: (T, SegmentOrdinal),
    ) -> Result<(), Option<(T, SegmentOrdinal)>>;

    /// Returns a threshold value `T` that is strictly better than the current shared threshold
    /// if the given `segment_ord` would lose a tie-break against the `threshold_ord`.
    ///
    /// This is used for Block-Max WAND pruning to ensure determinism when multiple segments
    /// have documents with the same score. Currently, this is only used for score-based
    /// pushdown; for other types, it can safely return the value unchanged.
    ///
    /// # Tie-breaking Logic
    /// Tantivy breaks ties on sort keys by favoring documents with a lower [`DocAddress`].
    /// A [`DocAddress`] is composed of a `segment_ord` and a `doc_id`. Since documents within
    /// a segment are processed in ascending `doc_id` order, any new document in a segment
    /// with `segment_ord >= threshold_ord` will have a strictly higher [`DocAddress`] than the
    /// document that set the threshold.
    ///
    /// Therefore, if `segment_ord >= threshold_ord`, this method should return a threshold that
    /// is "one step better" than `value`. For floating-point scores, this is typically
    /// `value.next_up()`. If `segment_ord < threshold_ord`, the document could still win the
    /// tie-break, so the `value` should be returned unchanged.
    fn competitive_threshold(
        &self,
        value: T,
        threshold_ord: SegmentOrdinal,
        segment_ord: SegmentOrdinal,
    ) -> T {
        let _ = threshold_ord;
        let _ = segment_ord;
        value
    }
}

#[inline]
fn f32_to_ordered_u32(f: f32) -> u32 {
    let bits = f.to_bits();
    if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

#[inline]
fn ordered_u32_to_f32(u: u32) -> f32 {
    let bits = if u & 0x8000_0000 != 0 {
        u ^ 0x8000_0000
    } else {
        !u
    };
    f32::from_bits(bits)
}

/// Packs an f32 Score and a segment ordinal into a u64 for lock-free atomic max operations.
/// We want to maximize the score, but *minimize* the segment ordinal to favor lower segments
/// in tie-breakers. So we pack the inverted segment ordinal into the lower 32 bits.
#[inline(always)]
fn pack_score_and_ord(score: Score, segment_ord: SegmentOrdinal) -> u64 {
    let top = f32_to_ordered_u32(score) as u64;
    let bottom = (!segment_ord) as u64;
    (top << 32) | bottom
}

#[inline(always)]
fn unpack_score_and_ord(val: u64) -> (Score, SegmentOrdinal) {
    let score = ordered_u32_to_f32((val >> 32) as u32);
    let segment_ord = !(val as u32);
    (score, segment_ord)
}

pub struct AtomicSharedThreshold {
    value: AtomicU64,
}

impl Default for AtomicSharedThreshold {
    fn default() -> Self {
        Self {
            value: AtomicU64::new(pack_score_and_ord(Score::MIN, SegmentOrdinal::MAX)),
        }
    }
}
impl SharedThreshold<Score> for AtomicSharedThreshold {
    fn load(&self) -> Option<(Score, SegmentOrdinal)> {
        let packed = self.value.load(Ordering::Relaxed);
        let (score, ord) = unpack_score_and_ord(packed);
        if score == Score::MIN && ord == SegmentOrdinal::MAX {
            None
        } else {
            Some((score, ord))
        }
    }

    fn try_update(
        &self,
        expected_threshold: &Option<(Score, SegmentOrdinal)>,
        new_threshold: (Score, SegmentOrdinal),
    ) -> Result<(), Option<(Score, SegmentOrdinal)>> {
        let expected_packed = match expected_threshold {
            Some((score, ord)) => pack_score_and_ord(*score, *ord),
            None => pack_score_and_ord(Score::MIN, SegmentOrdinal::MAX),
        };
        let new_packed = pack_score_and_ord(new_threshold.0, new_threshold.1);
        match self.value.compare_exchange_weak(
            expected_packed,
            new_packed,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(()),
            Err(actual_packed) => {
                let actual = unpack_score_and_ord(actual_packed);
                if actual.0 == Score::MIN && actual.1 == SegmentOrdinal::MAX {
                    Err(None)
                } else {
                    Err(Some(actual))
                }
            }
        }
    }

    fn competitive_threshold(
        &self,
        value: Score,
        threshold_ord: SegmentOrdinal,
        segment_ord: SegmentOrdinal,
    ) -> Score {
        if segment_ord < threshold_ord {
            // If our segment wins tie-breaks against the threshold setter, we want to accept
            // documents with a score GREATER THAN OR EQUAL to the threshold.
            // Since Tantivy's pruning loop uses a strict `score > threshold` check, we
            // return `next_down()` to effectively relax the check to `>=`.
            value.next_down()
        } else {
            // If our segment loses tie-breaks, we demand a score STRICTLY GREATER than the
            // threshold. The pruning loop's `score > threshold` check already achieves this.
            value
        }
    }
}

/// A shared threshold for `Option<u64>` values.
///
/// In both `NaturalComparator` and `ReverseNoneIsLowerComparator`, `None` is the worst value
/// (it appears last in top docs). So the initial threshold is `None`.
///
/// Since `AtomicU64` cannot cleanly pack `Option<u64>` without losing a state, and threshold
/// updates are very rare compared to reads, we use a `RwLock<(Option<u64>, SegmentOrdinal)>`.
pub struct RwLockSharedThresholdOptionU64 {
    value: RwLock<Option<(Option<u64>, SegmentOrdinal)>>,
}

impl Default for RwLockSharedThresholdOptionU64 {
    fn default() -> Self {
        Self {
            value: RwLock::new(None),
        }
    }
}

impl RwLockSharedThresholdOptionU64 {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SharedThreshold<Option<u64>> for RwLockSharedThresholdOptionU64 {
    fn load(&self) -> Option<(Option<u64>, SegmentOrdinal)> {
        *self.value.read().unwrap()
    }

    fn try_update(
        &self,
        expected_threshold: &Option<(Option<u64>, SegmentOrdinal)>,
        new_threshold: (Option<u64>, SegmentOrdinal),
    ) -> Result<(), Option<(Option<u64>, SegmentOrdinal)>> {
        let mut guard = self.value.write().unwrap();
        if *guard == *expected_threshold {
            *guard = Some(new_threshold);
            Ok(())
        } else {
            Err(*guard)
        }
    }

    fn competitive_threshold(
        &self,
        value: Option<u64>,
        _threshold_ord: SegmentOrdinal,
        _segment_ord: SegmentOrdinal,
    ) -> Option<u64> {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::sort_key::{Comparator, ComparatorEnum};
    use crate::Order;

    #[test]
    fn test_f32_ordered_roundtrip() {
        let values = [Score::MIN, -1.0, -0.0, 0.0, 0.5, 1.0, 42.0, Score::MAX];
        for &v in &values {
            let u = f32_to_ordered_u32(v);
            let back = ordered_u32_to_f32(u);
            assert_eq!(v.to_bits(), back.to_bits(), "roundtrip failed for {v}");
        }
    }

    fn update_helper<T: Clone + std::fmt::Debug, C>(
        t: &impl SharedThreshold<T>,
        val: T,
        ord: SegmentOrdinal,
        _comparator: &C,
        is_better: impl Fn(&T, SegmentOrdinal, &T, SegmentOrdinal) -> bool,
    ) {
        let mut current = t.load();
        loop {
            if let Some(ref curr) = current {
                if !is_better(&val, ord, &curr.0, curr.1) {
                    break;
                }
            }
            match t.try_update(&current, (val.clone(), ord)) {
                Ok(()) => break,
                Err(actual) => current = actual,
            }
        }
    }

    #[test]
    fn test_pack_score_and_ord() {
        let packed1 = pack_score_and_ord(1.0, 5);
        let packed2 = pack_score_and_ord(1.0, 2); // ord 2 is better than ord 5
        let packed3 = pack_score_and_ord(2.0, 10); // score 2.0 is better than score 1.0

        assert!(packed2 > packed1);
        assert!(packed3 > packed2);

        assert_eq!(unpack_score_and_ord(packed1), (1.0, 5));
        assert_eq!(unpack_score_and_ord(packed2), (1.0, 2));
        assert_eq!(unpack_score_and_ord(packed3), (2.0, 10));
    }

    #[test]
    fn test_atomic_shared_threshold() {
        let t = AtomicSharedThreshold::default();
        assert_eq!(t.load(), None);

        let is_better = |a: &Score, a_ord: SegmentOrdinal, b: &Score, b_ord: SegmentOrdinal| {
            if a > b {
                true
            } else if a == b {
                a_ord < b_ord
            } else {
                false
            }
        };

        update_helper(&t, 0.5, 5, &(), &is_better);
        assert_eq!(t.load(), Some((0.5, 5)));

        update_helper(&t, 0.3, 2, &(), &is_better);
        assert_eq!(t.load(), Some((0.5, 5)));

        update_helper(&t, 0.5, 2, &(), &is_better); // Same score, better ord
        assert_eq!(t.load(), Some((0.5, 2)));

        update_helper(&t, 0.9, 10, &(), &is_better);
        assert_eq!(t.load(), Some((0.9, 10)));
    }

    #[test]
    fn test_competitive_threshold() {
        let t = AtomicSharedThreshold::default();
        let is_better = |a: &Score, a_ord: SegmentOrdinal, b: &Score, b_ord: SegmentOrdinal| {
            if a > b {
                true
            } else if a == b {
                a_ord < b_ord
            } else {
                false
            }
        };
        update_helper(&t, 0.5, 5, &(), &is_better);

        // Segment 5 itself: should require strictly greater score for new docs.
        // The pruning loop uses `score > threshold`, so returning 0.5 unchanged
        // means we only accept scores > 0.5.
        assert_eq!(t.competitive_threshold(0.5, 5, 5), 0.5);

        // Segment 6 (later): should require strictly greater score.
        assert_eq!(t.competitive_threshold(0.5, 5, 6), 0.5);

        // Segment 4 (earlier): can accept equal score (>= 0.5).
        // Since pruning loop is `score > threshold`, we must return `next_down(0.5)`
        // so that `0.5 > next_down(0.5)` is true.
        assert_eq!(t.competitive_threshold(0.5, 5, 4), 0.5f32.next_down());
    }

    #[test]
    fn test_rwlock_shared_threshold_option_u64_asc() {
        let t = RwLockSharedThresholdOptionU64::new();
        let cmp = ComparatorEnum::from(Order::Asc);
        let compare = |a: &Option<u64>, b: &Option<u64>| cmp.compare(a, b);
        let is_better =
            |a: &Option<u64>, a_ord: SegmentOrdinal, b: &Option<u64>, b_ord: SegmentOrdinal| {
                match compare(a, b) {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Equal => a_ord < b_ord,
                }
            };

        assert_eq!(t.load(), None);

        update_helper(&t, Some(100), 5, &cmp, &is_better);
        assert_eq!(t.load(), Some((Some(100), 5)));

        update_helper(&t, Some(200), 2, &cmp, &is_better); // 200 > 100, worse
        assert_eq!(t.load(), Some((Some(100), 5)));

        update_helper(&t, Some(100), 2, &cmp, &is_better); // Same score, better ord
        assert_eq!(t.load(), Some((Some(100), 2)));

        update_helper(&t, None, 1, &cmp, &is_better); // None is strictly smaller than Some(100)
        assert_eq!(t.load(), Some((Some(100), 2)));

        update_helper(&t, Some(0), 10, &cmp, &is_better); // Some(0) > None, better
        assert_eq!(t.load(), Some((Some(0), 10)));
    }

    #[test]
    fn test_rwlock_shared_threshold_option_u64_desc() {
        let t = RwLockSharedThresholdOptionU64::new();
        let cmp = ComparatorEnum::from(Order::Desc);
        let compare = |a: &Option<u64>, b: &Option<u64>| cmp.compare(a, b);
        let is_better =
            |a: &Option<u64>, a_ord: SegmentOrdinal, b: &Option<u64>, b_ord: SegmentOrdinal| {
                match compare(a, b) {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Equal => a_ord < b_ord,
                }
            };

        assert_eq!(t.load(), None);

        update_helper(&t, Some(100), 5, &cmp, &is_better);
        assert_eq!(t.load(), Some((Some(100), 5)));

        update_helper(&t, Some(50), 2, &cmp, &is_better); // 50 < 100, worse
        assert_eq!(t.load(), Some((Some(100), 5)));

        update_helper(&t, Some(100), 2, &cmp, &is_better); // Same score, better ord
        assert_eq!(t.load(), Some((Some(100), 2)));

        update_helper(&t, Some(200), 10, &cmp, &is_better); // 200 > 100
        assert_eq!(t.load(), Some((Some(200), 10)));

        update_helper(&t, None, 1, &cmp, &is_better); // None < Some(200), worse
        assert_eq!(t.load(), Some((Some(200), 10)));
    }
}
