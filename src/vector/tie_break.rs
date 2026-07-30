//! The default, empty tie-break for the vector top-N heap.
//!
//! [`TopDocsByVectorSimilarity`](super::TopDocsByVectorSimilarity) sorts by a
//! composite `(similarity, tie_break)` key so a secondary `ORDER BY` column can
//! participate in heap eviction rather than being applied after the losing
//! candidates are already gone. When no secondary column was requested, the tail
//! is this ZST: the key becomes `(Score, ())`, which has the same layout as
//! `Score`, and the comparator pair degenerates to comparing the similarity
//! alone. The no-tie-break path is therefore identical to the pre-composite one
//! by construction rather than by measurement.

use std::cmp::Ordering;

use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::{DocId, Score, SegmentOrdinal, SegmentReader};

/// A [`SortKeyComputer`] that contributes nothing to the ordering.
///
/// See the [module docs](self) for why this exists rather than a separate
/// non-composite code path.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTieBreak;

impl SortKeyComputer for NoTieBreak {
    type SortKey = ();
    type Child = NoTieBreak;
    type Comparator = NaturalComparator;

    fn segment_sort_key_computer(&self, _segment_reader: &SegmentReader) -> crate::Result<Self> {
        Ok(NoTieBreak)
    }
}

impl SegmentSortKeyComputer for NoTieBreak {
    type SortKey = ();
    type SegmentSortKey = ();
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, _doc: DocId, _score: Score) {}

    #[inline(always)]
    fn compare_segment_sort_key(&self, _left: &(), _right: &()) -> Ordering {
        Ordering::Equal
    }

    fn convert_segment_sort_key(&self, _sort_key: ()) {}

    fn supports_bm25_pruning(&self) -> bool {
        false
    }

    fn bm25_pruning_threshold(
        &self,
        _threshold: &(),
        _segment_ord: SegmentOrdinal,
        _threshold_ord: SegmentOrdinal,
    ) -> Option<Score> {
        None
    }
}
