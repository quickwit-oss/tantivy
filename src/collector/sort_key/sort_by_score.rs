use std::sync::Arc;

use super::shared_threshold::{AtomicSharedThreshold, SharedThresholdArcOpt};
use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::{DocId, Score, SegmentOrdinal};

#[derive(Clone)]
pub struct SortBySimilarityScore {
    shared_threshold: SharedThresholdArcOpt<Score>,
}

impl std::fmt::Debug for SortBySimilarityScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortBySimilarityScore")
            .field(
                "threshold",
                &self.shared_threshold.as_ref().and_then(|s| s.load()),
            )
            .finish()
    }
}

impl Default for SortBySimilarityScore {
    fn default() -> Self {
        Self {
            shared_threshold: Some(Arc::new(AtomicSharedThreshold::default())),
        }
    }
}

impl SortBySimilarityScore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_shared_threshold(shared_threshold: SharedThresholdArcOpt<Score>) -> Self {
        Self { shared_threshold }
    }
}

impl SortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;

    type Child = SortBySimilarityScore;

    type Comparator = NaturalComparator;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn shared_threshold(
        &self,
    ) -> SharedThresholdArcOpt<
        <<Self as SortKeyComputer>::Child as SegmentSortKeyComputer>::SegmentSortKey,
    > {
        self.shared_threshold.clone()
    }

    fn segment_sort_key_computer(
        &self,
        _segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(self.clone())
    }
}

impl SegmentSortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;
    type SegmentSortKey = Score;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, _doc: DocId, score: Score) -> Score {
        score
    }

    fn convert_segment_sort_key(&self, score: Score) -> Score {
        score
    }

    fn supports_bm25_pruning(&self) -> bool {
        true
    }

    fn bm25_pruning_threshold(
        &self,
        threshold: &Score,
        segment_ord: SegmentOrdinal,
        threshold_ord: SegmentOrdinal,
    ) -> Option<Score> {
        if segment_ord < threshold_ord {
            Some(threshold.next_down())
        } else {
            Some(*threshold)
        }
    }
}
