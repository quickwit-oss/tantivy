use std::sync::Arc;

use super::shared_threshold::{AtomicSharedThreshold, SharedThresholdArcOpt};
use crate::collector::sort_key::NaturalComparator;
use crate::collector::sort_key_top_collector::TopBySortKeySegmentCollector;
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

    fn collect_segment_top_k(
        &self,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_collector: &mut TopBySortKeySegmentCollector<Self::Child, Self::Comparator>,
    ) -> crate::Result<()> {
        let top_n = &mut segment_collector.topn_computer;

        let (initial_score, initial_ord) = top_n
            .shared_threshold
            .as_ref()
            .and_then(|s| s.load())
            .unwrap_or((Score::MIN, SegmentOrdinal::MAX));

        top_n.set_threshold((initial_score, initial_ord));

        let pruning_threshold = top_n.pruning_threshold.unwrap_or(Score::MIN);

        if let Some(alive_bitset) = reader.alive_bitset() {
            weight.for_each_pruning(pruning_threshold, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) {
                    return top_n.pruning_threshold.unwrap_or(Score::MIN);
                }
                top_n.push(score, doc);
                top_n.pruning_threshold.unwrap_or(Score::MIN)
            })?;
        } else {
            weight.for_each_pruning(pruning_threshold, reader, &mut |doc, score| {
                top_n.push(score, doc);
                top_n.pruning_threshold.unwrap_or(Score::MIN)
            })?;
        }

        Ok(())
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
}
