use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::{DocId, Score};

/// Sort by similarity score.
#[derive(Clone, Debug, Copy)]
pub struct SortByScore;

impl SortKeyComputer for SortByScore {
    type SortKey = Score;

    type Child = SortByScore;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn segment_sort_key_computer(
        &self,
        _segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(SortByScore)
    }
}

impl SegmentSortKeyComputer for SortByScore {
    type SortKey = Score;

    type SegmentSortKey = Score;

    fn sort_key(&mut self, _doc: DocId, score: Score) -> Score {
        score
    }

    fn convert_segment_sort_key(&self, score: Score) -> Score {
        score
    }
}
