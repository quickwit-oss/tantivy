use columnar::ValueRange;

use crate::collector::sort_key::NaturalComparator;
use crate::collector::{ComparableDoc, SegmentSortKeyComputer, SortKeyComputer, TopNComputer};
use crate::{DocAddress, DocId, Score};

/// Sort by similarity score.
#[derive(Clone, Debug, Copy)]
pub struct SortBySimilarityScore;

impl SortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;

    type Child = SortBySimilarityScoreSegmentComputer;

    type Comparator = NaturalComparator;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn segment_sort_key_computer(
        &self,
        _segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(SortBySimilarityScoreSegmentComputer)
    }

    // Sorting by score is special in that it allows for the Block-Wand optimization.
    fn collect_segment_top_k(
        &self,
        k: usize,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_ord: u32,
    ) -> crate::Result<Vec<(Self::SortKey, DocAddress)>> {
        let mut top_n: TopNComputer<Score, DocId, Self::Comparator> =
            TopNComputer::new_with_comparator(k, self.comparator());

        if let Some(alive_bitset) = reader.alive_bitset() {
            let mut threshold = Score::MIN;
            top_n.threshold = Some(threshold);
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) {
                    return threshold;
                }
                top_n.push(score, doc);
                threshold = top_n.threshold.unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                top_n.push(score, doc);
                top_n.threshold.unwrap_or(Score::MIN)
            })?;
        }

        Ok(top_n
            .into_vec()
            .into_iter()
            .map(|cid| (cid.sort_key, DocAddress::new(segment_ord, cid.doc)))
            .collect())
    }
}

pub struct SortBySimilarityScoreSegmentComputer;

impl SegmentSortKeyComputer for SortBySimilarityScoreSegmentComputer {
    type SortKey = Score;
    type SegmentSortKey = Score;
    type SegmentComparator = NaturalComparator;
    type Buffer = ();

    #[inline(always)]
    fn segment_sort_key(&mut self, _doc: DocId, score: Score) -> Score {
        score
    }

    fn segment_sort_keys(
        &mut self,
        _input_docs: &[DocId],
        _output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
        _buffer: &mut Self::Buffer,
        _filter: ValueRange<Self::SegmentSortKey>,
    ) {
        unimplemented!("Batch computation not supported for score sorting")
    }

    fn convert_segment_sort_key(&self, score: Score) -> Score {
        score
    }
}
