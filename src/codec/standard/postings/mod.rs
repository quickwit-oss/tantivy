use std::io;

use crate::codec::postings::block_wand::{block_wand, block_wand_single_scorer};
use crate::codec::postings::PostingsCodec;
use crate::codec::standard::postings::block_segment_postings::BlockSegmentPostings;
pub use crate::codec::standard::postings::segment_postings::SegmentPostings;
use crate::fieldnorm::FieldNormReader;
use crate::positions::PositionReader;
use crate::query::term_query::TermScorer;
use crate::query::{BufferedUnionScorer, Scorer, SumCombiner};
use crate::schema::IndexRecordOption;
use crate::{DocSet as _, Score, TERMINATED};

mod block;
mod block_segment_postings;
mod segment_postings;
mod skip;
mod standard_postings_serializer;

pub use segment_postings::SegmentPostings as StandardPostings;
pub use standard_postings_serializer::StandardPostingsSerializer;

/// The default postings codec for tantivy.
pub struct StandardPostingsCodec;

#[expect(clippy::enum_variant_names)]
#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub(crate) enum FreqReadingOption {
    NoFreq,
    SkipFreq,
    ReadFreq,
}

impl PostingsCodec for StandardPostingsCodec {
    type PostingsSerializer = StandardPostingsSerializer;
    type Postings = SegmentPostings;

    fn new_serializer(
        &self,
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> Self::PostingsSerializer {
        StandardPostingsSerializer::new(avg_fieldnorm, mode, fieldnorm_reader)
    }

    fn load_postings(
        &self,
        doc_freq: u32,
        postings_data: common::OwnedBytes,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
        positions_data_opt: Option<common::OwnedBytes>,
    ) -> io::Result<Self::Postings> {
        // Rationalize record_option/requested_option.
        let requested_option = requested_option.downgrade(record_option);
        let block_segment_postings =
            BlockSegmentPostings::open(doc_freq, postings_data, record_option, requested_option)?;
        let position_reader = positions_data_opt.map(PositionReader::open).transpose()?;
        Ok(SegmentPostings::from_block_postings(
            block_segment_postings,
            position_reader,
        ))
    }

    fn try_accelerated_for_each_pruning(
        mut threshold: Score,
        mut scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(crate::DocId, Score) -> Score,
    ) -> Result<(), Box<dyn Scorer>> {
        scorer = match scorer.downcast::<TermScorer<Self::Postings>>() {
            Ok(term_scorer) => {
                block_wand_single_scorer(*term_scorer, threshold, callback);
                return Ok(());
            }
            Err(scorer) => scorer,
        };
        let mut union_scorer =
            scorer.downcast::<BufferedUnionScorer<Box<dyn Scorer>, SumCombiner>>()?;
        if !union_scorer
            .scorers()
            .iter()
            .all(|scorer| scorer.is::<TermScorer<Self::Postings>>())
        {
            return Err(union_scorer);
        }
        let doc = union_scorer.doc();
        if doc == TERMINATED {
            return Ok(());
        }
        let score = union_scorer.score();
        if score > threshold {
            threshold = callback(doc, score);
        }
        let boxed_scorers: Vec<Box<dyn Scorer>> = union_scorer.into_scorers();
        let scorers: Vec<TermScorer<Self::Postings>> = boxed_scorers
            .into_iter()
            .map(|scorer| {
                *scorer.downcast::<TermScorer<Self::Postings>>().ok().expect(
                    "Downcast failed despite the fact we already checked the type was correct",
                )
            })
            .collect();
        block_wand(scorers, threshold, callback);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use super::*;
    use crate::codec::postings::PostingsSerializer as _;
    use crate::postings::Postings as _;

    fn test_segment_postings_tf_aux(num_docs: u32, include_term_freq: bool) -> SegmentPostings {
        let mut postings_serializer =
            StandardPostingsCodec.new_serializer(1.0f32, IndexRecordOption::WithFreqs, None);
        let mut buffer = Vec::new();
        postings_serializer.new_term(num_docs, include_term_freq);
        for i in 0..num_docs {
            postings_serializer.write_doc(i, 2);
        }
        postings_serializer
            .close_term(num_docs, &mut buffer)
            .unwrap();
        StandardPostingsCodec
            .load_postings(
                num_docs,
                OwnedBytes::new(buffer),
                IndexRecordOption::WithFreqs,
                IndexRecordOption::WithFreqs,
                None,
            )
            .unwrap()
    }

    #[test]
    fn test_segment_postings_small_block_with_and_without_freq() {
        let small_block_without_term_freq = test_segment_postings_tf_aux(1, false);
        assert!(!small_block_without_term_freq.has_freq());
        assert_eq!(small_block_without_term_freq.doc(), 0);
        assert_eq!(small_block_without_term_freq.term_freq(), 1);

        let small_block_with_term_freq = test_segment_postings_tf_aux(1, true);
        assert!(small_block_with_term_freq.has_freq());
        assert_eq!(small_block_with_term_freq.doc(), 0);
        assert_eq!(small_block_with_term_freq.term_freq(), 2);
    }

    #[test]
    fn test_segment_postings_large_block_with_and_without_freq() {
        let large_block_without_term_freq = test_segment_postings_tf_aux(128, false);
        assert!(!large_block_without_term_freq.has_freq());
        assert_eq!(large_block_without_term_freq.doc(), 0);
        assert_eq!(large_block_without_term_freq.term_freq(), 1);

        let large_block_with_term_freq = test_segment_postings_tf_aux(128, true);
        assert!(large_block_with_term_freq.has_freq());
        assert_eq!(large_block_with_term_freq.doc(), 0);
        assert_eq!(large_block_with_term_freq.term_freq(), 2);
    }
}
