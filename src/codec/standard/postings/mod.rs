use std::io;

use common::BitSet;

use crate::codec::postings::block_wand::{block_wand, block_wand_single_scorer};
use crate::codec::postings::{PostingsCodec, RawPostingsData};
use crate::codec::standard::postings::block_segment_postings::BlockSegmentPostings;
pub use crate::codec::standard::postings::segment_postings::SegmentPostings;
use crate::positions::PositionReader;
use crate::query::term_query::TermScorer;
use crate::query::{BufferedUnionScorer, Scorer, SumCombiner};
use crate::{DocSet as _, Score, TERMINATED};

mod block_segment_postings;
mod segment_postings;

pub use segment_postings::SegmentPostings as StandardPostings;

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
    type Postings = SegmentPostings;

    fn load_postings(
        &self,
        doc_freq: u32,
        postings_data: RawPostingsData,
    ) -> io::Result<Self::Postings> {
        load_postings_from_raw_data(doc_freq, postings_data)
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
            scorer.downcast::<BufferedUnionScorer<TermScorer<Self::Postings>, SumCombiner>>()?;
        let doc = union_scorer.doc();
        if doc == TERMINATED {
            return Ok(());
        }
        let score = union_scorer.score();
        if score > threshold {
            threshold = callback(doc, score);
        }
        let scorers: Vec<TermScorer<Self::Postings>> = union_scorer.into_scorers();
        block_wand(scorers, threshold, callback);
        Ok(())
    }
}
pub(crate) fn load_postings_from_raw_data(
    doc_freq: u32,
    postings_data: RawPostingsData,
) -> io::Result<SegmentPostings> {
    let RawPostingsData {
        postings_data,
        positions_data: positions_data_opt,
        record_option,
        effective_option,
    } = postings_data;
    let requested_option = effective_option;
    let block_segment_postings =
        BlockSegmentPostings::open(doc_freq, postings_data, record_option, requested_option)?;
    let position_reader = positions_data_opt.map(PositionReader::open).transpose()?;
    Ok(SegmentPostings::from_block_postings(
        block_segment_postings,
        position_reader,
    ))
}

pub(crate) fn fill_bitset_from_raw_data(
    doc_freq: u32,
    postings_data: RawPostingsData,
    doc_bitset: &mut BitSet,
) -> io::Result<()> {
    let RawPostingsData {
        postings_data,
        record_option,
        effective_option,
        ..
    } = postings_data;
    let mut block_postings =
        BlockSegmentPostings::open(doc_freq, postings_data, record_option, effective_option)?;
    loop {
        let docs = block_postings.docs();
        if docs.is_empty() {
            break;
        }
        for &doc in docs {
            doc_bitset.insert(doc);
        }
        block_postings.advance();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use super::*;
    use crate::postings::serializer::PostingsSerializer;
    use crate::postings::Postings as _;
    use crate::schema::IndexRecordOption;

    fn test_segment_postings_tf_aux(num_docs: u32, include_term_freq: bool) -> SegmentPostings {
        let mut postings_serializer =
            PostingsSerializer::new(1.0f32, IndexRecordOption::WithFreqs, None);
        let mut buffer = Vec::new();
        postings_serializer.new_term(num_docs, include_term_freq);
        for i in 0..num_docs {
            postings_serializer.write_doc(i, 2);
        }
        postings_serializer
            .close_term(num_docs, &mut buffer)
            .unwrap();
        load_postings_from_raw_data(
            num_docs,
            RawPostingsData {
                postings_data: OwnedBytes::new(buffer),
                positions_data: None,
                record_option: IndexRecordOption::WithFreqs,
                effective_option: IndexRecordOption::WithFreqs,
            },
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
