use std::io;

use crate::codec::postings::block_wand::block_wand;
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
        scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(crate::DocId, Score) -> Score,
    ) -> Result<(), Box<dyn Scorer>> {
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
