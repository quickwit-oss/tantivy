use std::io;

use crate::codec::postings::PostingsCodec;
use crate::codec::standard::postings::block_segment_postings::BlockSegmentPostings;
use crate::codec::standard::postings::segment_postings::SegmentPostings;
use crate::fieldnorm::FieldNormReader;
use crate::positions::PositionReader;
use crate::schema::IndexRecordOption;
use crate::Score;

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
        let record_option = requested_option.downgrade(record_option);
        let block_segment_postings =
            BlockSegmentPostings::open(doc_freq, postings_data, record_option, requested_option)?;
        let position_reader = positions_data_opt.map(PositionReader::open).transpose()?;
        Ok(SegmentPostings::from_block_postings(
            block_segment_postings,
            position_reader,
        ))
    }
}
