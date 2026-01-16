use std::io;

use crate::codec::postings::PostingsCodec;
use crate::fieldnorm::FieldNormReader;
use crate::positions::PositionReader;
use crate::postings::SegmentPostings;
use crate::schema::IndexRecordOption;
use crate::Score;

mod block;
mod skip;
mod standard_postings_reader;
mod standard_postings_serializer;

pub use standard_postings_reader::StandardPostingsReader;
pub use standard_postings_serializer::StandardPostingsSerializer;

pub struct StandardPostingsCodec;

impl PostingsCodec for StandardPostingsCodec {
    type PostingsSerializer = StandardPostingsSerializer;
    type PostingsReader = StandardPostingsReader;
    type Postings = SegmentPostings;

    fn new_serializer(
        &self,
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> Self::PostingsSerializer {
        StandardPostingsSerializer::new(avg_fieldnorm, mode, fieldnorm_reader)
    }

    fn open(
        doc_freq: u32,
        data: common::OwnedBytes,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> std::io::Result<Self::PostingsReader> {
        StandardPostingsReader::open(doc_freq, data, record_option, requested_option)
    }

    fn load_postings(&self,
                doc_freq: u32,
                postings_data: common::OwnedBytes,
                record_option: IndexRecordOption,
                requested_option: IndexRecordOption,
                positions_data_opt: Option<common::OwnedBytes>) -> io::Result<Self::Postings> {
        // Rationalize record_option/requested_option.
        let record_option = requested_option.downgrade(record_option);
        let block_segment_postings = StandardPostingsReader::open(
            doc_freq,
            postings_data,
            record_option,
            requested_option,
        )?;
        let position_reader =
            positions_data_opt.map(PositionReader::open).transpose()?;
        Ok(SegmentPostings::from_block_postings(
            block_segment_postings,
            position_reader,
        ))
    }
}
