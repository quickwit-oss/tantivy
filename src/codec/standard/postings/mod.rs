use crate::codec::postings::PostingsCodec;
use crate::fieldnorm::FieldNormReader;
use crate::schema::IndexRecordOption;
use crate::Score;

mod block;
mod standard_postings_reader;
mod standard_postings_serializer;
mod skip;

pub use standard_postings_reader::StandardPostingsReader;
pub use standard_postings_serializer::StandardPostingsSerializer;

pub struct StandardPostingsCodec;

impl PostingsCodec for StandardPostingsCodec {
    type PostingsSerializer = StandardPostingsSerializer;
    type PostingsReader = StandardPostingsReader;

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
}
