use std::io;

use common::OwnedBytes;

use crate::fieldnorm::FieldNormReader;
use crate::postings::{FreqReadingOption, Postings};
use crate::query::Bm25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score};

pub trait PostingsCodec: Send + Sync + 'static {
    type PostingsSerializer: PostingsSerializer;
    type Postings: Postings;

    fn new_serializer(
        &self,
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> Self::PostingsSerializer;

    fn load_postings(
        &self,
        doc_freq: u32,
        postings_data: OwnedBytes,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
        positions_data: Option<OwnedBytes>,
    ) -> io::Result<Self::Postings>;
}

pub trait PostingsSerializer {
    fn new_term(&mut self, term_doc_freq: u32, record_term_freq: bool);

    fn write_doc(&mut self, doc_id: DocId, term_freq: u32);

    fn close_term(&mut self, doc_freq: u32, wrt: &mut impl io::Write) -> io::Result<()>;

    fn clear(&mut self);
}
