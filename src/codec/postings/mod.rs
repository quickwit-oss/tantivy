use std::io;

use common::OwnedBytes;

use crate::fieldnorm::FieldNormReader;
use crate::postings::FreqReadingOption;
use crate::query::Bm25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score};

pub trait PostingsCodec {
    type PostingsSerializer: PostingsSerializer;
    type PostingsReader: PostingsReader;

    fn new_serializer(
        &self,
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> Self::PostingsSerializer;

    /// Opens a `BlockSegmentPostings`.
    /// `doc_freq` is the number of documents in the posting list.
    /// `record_option` represents the amount of data available according to the schema.
    /// `requested_option` is the amount of data requested by the user.
    /// If for instance, we do not request for term frequencies, this function will not decompress
    /// term frequency blocks.
    // TODO simplify prototype (record_option + requested_option)
    fn open(
        doc_freq: u32,
        data: common::OwnedBytes,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> std::io::Result<Self::PostingsReader>;
}

pub trait PostingsSerializer {
    fn new_term(&mut self, term_doc_freq: u32, record_term_freq: bool);

    fn write_doc(&mut self, doc_id: DocId, term_freq: u32);

    fn close_term(&mut self, doc_freq: u32, wrt: &mut impl io::Write) -> io::Result<()>;

    fn clear(&mut self);
}

// TODO docs
// TODO Add blockwand trait
pub trait PostingsReader: Sized {
    fn freq_reading_option(&self) -> FreqReadingOption;

    fn reset(&mut self, doc_freq: u32, postings_data: OwnedBytes) -> io::Result<()>;

    fn doc_freq(&self) -> u32;

    fn docs(&self) -> &[DocId];

    fn doc(&self, idx: usize) -> u32;

    fn freqs(&self) -> &[u32];

    fn freq(&self, idx: usize) -> u32;

    fn block_len(&self) -> usize;

    fn seek(&mut self, target_doc: DocId) -> usize;

    fn position_offset(&self) -> u64;

    fn advance(&mut self);

    // TODO Move to the codec and use the serializer.
    fn empty() -> Self;

    fn block_max_score(
        &mut self,
        fieldnorm_reader: &FieldNormReader,
        bm25_weight: &Bm25Weight,
    ) -> Score;
}
