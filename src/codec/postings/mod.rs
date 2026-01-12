use std::io;

use crate::fieldnorm::FieldNormReader;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score};

pub trait PostingsCodec {
    type PostingsSerializer: PostingsSerializer;
}

pub trait PostingsSerializer {
    fn new(
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> Self;

    fn new_term(&mut self, term_doc_freq: u32, record_term_freq: bool);

    fn write_doc(&mut self, doc_id: DocId, term_freq: u32);

    fn close_term(&mut self, doc_freq: u32, wrt: &mut impl io::Write) -> io::Result<()>;
}
