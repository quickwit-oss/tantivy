use std::io;
use crate::Term;
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::postings_writer::SpecializedPostingsWriter;
use crate::postings::recorder::{Recorder, NothingRecorder};
use crate::postings::stacker::Addr;
use crate::postings::{PostingsWriter, IndexingContext, UnorderedTermId, FieldSerializer};

pub struct JsonPostingsWriter {
    str_posting_writer: Box<dyn PostingsWriter>,
    non_str_posting_writer: Box<dyn PostingsWriter>,
}

impl JsonPostingsWriter {
    pub(crate) fn new<R: Recorder>() -> Self {
        JsonPostingsWriter {
            str_posting_writer: SpecializedPostingsWriter::<R>::new_boxed(),
            non_str_posting_writer: SpecializedPostingsWriter::<NothingRecorder>::new_boxed(),
        }
    }

}

impl PostingsWriter for JsonPostingsWriter {
    fn subscribe(
        &mut self,
        doc: crate::DocId,
        pos: u32,
        term: &crate::Term,
        ctx: &mut IndexingContext,
    ) -> UnorderedTermId {
        let term_type = term.typ();
        todo!()
    }

    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr, UnorderedTermId)],
        doc_id_map: Option<&DocIdMapping>,
        indexing_context: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
       todo!()
    }

    fn total_num_tokens(&self) -> u64 {
        todo!()
    }
}


