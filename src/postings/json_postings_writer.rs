use std::io::Read;

use regex::internal::Input;

use crate::postings::postings_writer::PostingsWriter;

use super::stacker::TermHashMap;

pub struct JsonPostingsWriter {
    str_posting_writer: Box<dyn PostingsWriter>,
    non_str_posting_writer: Box<dyn PostingsWriter>,
}

impl PostingsWriter for JsonPostingWriter {
    fn subscribe(
        &mut self,
        term_index: &mut TermHashMap,
        doc: crate::DocId,
        pos: u32,
        term: &crate::Term,
        heap: &mut super::stacker::MemoryArena,
    ) -> super::UnorderedTermId {
        let term_type = term.typ();
    }

    fn serialize(
        &self,
        term_addrs: &[(&[u8], super::stacker::Addr, super::UnorderedTermId)],
        serializer: &mut super::FieldSerializer<'_>,
        term_heap: &super::stacker::MemoryArena,
        heap: &super::stacker::MemoryArena,
        doc_id_map: Option<&crate::indexer::doc_id_mapping::DocIdMapping>,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn total_num_tokens(&self) -> u64 {
        todo!()
    }
}


