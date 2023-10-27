use std::io;

use stacker::Addr;

use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::postings_writer::SpecializedPostingsWriter;
use crate::postings::recorder::{BufferLender, DocIdRecorder, Recorder};
use crate::postings::{FieldSerializer, IndexingContext, IndexingPosition, PostingsWriter};
use crate::schema::{Field, Type, JSON_END_OF_PATH};
use crate::tokenizer::TokenStream;
use crate::{DocId, Term};

#[derive(Default)]
pub(crate) struct JsonPostingsWriter<Rec: Recorder> {
    str_posting_writer: SpecializedPostingsWriter<Rec>,
    non_str_posting_writer: SpecializedPostingsWriter<DocIdRecorder>,
}

impl<Rec: Recorder> From<JsonPostingsWriter<Rec>> for Box<dyn PostingsWriter> {
    fn from(json_postings_writer: JsonPostingsWriter<Rec>) -> Box<dyn PostingsWriter> {
        Box::new(json_postings_writer)
    }
}

impl<Rec: Recorder> PostingsWriter for JsonPostingsWriter<Rec> {
    #[inline]
    fn subscribe(
        &mut self,
        doc: crate::DocId,
        pos: u32,
        term: &crate::Term,
        ctx: &mut IndexingContext,
    ) {
        self.non_str_posting_writer.subscribe(doc, pos, term, ctx);
    }

    fn index_text(
        &mut self,
        doc_id: DocId,
        token_stream: &mut dyn TokenStream,
        term_buffer: &mut Term,
        ctx: &mut IndexingContext,
        indexing_position: &mut IndexingPosition,
    ) {
        self.str_posting_writer.index_text(
            doc_id,
            token_stream,
            term_buffer,
            ctx,
            indexing_position,
        );
    }

    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr)],
        ordered_id_to_path: &[&String],
        doc_id_map: Option<&DocIdMapping>,
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let mut term_buffer = Term::with_capacity(48);
        let mut buffer_lender = BufferLender::default();
        for (term, addr) in term_addrs {
            let ordered_id_bytes: [u8; 4] = term.serialized_value_bytes()[..4].try_into().unwrap();
            let ordered_id = u32::from_be_bytes(ordered_id_bytes);
            term_buffer.clear_with_field_and_type(Type::Json, Field::from_field_id(0));
            term_buffer.append_bytes(ordered_id_to_path[ordered_id as usize].as_bytes());
            term_buffer.append_bytes(&[JSON_END_OF_PATH]);
            term_buffer.append_bytes(&term.serialized_value_bytes()[4..]);
            if let Some(json_value) = term_buffer.value().as_json_value_bytes() {
                let typ = json_value.typ();
                if typ == Type::Str {
                    SpecializedPostingsWriter::<Rec>::serialize_one_term(
                        term_buffer.serialized_value_bytes(),
                        *addr,
                        doc_id_map,
                        &mut buffer_lender,
                        ctx,
                        serializer,
                    )?;
                } else {
                    SpecializedPostingsWriter::<DocIdRecorder>::serialize_one_term(
                        term_buffer.serialized_value_bytes(),
                        *addr,
                        doc_id_map,
                        &mut buffer_lender,
                        ctx,
                        serializer,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn total_num_tokens(&self) -> u64 {
        self.str_posting_writer.total_num_tokens() + self.non_str_posting_writer.total_num_tokens()
    }
}
