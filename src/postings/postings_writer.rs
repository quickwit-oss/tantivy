use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::ops::Range;

use fnv::FnvHashMap;

use super::stacker::Addr;
use crate::fieldnorm::FieldNormReaders;
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::recorder::{BufferLender, Recorder};
use crate::postings::{
    FieldSerializer, IndexingContext, InvertedIndexSerializer, PerFieldPostingsWriter,
    UnorderedTermId,
};
use crate::schema::{Field, FieldType, Schema, Term};
use crate::termdict::TermOrdinal;
use crate::tokenizer::{Token, TokenStream, MAX_TOKEN_LEN};
use crate::DocId;

const POSITION_GAP: u32 = 1;

fn make_field_partition(
    term_offsets: &[(Term<&[u8]>, Addr, UnorderedTermId)],
) -> Vec<(Field, Range<usize>)> {
    let term_offsets_it = term_offsets
        .iter()
        .map(|(term, _, _)| term.field())
        .enumerate();
    let mut prev_field_opt = None;
    let mut fields = vec![];
    let mut offsets = vec![];
    for (offset, field) in term_offsets_it {
        if Some(field) != prev_field_opt {
            prev_field_opt = Some(field);
            fields.push(field);
            offsets.push(offset);
        }
    }
    offsets.push(term_offsets.len());
    let mut field_offsets = vec![];
    for i in 0..fields.len() {
        field_offsets.push((fields[i], offsets[i]..offsets[i + 1]));
    }
    field_offsets
}

/// Serialize the inverted index.
/// It pushes all term, one field at a time, towards the
/// postings serializer.
pub(crate) fn serialize_postings(
    ctx: IndexingContext,
    per_field_postings_writers: &PerFieldPostingsWriter,
    fieldnorm_readers: FieldNormReaders,
    doc_id_map: Option<&DocIdMapping>,
    schema: &Schema,
    serializer: &mut InvertedIndexSerializer,
) -> crate::Result<HashMap<Field, FnvHashMap<UnorderedTermId, TermOrdinal>>> {
    let mut term_offsets: Vec<(Term<&[u8]>, Addr, UnorderedTermId)> =
        Vec::with_capacity(ctx.term_index.len());
    term_offsets.extend(ctx.term_index.iter());
    term_offsets.sort_unstable_by_key(|(k, _, _)| k.clone());
    let mut unordered_term_mappings: HashMap<Field, FnvHashMap<UnorderedTermId, TermOrdinal>> =
        HashMap::new();

    let field_offsets = make_field_partition(&term_offsets);
    for (field, byte_offsets) in field_offsets {
        let field_entry = schema.get_field_entry(field);
        match *field_entry.field_type() {
            FieldType::Str(_) | FieldType::Facet(_) => {
                // populating the (unordered term ord) -> (ordered term ord) mapping
                // for the field.
                let unordered_term_ids = term_offsets[byte_offsets.clone()]
                    .iter()
                    .map(|&(_, _, bucket)| bucket);
                let mapping: FnvHashMap<UnorderedTermId, TermOrdinal> = unordered_term_ids
                    .enumerate()
                    .map(|(term_ord, unord_term_id)| {
                        (unord_term_id as UnorderedTermId, term_ord as TermOrdinal)
                    })
                    .collect();
                unordered_term_mappings.insert(field, mapping);
            }
            FieldType::U64(_) | FieldType::I64(_) | FieldType::F64(_) | FieldType::Date(_) => {}
            FieldType::Bytes(_) => {}
            FieldType::JsonObject(_) => {}
        }

        let postings_writer = per_field_postings_writers.get_for_field(field);
        let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
        let mut field_serializer =
            serializer.new_field(field, postings_writer.total_num_tokens(), fieldnorm_reader)?;
        postings_writer.serialize(
            &term_offsets[byte_offsets],
            doc_id_map,
            &ctx,
            &mut field_serializer,
        )?;
        field_serializer.close()?;
    }
    Ok(unordered_term_mappings)
}

#[derive(Default)]
pub(crate) struct IndexingPosition {
    pub num_tokens: u32,
    pub end_position: u32,
}

/// The `PostingsWriter` is in charge of receiving documenting
/// and building a `Segment` in anonymous memory.
///
/// `PostingsWriter` writes in a `MemoryArena`.
pub(crate) trait PostingsWriter {
    /// Record that a document contains a term at a given position.
    ///
    /// * doc  - the document id
    /// * pos  - the term position (expressed in tokens)
    /// * term - the term
    /// * ctx - Contains a term hashmap and a memory arena to store all necessary posting list
    ///   information.
    fn subscribe(
        &mut self,
        doc: DocId,
        pos: u32,
        term: &Term,
        ctx: &mut IndexingContext,
    ) -> UnorderedTermId;

    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr, UnorderedTermId)],
        doc_id_map: Option<&DocIdMapping>,
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()>;

    /// Tokenize a text and subscribe all of its token.
    fn index_text(
        &mut self,
        doc_id: DocId,
        token_stream: &mut dyn TokenStream,
        term_buffer: &mut Term,
        ctx: &mut IndexingContext,
        indexing_position: &mut IndexingPosition,
    ) {
        let end_of_path_idx = term_buffer.as_slice().len();
        let mut num_tokens = 0;
        let mut end_position = 0;
        token_stream.process(&mut |token: &Token| {
            // We skip all tokens with a len greater than u16.
            if token.text.len() > MAX_TOKEN_LEN {
                warn!(
                    "A token exceeding MAX_TOKEN_LEN ({}>{}) was dropped. Search for \
                     MAX_TOKEN_LEN in the documentation for more information.",
                    token.text.len(),
                    MAX_TOKEN_LEN
                );
                return;
            }
            term_buffer.truncate(end_of_path_idx);
            term_buffer.append_bytes(token.text.as_bytes());
            let start_position = indexing_position.end_position + token.position as u32;
            end_position = start_position + token.position_length as u32;
            self.subscribe(doc_id, start_position, term_buffer, ctx);
            num_tokens += 1;
        });
        indexing_position.end_position = end_position + POSITION_GAP;
        indexing_position.num_tokens += num_tokens;
        term_buffer.truncate(end_of_path_idx);
    }

    fn total_num_tokens(&self) -> u64;
}

/// The `SpecializedPostingsWriter` is just here to remove dynamic
/// dispatch to the recorder information.
#[derive(Default)]
pub(crate) struct SpecializedPostingsWriter<Rec: Recorder> {
    total_num_tokens: u64,
    _recorder_type: PhantomData<Rec>,
}

impl<Rec: Recorder> From<SpecializedPostingsWriter<Rec>> for Box<dyn PostingsWriter> {
    fn from(
        specialized_postings_writer: SpecializedPostingsWriter<Rec>,
    ) -> Box<dyn PostingsWriter> {
        Box::new(specialized_postings_writer)
    }
}

impl<Rec: Recorder> SpecializedPostingsWriter<Rec> {
    #[inline]
    pub(crate) fn serialize_one_term(
        term: &Term<&[u8]>,
        addr: Addr,
        doc_id_map: Option<&DocIdMapping>,
        buffer_lender: &mut BufferLender,
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let recorder: Rec = ctx.term_index.read(addr);
        let term_doc_freq = recorder.term_doc_freq().unwrap_or(0u32);
        serializer.new_term(term.value_bytes(), term_doc_freq)?;
        recorder.serialize(&ctx.arena, doc_id_map, serializer, buffer_lender);
        serializer.close_term()?;
        Ok(())
    }
}

impl<Rec: Recorder> PostingsWriter for SpecializedPostingsWriter<Rec> {
    fn subscribe(
        &mut self,
        doc: DocId,
        position: u32,
        term: &Term,
        ctx: &mut IndexingContext,
    ) -> UnorderedTermId {
        debug_assert!(term.as_slice().len() >= 4);
        self.total_num_tokens += 1;
        let (term_index, arena) = (&mut ctx.term_index, &mut ctx.arena);
        term_index.mutate_or_create(term.as_slice(), |opt_recorder: Option<Rec>| {
            if let Some(mut recorder) = opt_recorder {
                let current_doc = recorder.current_doc();
                if current_doc != doc {
                    recorder.close_doc(arena);
                    recorder.new_doc(doc, arena);
                }
                recorder.record_position(position, arena);
                recorder
            } else {
                let mut recorder = Rec::default();
                recorder.new_doc(doc, arena);
                recorder.record_position(position, arena);
                recorder
            }
        }) as UnorderedTermId
    }

    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr, UnorderedTermId)],
        doc_id_map: Option<&DocIdMapping>,
        ctx: &IndexingContext,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let mut buffer_lender = BufferLender::default();
        for (term, addr, _) in term_addrs {
            Self::serialize_one_term(term, *addr, doc_id_map, &mut buffer_lender, ctx, serializer)?;
        }
        Ok(())
    }

    fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }
}
