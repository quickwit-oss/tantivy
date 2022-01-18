use super::stacker::{Addr, MemoryArena, TermHashMap};

use crate::postings::recorder::{
    BufferLender, NothingRecorder, Recorder, TermFrequencyRecorder, TfAndPositionRecorder,
};
use crate::postings::UnorderedTermId;
use crate::postings::{FieldSerializer, InvertedIndexSerializer};
use crate::schema::{Field, FieldEntry, FieldType, Schema, Term};
use crate::schema::{IndexRecordOption, Type};
use crate::termdict::TermOrdinal;
use crate::tokenizer::TokenStream;
use crate::tokenizer::{Token, MAX_TOKEN_LEN};
use crate::DocId;
use crate::{fieldnorm::FieldNormReaders, indexer::doc_id_mapping::DocIdMapping};
use fnv::FnvHashMap;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::ops::{DerefMut, Range};

fn postings_writer_from_field_entry(field_entry: &FieldEntry) -> Box<dyn PostingsWriter> {
    match *field_entry.field_type() {
        FieldType::Str(ref text_options) => text_options
            .get_indexing_options()
            .map(|indexing_options| match indexing_options.index_option() {
                IndexRecordOption::Basic => {
                    SpecializedPostingsWriter::<NothingRecorder>::new_boxed()
                }
                IndexRecordOption::WithFreqs => {
                    SpecializedPostingsWriter::<TermFrequencyRecorder>::new_boxed()
                }
                IndexRecordOption::WithFreqsAndPositions => {
                    SpecializedPostingsWriter::<TfAndPositionRecorder>::new_boxed()
                }
            })
            .unwrap_or_else(SpecializedPostingsWriter::<NothingRecorder>::new_boxed),
        FieldType::U64(_)
        | FieldType::I64(_)
        | FieldType::F64(_)
        | FieldType::Date(_)
        | FieldType::Bytes(_)
        | FieldType::Facet(_) => SpecializedPostingsWriter::<NothingRecorder>::new_boxed(),
        FieldType::JsonObject(ref json_object_options) => {
            if let Some(text_indexing_option) = json_object_options.get_text_indexing_option() {
                match text_indexing_option.index_option() {
                    IndexRecordOption::Basic => JsonPostingsWriter::<NothingRecorder>::new_boxed(),
                    IndexRecordOption::WithFreqs => {
                        JsonPostingsWriter::<TermFrequencyRecorder>::new_boxed()
                    }
                    IndexRecordOption::WithFreqsAndPositions => {
                        JsonPostingsWriter::<TfAndPositionRecorder>::new_boxed()
                    }
                }
            } else {
                JsonPostingsWriter::<NothingRecorder>::new_boxed()
            }
        }
    }
}

pub struct MultiFieldPostingsWriter {
    heap: MemoryArena,
    schema: Schema,
    term_index: TermHashMap,
    per_field_postings_writers: Vec<Box<dyn PostingsWriter>>,
}

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

impl MultiFieldPostingsWriter {
    /// Create a new `MultiFieldPostingsWriter` given
    /// a schema and a heap.
    pub fn new(schema: &Schema, table_bits: usize) -> MultiFieldPostingsWriter {
        let term_index = TermHashMap::new(table_bits);
        let per_field_postings_writers: Vec<_> = schema
            .fields()
            .map(|(_, field_entry)| postings_writer_from_field_entry(field_entry))
            .collect();
        MultiFieldPostingsWriter {
            heap: MemoryArena::new(),
            schema: schema.clone(),
            term_index,
            per_field_postings_writers,
        }
    }

    pub fn mem_usage(&self) -> usize {
        self.term_index.mem_usage() + self.heap.mem_usage()
    }

    pub fn index_text(
        &mut self,
        doc: DocId,
        field: Field,
        token_stream: &mut dyn TokenStream,
        term_buffer: &mut Term,
    ) -> u32 {
        let postings_writer =
            self.per_field_postings_writers[field.field_id() as usize].deref_mut();
        postings_writer.index_text(
            doc,
            field,
            token_stream,
            &mut self.term_index,
            &mut self.heap,
            term_buffer,
        )
    }

    pub fn subscribe(&mut self, doc: DocId, term: &Term) -> UnorderedTermId {
        let postings_writer =
            self.per_field_postings_writers[term.field().field_id() as usize].deref_mut();
        postings_writer.subscribe(doc, 0u32, term, &mut self.term_index, &mut self.heap)
    }

    /// Serialize the inverted index.
    /// It pushes all term, one field at a time, towards the
    /// postings serializer.
    pub fn serialize(
        &self,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_readers: FieldNormReaders,
        doc_id_map: Option<&DocIdMapping>,
    ) -> crate::Result<HashMap<Field, FnvHashMap<UnorderedTermId, TermOrdinal>>> {
        let mut term_offsets: Vec<(Term<&[u8]>, Addr, UnorderedTermId)> =
            Vec::with_capacity(self.term_index.len());
        term_offsets.extend(self.term_index.iter());
        term_offsets.sort_unstable_by_key(|(k, _, _)| k.clone());

        let mut unordered_term_mappings: HashMap<Field, FnvHashMap<UnorderedTermId, TermOrdinal>> =
            HashMap::new();

        let field_offsets = make_field_partition(&term_offsets);

        for (field, byte_offsets) in field_offsets {
            let field_entry = self.schema.get_field_entry(field);

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

            let postings_writer =
                self.per_field_postings_writers[field.field_id() as usize].as_ref();
            let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
            let mut field_serializer = serializer.new_field(
                field,
                postings_writer.total_num_tokens(),
                fieldnorm_reader,
            )?;
            postings_writer.serialize(
                &term_offsets[byte_offsets],
                &self.term_index.heap,
                &self.heap,
                doc_id_map,
                &mut field_serializer,
            )?;
            field_serializer.close()?;
        }
        Ok(unordered_term_mappings)
    }
}

/// The `PostingsWriter` is in charge of receiving documenting
/// and building a `Segment` in anonymous memory.
///
/// `PostingsWriter` writes in a `MemoryArena`.
pub trait PostingsWriter {
    /// Record that a document contains a term at a given position.
    ///
    /// * doc  - the document id
    /// * pos  - the term position (expressed in tokens)
    /// * term - the term
    /// * heap - heap used to store the postings informations as well as the terms
    /// in the hashmap.
    fn subscribe(
        &mut self,
        doc: DocId,
        pos: u32,
        term: &Term,
        term_index: &mut TermHashMap,
        heap: &mut MemoryArena,
    ) -> UnorderedTermId;

    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr, UnorderedTermId)],
        term_heap: &MemoryArena,
        heap: &MemoryArena,
        doc_id_map: Option<&DocIdMapping>,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()>;

    /// Tokenize a text and subscribe all of its token.
    fn index_text(
        &mut self,
        doc_id: DocId,
        field: Field,
        token_stream: &mut dyn TokenStream,
        term_index: &mut TermHashMap,
        heap: &mut MemoryArena,
        term_buffer: &mut Term,
    ) -> u32 {
        term_buffer.set_field(Type::Str, field);
        let mut sink = |token: &Token| {
            // We skip all tokens with a len greater than u16.
            if token.text.len() <= MAX_TOKEN_LEN {
                term_buffer.set_text(token.text.as_str());
                self.subscribe(doc_id, token.position as u32, term_buffer, term_index, heap);
            } else {
                warn!(
                    "A token exceeding MAX_TOKEN_LEN ({}>{}) was dropped. Search for \
                     MAX_TOKEN_LEN in the documentation for more information.",
                    token.text.len(),
                    MAX_TOKEN_LEN
                );
            }
        };
        token_stream.process(&mut sink)
    }

    fn total_num_tokens(&self) -> u64;
}

pub(crate) struct JsonPostingsWriter<Rec: Recorder> {
    text_postings_writer: SpecializedPostingsWriter<Rec>,
    other_postings_writer: SpecializedPostingsWriter<NothingRecorder>,
}

impl<Rec: Recorder> JsonPostingsWriter<Rec> {
    pub fn new_boxed() -> Box<dyn PostingsWriter> {
        let text_postings_writer: SpecializedPostingsWriter<Rec> = SpecializedPostingsWriter {
            total_num_tokens: 0u64,
            _recorder_type: PhantomData,
        };
        let other_postings_writer: SpecializedPostingsWriter<NothingRecorder> =
            SpecializedPostingsWriter {
                total_num_tokens: 0u64,
                _recorder_type: PhantomData,
            };
        Box::new(JsonPostingsWriter {
            text_postings_writer,
            other_postings_writer,
        })
    }
}

impl<Rec: Recorder> PostingsWriter for JsonPostingsWriter<Rec> {
    fn subscribe(
        &mut self,
        doc: DocId,
        pos: u32,
        term: &Term,
        term_index: &mut TermHashMap,
        heap: &mut MemoryArena,
    ) -> UnorderedTermId {
        // TODO will the unordered term id be correct!?
        debug_assert!(term.is_json());
        if term.typ() == Type::Str {
            self.text_postings_writer
                .subscribe(doc, pos, term, term_index, heap)
        } else {
            self.other_postings_writer
                .subscribe(doc, pos, term, term_index, heap)
        }
    }

    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr, UnorderedTermId)],
        term_heap: &MemoryArena,
        heap: &MemoryArena,
        doc_id_map: Option<&DocIdMapping>,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let mut buffer_lender = BufferLender::default();
        for (term, addr, _) in term_addrs {
            if term.typ() == Type::Str {
                SpecializedPostingsWriter::<Rec>::serialize_one_term(
                    term,
                    *addr,
                    term_heap,
                    heap,
                    doc_id_map,
                    &mut buffer_lender,
                    serializer,
                )?;
            } else {
                SpecializedPostingsWriter::<NothingRecorder>::serialize_one_term(
                    term,
                    *addr,
                    term_heap,
                    heap,
                    doc_id_map,
                    &mut buffer_lender,
                    serializer,
                )?;
            }
        }
        Ok(())
    }

    fn total_num_tokens(&self) -> u64 {
        self.text_postings_writer.total_num_tokens() + self.other_postings_writer.total_num_tokens()
    }
}

/// The `SpecializedPostingsWriter` is just here to remove dynamic
/// dispatch to the recorder information.
#[derive(Default)]
pub(crate) struct SpecializedPostingsWriter<Rec: Recorder> {
    total_num_tokens: u64,
    _recorder_type: PhantomData<Rec>,
}

impl<Rec: Recorder> SpecializedPostingsWriter<Rec> {
    pub fn new_boxed() -> Box<dyn PostingsWriter> {
        let new_specialized_posting_writer: Self = Self {
            total_num_tokens: 0u64,
            _recorder_type: PhantomData,
        };
        Box::new(new_specialized_posting_writer)
    }

    #[inline]
    fn serialize_one_term(
        term: &Term<&[u8]>,
        addr: Addr,
        termdict_heap: &MemoryArena,
        heap: &MemoryArena,
        doc_id_map: Option<&DocIdMapping>,
        buffer_lender: &mut BufferLender,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let recorder: Rec = termdict_heap.read(addr);
        let term_doc_freq = recorder.term_doc_freq().unwrap_or(0u32);
        serializer.new_term(term.value_bytes(), term_doc_freq)?;
        recorder.serialize(buffer_lender, heap, doc_id_map, serializer);
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
        term_index: &mut TermHashMap,
        heap: &mut MemoryArena,
    ) -> UnorderedTermId {
        debug_assert!(term.as_slice().len() >= 4);
        self.total_num_tokens += 1;
        term_index.mutate_or_create(term.as_slice(), |opt_recorder: Option<Rec>| {
            if let Some(mut recorder) = opt_recorder {
                let current_doc = recorder.current_doc();
                if current_doc != doc {
                    recorder.close_doc(heap);
                    recorder.new_doc(doc, heap);
                }
                recorder.record_position(position, heap);
                recorder
            } else {
                let mut recorder = Rec::new();
                recorder.new_doc(doc, heap);
                recorder.record_position(position, heap);
                recorder
            }
        }) as UnorderedTermId
    }

    fn serialize(
        &self,
        term_addrs: &[(Term<&[u8]>, Addr, UnorderedTermId)],
        termdict_heap: &MemoryArena,
        heap: &MemoryArena,
        doc_id_map: Option<&DocIdMapping>,
        serializer: &mut FieldSerializer,
    ) -> io::Result<()> {
        let mut buffer_lender = BufferLender::default();
        for (term, addr, _) in term_addrs {
            Self::serialize_one_term(
                term,
                *addr,
                termdict_heap,
                heap,
                doc_id_map,
                &mut buffer_lender,
                serializer,
            )?;
        }
        Ok(())
    }

    fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }
}
