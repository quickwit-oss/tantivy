use super::stacker::{Addr, MemoryArena, TermHashMap};

use postings::recorder::{NothingRecorder, Recorder, TFAndPositionRecorder, TermFrequencyRecorder};
use postings::UnorderedTermId;
use postings::{FieldSerializer, InvertedIndexSerializer};
use schema::IndexRecordOption;
use schema::{Field, FieldEntry, FieldType, Schema, Term};
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::ops::DerefMut;
use termdict::TermOrdinal;
use tokenizer::Token;
use tokenizer::TokenStream;
use DocId;
use Result;

fn posting_from_field_entry<'a>(field_entry: &FieldEntry) -> Box<PostingsWriter> {
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
                    SpecializedPostingsWriter::<TFAndPositionRecorder>::new_boxed()
                }
            })
            .unwrap_or_else(|| SpecializedPostingsWriter::<NothingRecorder>::new_boxed()),
        FieldType::U64(_) | FieldType::I64(_) | FieldType::HierarchicalFacet => {
            SpecializedPostingsWriter::<NothingRecorder>::new_boxed()
        }
        FieldType::Bytes => {
            // FieldType::Bytes cannot actually be indexed.
            // TODO fix during the indexer refactoring described in #276
            SpecializedPostingsWriter::<NothingRecorder>::new_boxed()
        }
    }
}

pub struct MultiFieldPostingsWriter {
    heap: MemoryArena,
    schema: Schema,
    term_index: TermHashMap,
    per_field_postings_writers: Vec<Box<PostingsWriter>>,
}

impl MultiFieldPostingsWriter {
    /// Create a new `MultiFieldPostingsWriter` given
    /// a schema and a heap.
    pub fn new(schema: &Schema, table_bits: usize) -> MultiFieldPostingsWriter {
        let term_index = TermHashMap::new(table_bits);
        let per_field_postings_writers: Vec<_> = schema
            .fields()
            .iter()
            .map(|field_entry| posting_from_field_entry(field_entry))
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

    pub fn index_text(&mut self, doc: DocId, field: Field, token_stream: &mut TokenStream) -> u32 {
        let postings_writer = self.per_field_postings_writers[field.0 as usize].deref_mut();
        postings_writer.index_text(
            &mut self.term_index,
            doc,
            field,
            token_stream,
            &mut self.heap,
        )
    }

    pub fn subscribe(&mut self, doc: DocId, term: &Term) -> UnorderedTermId {
        let postings_writer = self.per_field_postings_writers[term.field().0 as usize].deref_mut();
        postings_writer.subscribe(&mut self.term_index, doc, 0u32, term, &mut self.heap)
    }

    /// Serialize the inverted index.
    /// It pushes all term, one field at a time, towards the
    /// postings serializer.
    pub fn serialize(
        &self,
        serializer: &mut InvertedIndexSerializer,
    ) -> Result<HashMap<Field, HashMap<UnorderedTermId, TermOrdinal>>> {
        let mut term_offsets: Vec<(&[u8], Addr, UnorderedTermId)> = self
            .term_index
            .iter()
            .map(|(term_bytes, addr, bucket_id)| (term_bytes, addr, bucket_id as UnorderedTermId))
            .collect();
        term_offsets.sort_by_key(|&(k, _, _)| k);

        let mut offsets: Vec<(Field, usize)> = vec![];
        let term_offsets_it = term_offsets
            .iter()
            .cloned()
            .map(|(key, _, _)| Term::wrap(key).field())
            .enumerate();

        let mut unordered_term_mappings: HashMap<
            Field,
            HashMap<UnorderedTermId, TermOrdinal>,
        > = HashMap::new();

        let mut prev_field = Field(u32::max_value());
        for (offset, field) in term_offsets_it {
            if field != prev_field {
                offsets.push((field, offset));
                prev_field = field;
            }
        }
        offsets.push((Field(0), term_offsets.len()));

        for i in 0..(offsets.len() - 1) {
            let (field, start) = offsets[i];
            let (_, stop) = offsets[i + 1];

            let field_entry = self.schema.get_field_entry(field);

            match field_entry.field_type() {
                &FieldType::Str(_) | &FieldType::HierarchicalFacet => {
                    // populating the (unordered term ord) -> (ordered term ord) mapping
                    // for the field.
                    let mut unordered_term_ids = term_offsets[start..stop]
                        .iter()
                        .map(|&(_, _, bucket)| bucket);
                    let mut mapping: HashMap<UnorderedTermId, TermOrdinal> = unordered_term_ids
                        .enumerate()
                        .map(|(term_ord, unord_term_id)| {
                            (unord_term_id as UnorderedTermId, term_ord as TermOrdinal)
                        })
                        .collect();
                    unordered_term_mappings.insert(field, mapping);
                }
                &FieldType::U64(_) | &FieldType::I64(_) => {}
                &FieldType::Bytes => {}
            }

            let postings_writer = &self.per_field_postings_writers[field.0 as usize];
            let mut field_serializer =
                serializer.new_field(field, postings_writer.total_num_tokens())?;
            postings_writer.serialize(
                &term_offsets[start..stop],
                &mut field_serializer,
                &self.term_index.heap,
                &self.heap,
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
        term_index: &mut TermHashMap,
        doc: DocId,
        pos: u32,
        term: &Term,
        heap: &mut MemoryArena,
    ) -> UnorderedTermId;

    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(&[u8], Addr, UnorderedTermId)],
        serializer: &mut FieldSerializer,
        term_heap: &MemoryArena,
        heap: &MemoryArena,
    ) -> io::Result<()>;

    /// Tokenize a text and subscribe all of its token.
    fn index_text(
        &mut self,
        term_index: &mut TermHashMap,
        doc_id: DocId,
        field: Field,
        token_stream: &mut TokenStream,
        heap: &mut MemoryArena,
    ) -> u32 {
        let mut term = Term::for_field(field);
        let num_tokens = {
            let mut sink = |token: &Token| {
                term.set_text(token.text.as_str());
                self.subscribe(term_index, doc_id, token.position as u32, &term, heap);
            };
            token_stream.process(&mut sink)
        };
        num_tokens
    }

    fn total_num_tokens(&self) -> u64;
}

/// The `SpecializedPostingsWriter` is just here to remove dynamic
/// dispatch to the recorder information.
pub struct SpecializedPostingsWriter<Rec: Recorder + 'static> {
    total_num_tokens: u64,
    _recorder_type: PhantomData<Rec>,
}

impl<Rec: Recorder + 'static> SpecializedPostingsWriter<Rec> {
    /// constructor
    pub fn new() -> SpecializedPostingsWriter<Rec> {
        SpecializedPostingsWriter {
            total_num_tokens: 0u64,
            _recorder_type: PhantomData,
        }
    }

    /// Builds a `SpecializedPostingsWriter` storing its data in a heap.
    pub fn new_boxed() -> Box<PostingsWriter> {
        Box::new(SpecializedPostingsWriter::<Rec>::new())
    }
}

impl<Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<Rec> {
    fn subscribe(
        &mut self,
        term_index: &mut TermHashMap,
        doc: DocId,
        position: u32,
        term: &Term,
        heap: &mut MemoryArena,
    ) -> UnorderedTermId {
        debug_assert!(term.as_slice().len() >= 4);
        self.total_num_tokens += 1;
        term_index.mutate_or_create(term, |opt_recorder: Option<Rec>| {
            if opt_recorder.is_some() {
                let mut recorder = opt_recorder.unwrap();
                let current_doc = recorder.current_doc();
                if current_doc != doc {
                    recorder.close_doc(heap);
                    recorder.new_doc(doc, heap);
                }
                recorder.record_position(position, heap);
                recorder
            } else {
                let mut recorder = Rec::new(heap);
                recorder.new_doc(doc, heap);
                recorder.record_position(position, heap);
                recorder
            }
        }) as UnorderedTermId
    }

    fn serialize(
        &self,
        term_addrs: &[(&[u8], Addr, UnorderedTermId)],
        serializer: &mut FieldSerializer,
        termdict_heap: &MemoryArena,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        for &(term_bytes, addr, _) in term_addrs {
            let recorder: Rec = unsafe { termdict_heap.read(addr) };
            serializer.new_term(&term_bytes[4..])?;
            recorder.serialize(serializer, heap)?;
            serializer.close_term()?;
        }
        Ok(())
    }

    fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }
}
