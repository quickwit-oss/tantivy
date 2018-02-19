use DocId;
use schema::Term;
use postings::{FieldSerializer, InvertedIndexSerializer};
use std::io;
use std::collections::HashMap;
use postings::Recorder;
use Result;
use schema::{Field, Schema};
use std::marker::PhantomData;
use std::ops::DerefMut;
use datastruct::stacker::{Heap, TermHashMap};
use postings::{NothingRecorder, TFAndPositionRecorder, TermFrequencyRecorder};
use schema::FieldEntry;
use schema::FieldType;
use tokenizer::Token;
use tokenizer::TokenStream;
use schema::IndexRecordOption;
use postings::UnorderedTermId;

fn posting_from_field_entry<'a>(
    field_entry: &FieldEntry,
    heap: &'a Heap,
) -> Box<PostingsWriter + 'a> {
    match *field_entry.field_type() {
        FieldType::Str(ref text_options) => text_options
            .get_indexing_options()
            .map(|indexing_options| match indexing_options.index_option() {
                IndexRecordOption::Basic => {
                    SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap)
                }
                IndexRecordOption::WithFreqs => {
                    SpecializedPostingsWriter::<TermFrequencyRecorder>::new_boxed(heap)
                }
                IndexRecordOption::WithFreqsAndPositions => {
                    SpecializedPostingsWriter::<TFAndPositionRecorder>::new_boxed(heap)
                }
            })
            .unwrap_or_else(|| SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap)),
        FieldType::U64(_) | FieldType::I64(_) | FieldType::HierarchicalFacet => {
            SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap)
        }
    }
}

pub struct MultiFieldPostingsWriter<'a> {
    heap: &'a Heap,
    term_index: TermHashMap<'a>,
    per_field_postings_writers: Vec<Box<PostingsWriter + 'a>>,
}

impl<'a> MultiFieldPostingsWriter<'a> {
    /// Create a new `MultiFieldPostingsWriter` given
    /// a schema and a heap.
    pub fn new(schema: &Schema, table_bits: usize, heap: &'a Heap) -> MultiFieldPostingsWriter<'a> {
        let term_index = TermHashMap::new(table_bits, heap);
        let per_field_postings_writers: Vec<_> = schema
            .fields()
            .iter()
            .map(|field_entry| posting_from_field_entry(field_entry, heap))
            .collect();

        MultiFieldPostingsWriter {
            heap,
            term_index,
            per_field_postings_writers,
        }
    }

    pub fn index_text(&mut self, doc: DocId, field: Field, token_stream: &mut TokenStream) -> u32 {
        let postings_writer = self.per_field_postings_writers[field.0 as usize].deref_mut();
        postings_writer.index_text(&mut self.term_index, doc, field, token_stream, self.heap)
    }

    pub fn subscribe(&mut self, doc: DocId, term: &Term) -> UnorderedTermId {
        let postings_writer = self.per_field_postings_writers[term.field().0 as usize].deref_mut();
        postings_writer.subscribe(&mut self.term_index, doc, 0u32, term, self.heap)
    }

    /// Serialize the inverted index.
    /// It pushes all term, one field at a time, towards the
    /// postings serializer.
    #[allow(needless_range_loop)]
    pub fn serialize(
        &self,
        serializer: &mut InvertedIndexSerializer,
    ) -> Result<HashMap<Field, HashMap<UnorderedTermId, usize>>> {
        let mut term_offsets: Vec<(&[u8], u32, UnorderedTermId)> = self.term_index.iter().collect();
        term_offsets.sort_by_key(|&(k, _, _)| k);

        let mut offsets: Vec<(Field, usize)> = vec![];
        let term_offsets_it = term_offsets
            .iter()
            .cloned()
            .map(|(key, _, _)| Term::wrap(key).field())
            .enumerate();

        let mut unordered_term_mappings: HashMap<Field, HashMap<UnorderedTermId, usize>> =
            HashMap::new();

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

            // populating the unordered term ord -> ordered term ord mapping
            // for the field.
            let mut mapping = HashMap::new();
            for (term_ord, term_unord_id) in term_offsets[start..stop]
                .iter()
                .map(|&(_, _, bucket)| bucket)
                .enumerate()
            {
                mapping.insert(term_unord_id, term_ord);
            }
            unordered_term_mappings.insert(field, mapping);

            let postings_writer = &self.per_field_postings_writers[field.0 as usize];
            let mut field_serializer = serializer.new_field(field)?;
            postings_writer.serialize(
                &term_offsets[start..stop],
                &mut field_serializer,
                self.heap,
            )?;
            field_serializer.close()?;
        }
        Ok(unordered_term_mappings)
    }

    /// Return true iff the term dictionary is saturated.
    pub fn is_term_saturated(&self) -> bool {
        self.term_index.is_saturated()
    }
}

/// The `PostingsWriter` is in charge of receiving documenting
/// and building a `Segment` in anonymous memory.
///
/// `PostingsWriter` writes in a `Heap`.
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
        heap: &Heap,
    ) -> UnorderedTermId;

    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(
        &self,
        term_addrs: &[(&[u8], u32, UnorderedTermId)],
        serializer: &mut FieldSerializer,
        heap: &Heap,
    ) -> io::Result<()>;

    /// Tokenize a text and subscribe all of its token.
    fn index_text(
        &mut self,
        term_index: &mut TermHashMap,
        doc_id: DocId,
        field: Field,
        token_stream: &mut TokenStream,
        heap: &Heap,
    ) -> u32 {
        let mut term = unsafe { Term::with_capacity(100) };
        term.set_field(field);
        let mut sink = |token: &Token| {
            term.set_text(token.text.as_str());
            self.subscribe(term_index, doc_id, token.position as u32, &term, heap);
        };
        token_stream.process(&mut sink)
    }
}

/// The `SpecializedPostingsWriter` is just here to remove dynamic
/// dispatch to the recorder information.
pub struct SpecializedPostingsWriter<'a, Rec: Recorder + 'static> {
    heap: &'a Heap,
    _recorder_type: PhantomData<Rec>,
}

impl<'a, Rec: Recorder + 'static> SpecializedPostingsWriter<'a, Rec> {
    /// constructor
    pub fn new(heap: &'a Heap) -> SpecializedPostingsWriter<'a, Rec> {
        SpecializedPostingsWriter {
            heap,
            _recorder_type: PhantomData,
        }
    }

    /// Builds a `SpecializedPostingsWriter` storing its data in a heap.
    pub fn new_boxed(heap: &'a Heap) -> Box<PostingsWriter + 'a> {
        Box::new(SpecializedPostingsWriter::<Rec>::new(heap))
    }
}

impl<'a, Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<'a, Rec> {
    fn subscribe(
        &mut self,
        term_index: &mut TermHashMap,
        doc: DocId,
        position: u32,
        term: &Term,
        heap: &Heap,
    ) -> UnorderedTermId {
        debug_assert!(term.as_slice().len() >= 4);
        let (term_ord, recorder): (UnorderedTermId, &mut Rec) = term_index.get_or_create(term);
        let current_doc = recorder.current_doc();
        if current_doc != doc {
            if current_doc != u32::max_value() {
                recorder.close_doc(heap);
            }
            recorder.new_doc(doc, heap);
        }
        recorder.record_position(position, heap);
        term_ord
    }

    fn serialize(
        &self,
        term_addrs: &[(&[u8], u32, UnorderedTermId)],
        serializer: &mut FieldSerializer,
        heap: &Heap,
    ) -> io::Result<()> {
        for &(term_bytes, addr, _) in term_addrs {
            let recorder: &mut Rec = self.heap.get_mut_ref(addr);
            serializer.new_term(&term_bytes[4..])?;
            recorder.serialize(addr, serializer, heap)?;
            serializer.close_term()?;
        }
        Ok(())
    }
}
