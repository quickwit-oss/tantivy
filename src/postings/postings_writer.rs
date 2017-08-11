use DocId;
use schema::Term;
use schema::FieldValue;
use postings::{InvertedIndexSerializer, FieldSerializer};
use std::io;
use postings::Recorder;
use analyzer::SimpleTokenizer;
use Result;
use schema::{Schema, Field};
use analyzer::StreamingIterator;
use std::marker::PhantomData;
use std::ops::DerefMut;
use datastruct::stacker::{HashMap, Heap};
use postings::{NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
use schema::FieldEntry;
use schema::FieldType;
use schema::TextIndexingOptions;

fn posting_from_field_entry<'a>(field_entry: &FieldEntry,
                                heap: &'a Heap)
                                -> Box<PostingsWriter + 'a> {
    match *field_entry.field_type() {
        FieldType::Str(ref text_options) => {
            match text_options.get_indexing_options() {
                TextIndexingOptions::TokenizedWithFreq => {
                    SpecializedPostingsWriter::<TermFrequencyRecorder>::new_boxed(heap)
                }
                TextIndexingOptions::TokenizedWithFreqAndPosition => {
                    SpecializedPostingsWriter::<TFAndPositionRecorder>::new_boxed(heap)
                }
                _ => SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap),
            }
        }
        FieldType::U64(_) |
        FieldType::I64(_) => SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap),
    }
}


pub struct MultiFieldPostingsWriter<'a> {
    heap: &'a Heap,
    term_index: HashMap<'a>,
    per_field_postings_writers: Vec<Box<PostingsWriter + 'a>>,
}

impl<'a> MultiFieldPostingsWriter<'a> {
    /// Create a new `MultiFieldPostingsWriter` given
    /// a schema and a heap.
    pub fn new(schema: &Schema, table_bits: usize, heap: &'a Heap) -> MultiFieldPostingsWriter<'a> {
        let term_index = HashMap::new(table_bits, heap);
        let per_field_postings_writers: Vec<_> = schema
            .fields()
            .iter()
            .map(|field_entry| {
                posting_from_field_entry(field_entry, heap)
            })
            .collect();

        MultiFieldPostingsWriter {
            heap: heap,
            term_index: term_index,
            per_field_postings_writers: per_field_postings_writers,
        }
    }

    pub fn index_text(&mut self, doc: DocId, field: Field, field_values: &[&FieldValue]) -> u32 {
        let postings_writer = self.per_field_postings_writers[field.0 as usize].deref_mut();
        postings_writer.index_text(&mut self.term_index, doc, field, field_values, self.heap)
    }

    pub fn suscribe(&mut self, doc: DocId, term: &Term) {
        let postings_writer = self.per_field_postings_writers[term.field().0 as usize].deref_mut();
        postings_writer.suscribe(&mut self.term_index, doc, 0u32, term, self.heap)
    }


    /// Serialize the inverted index.
    /// It pushes all term, one field at a time, towards the
    /// postings serializer.
    #[allow(needless_range_loop)]
    pub fn serialize(&self, serializer: &mut InvertedIndexSerializer) -> Result<()> {
        let mut term_offsets: Vec<(&[u8], u32)> = self.term_index.iter().collect();
        term_offsets.sort_by_key(|&(k, _v)| k);

        let mut offsets: Vec<(Field, usize)> = vec![];
        let term_offsets_it = term_offsets
            .iter()
            .cloned()
            .map(|(key, _)| Term::wrap(key).field())
            .enumerate();

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
            let postings_writer = &self.per_field_postings_writers[field.0 as usize];
            let field_serializer = serializer.new_field(field);
            postings_writer.serialize(&term_offsets[start..stop], field_serializer, self.heap)?;
        }
        Ok(())
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
    fn suscribe(&mut self,
                term_index: &mut HashMap,
                doc: DocId,
                pos: u32,
                term: &Term,
                heap: &Heap);

    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(&self,
                 term_addrs: &[(&[u8], u32)],
                 serializer: FieldSerializer,
                 heap: &Heap)
                 -> io::Result<()>;

    /// Tokenize a text and suscribe all of its token.
    fn index_text<'a>(&mut self,
                      term_index: &mut HashMap,
                      doc_id: DocId,
                      field: Field,
                      field_values: &[&'a FieldValue],
                      heap: &Heap)
                      -> u32 {
        let mut pos = 0u32;
        let mut num_tokens: u32 = 0u32;
        let mut term = unsafe { Term::with_capacity(100) };
        term.set_field(field);
        for field_value in field_values {
            let mut tokens = SimpleTokenizer.tokenize(field_value.value().text());
            // right now num_tokens and pos are redundant, but it should
            // change when we get proper analyzers
            while let Some(token) = tokens.next() {
                term.set_text(token);
                self.suscribe(term_index, doc_id, pos, &term, heap);
                pos += 1u32;
                num_tokens += 1u32;
            }
            pos += 1;
            // THIS is to avoid phrase query accross field repetition.
            // span queries might still match though :|
        }
        num_tokens
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
            heap: heap,
            _recorder_type: PhantomData,
        }
    }

    /// Builds a `SpecializedPostingsWriter` storing its data in a heap.
    pub fn new_boxed(heap: &'a Heap) -> Box<PostingsWriter + 'a> {
        Box::new(SpecializedPostingsWriter::<Rec>::new(heap))
    }
}

impl<'a, Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<'a, Rec> {
    fn suscribe(&mut self,
                term_index: &mut HashMap,
                doc: DocId,
                position: u32,
                term: &Term,
                heap: &Heap) {
        debug_assert!(term.as_slice().len() >= 4);
        let recorder: &mut Rec = term_index.get_or_create(term);
        let current_doc = recorder.current_doc();
        if current_doc != doc {
            if current_doc != u32::max_value() {
                recorder.close_doc(heap);
            }
            recorder.new_doc(doc, heap);
        }
        recorder.record_position(position, heap);
    }

    fn serialize(&self,
                 term_addrs: &[(&[u8], u32)],
                 mut serializer: FieldSerializer,
                 heap: &Heap)
                 -> io::Result<()> {
        for &(term_bytes, addr) in term_addrs {
            let recorder: &mut Rec = self.heap.get_mut_ref(addr);
            serializer.new_term(term_bytes)?;
            recorder.serialize(addr, &mut serializer, heap)?;
            serializer.close_term()?;
        }
        Ok(())
    }
}

