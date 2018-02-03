use Result;
use DocId;
use std::io;
use std::str;
use schema::Schema;
use schema::Term;
use core::Segment;
use core::SerializableSegment;
use fastfield::FastFieldsWriter;
use schema::Field;
use schema::FieldType;
use indexer::segment_serializer::SegmentSerializer;
use std::collections::HashMap;
use datastruct::stacker::Heap;
use indexer::index_writer::MARGIN_IN_BYTES;
use super::operation::AddOperation;
use postings::MultiFieldPostingsWriter;
use tokenizer::BoxedTokenizer;
use tokenizer::FacetTokenizer;
use tokenizer::{TokenStream, Tokenizer};
use schema::Value;

/// A `SegmentWriter` is in charge of creating segment index from a
/// documents.
///
/// They creates the postings list in anonymous memory.
/// The segment is layed on disk when the segment gets `finalized`.
pub struct SegmentWriter<'a> {
    heap: &'a Heap,
    max_doc: DocId,
    multifield_postings: MultiFieldPostingsWriter<'a>,
    segment_serializer: SegmentSerializer,
    fast_field_writers: FastFieldsWriter,
    fieldnorms_writer: FastFieldsWriter,
    doc_opstamps: Vec<u64>,
    tokenizers: Vec<Option<Box<BoxedTokenizer>>>,
}

fn create_fieldnorms_writer(schema: &Schema) -> FastFieldsWriter {
    let u64_fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|&(_, field_entry)| field_entry.is_indexed())
        .map(|(field_id, _)| Field(field_id as u32))
        .collect();
    FastFieldsWriter::new(u64_fields)
}

impl<'a> SegmentWriter<'a> {
    /// Creates a new `SegmentWriter`
    ///
    /// The arguments are defined as follows
    ///
    /// - heap: most of the segment writer data (terms, and postings lists recorders)
    /// is stored in a user-defined heap object. This makes it possible for the user to define
    /// the flushing behavior as a buffer limit
    /// - segment: The segment being written
    /// - schema
    pub fn for_segment(
        heap: &'a Heap,
        table_bits: usize,
        mut segment: Segment,
        schema: &Schema,
    ) -> Result<SegmentWriter<'a>> {
        let segment_serializer = SegmentSerializer::for_segment(&mut segment)?;
        let multifield_postings = MultiFieldPostingsWriter::new(schema, table_bits, heap);
        let tokenizers = schema
            .fields()
            .iter()
            .map(|field_entry| field_entry.field_type())
            .map(|field_type| match *field_type {
                FieldType::Str(ref text_options) => text_options.get_indexing_options().and_then(
                    |text_index_option| {
                        let tokenizer_name = &text_index_option.tokenizer();
                        segment.index().tokenizers().get(tokenizer_name)
                    },
                ),
                _ => None,
            })
            .collect();
        Ok(SegmentWriter {
            heap,
            max_doc: 0,
            multifield_postings,
            fieldnorms_writer: create_fieldnorms_writer(schema),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema(schema),
            doc_opstamps: Vec::with_capacity(1_000),
            tokenizers,
        })
    }

    /// Lay on disk the current content of the `SegmentWriter`
    ///
    /// Finalize consumes the `SegmentWriter`, so that it cannot
    /// be used afterwards.
    pub fn finalize(self) -> Result<Vec<u64>> {
        write(
            &self.multifield_postings,
            &self.fast_field_writers,
            &self.fieldnorms_writer,
            self.segment_serializer,
        )?;
        Ok(self.doc_opstamps)
    }

    /// Returns true iff the segment writer's buffer has reached capacity.
    ///
    /// The limit is defined as `the user defined heap size - an arbitrary margin of 10MB`
    /// The `Segment` is `finalize`d when the buffer gets full.
    ///
    /// Because, we cannot cut through a document, the margin is there to ensure that we rarely
    /// exceeds the heap size.
    pub fn is_buffer_full(&self) -> bool {
        self.heap.num_free_bytes() <= MARGIN_IN_BYTES
    }

    /// Return true if the term dictionary hashmap is reaching capacity.
    /// It is one of the condition that triggers a `SegmentWriter` to
    /// be finalized.
    pub(crate) fn is_term_saturated(&self) -> bool {
        self.multifield_postings.is_term_saturated()
    }

    /// Indexes a new document
    ///
    /// As a user, you should rather use `IndexWriter`'s add_document.
    pub fn add_document(&mut self, add_operation: AddOperation, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        let mut doc = add_operation.document;
        self.doc_opstamps.push(add_operation.opstamp);

        self.fast_field_writers.add_document(&doc);

        for (field, field_values) in doc.get_sorted_field_values() {
            let field_options = schema.get_field_entry(field);
            if !field_options.is_indexed() {
                continue;
            }
            match *field_options.field_type() {
                FieldType::HierarchicalFacet => {
                    let facets: Vec<&[u8]> = field_values
                        .iter()
                        .flat_map(|field_value|
                            match *field_value.value() {
                                Value::Facet(ref facet) => Some(facet.encoded_bytes()),
                                _ => {
                                    panic!("Expected hierarchical facet");
                                }
                            })
                        .collect();
                    let mut term = unsafe { Term::with_capacity(100) };
                    term.set_field(field);
                    for facet_bytes in facets {
                        let mut unordered_term_id_opt = None;
                        let fake_str = unsafe { str::from_utf8_unchecked(facet_bytes) };
                        FacetTokenizer
                            .token_stream(fake_str)
                            .process(&mut |token| {
                                term.set_text(&token.text);
                                let unordered_term_id =
                                    self.multifield_postings.subscribe(doc_id, &term);
                                unordered_term_id_opt = Some(unordered_term_id);
                            });

                        if let Some(unordered_term_id) = unordered_term_id_opt {
                            self.fast_field_writers
                                .get_multivalue_writer(field)
                                .expect("multified writer for facet missing")
                                .add_val(unordered_term_id);
                        }
                    }
                }
                FieldType::Str(_) => {
                    let num_tokens = if let Some(ref mut tokenizer) =
                        self.tokenizers[field.0 as usize]
                    {
                        let texts: Vec<&str> = field_values
                            .iter()
                            .flat_map(|field_value| match *field_value.value() {
                                Value::Str(ref text) => Some(text.as_str()),
                                _ => None,
                            })
                            .collect();
                        if texts.is_empty() {
                            0
                        } else {
                            let mut token_stream = tokenizer.token_stream_texts(&texts[..]);
                            self.multifield_postings
                                .index_text(doc_id, field, &mut token_stream)
                        }
                    } else {
                        0
                    };
                    self.fieldnorms_writer
                        .get_field_writer(field)
                        .map(|field_norms_writer| {
                            field_norms_writer.add_val(u64::from(num_tokens))
                        });
                }
                FieldType::U64(ref int_option) => {
                    if int_option.is_indexed() {
                        for field_value in field_values {
                            let term = Term::from_field_u64(
                                field_value.field(),
                                field_value.value().u64_value(),
                            );
                            self.multifield_postings.subscribe(doc_id, &term);
                        }
                    }
                }
                FieldType::I64(ref int_option) => {
                    if int_option.is_indexed() {
                        for field_value in field_values {
                            let term = Term::from_field_i64(
                                field_value.field(),
                                field_value.value().i64_value(),
                            );
                            self.multifield_postings.subscribe(doc_id, &term);
                        }
                    }
                }
            }
        }
        self.fieldnorms_writer.fill_val_up_to(doc_id);
        doc.filter_fields(|field| schema.get_field_entry(field).is_stored());
        let doc_writer = self.segment_serializer.get_store_writer();
        doc_writer.store(&doc)?;
        self.max_doc += 1;
        Ok(())
    }

    /// Max doc is
    /// - the number of documents in the segment assuming there is no deletes
    /// - the maximum document id (including deleted documents) + 1
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    pub fn max_doc(&self) -> u32 {
        self.max_doc
    }

    /// Number of documents in the index.
    /// Deleted documents are not counted.
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    #[allow(dead_code)]
    pub fn num_docs(&self) -> u32 {
        self.max_doc
    }
}

// This method is used as a trick to workaround the borrow checker
fn write(
    multifield_postings: &MultiFieldPostingsWriter,
    fast_field_writers: &FastFieldsWriter,
    fieldnorms_writer: &FastFieldsWriter,
    mut serializer: SegmentSerializer,
) -> Result<()> {
    let term_ord_map = multifield_postings.serialize(serializer.get_postings_serializer())?;
    fast_field_writers.serialize(serializer.get_fast_field_serializer(), &term_ord_map)?;
    fieldnorms_writer.serialize(serializer.get_fieldnorms_serializer(), &HashMap::new())?;
    serializer.close()?;

    Ok(())
}

impl<'a> SerializableSegment for SegmentWriter<'a> {
    fn write(&self, serializer: SegmentSerializer) -> Result<u32> {
        let max_doc = self.max_doc;
        write(
            &self.multifield_postings,
            &self.fast_field_writers,
            &self.fieldnorms_writer,
            serializer,
        )?;
        Ok(max_doc)
    }
}
