use super::doc_id_mapping::{get_doc_id_mapping_from_field, DocIdMapping};
use super::operation::AddOperation;
use crate::core::Segment;
use crate::fastfield::FastFieldsWriter;
use crate::fieldnorm::{FieldNormReaders, FieldNormsWriter};
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::postings::{
    compute_table_size, serialize_postings, IndexingContext, PerFieldPostingsWriter, PostingsWriter,
};
use crate::schema::{Field, FieldEntry, FieldType, FieldValue, Schema, Term, Type, Value};
use crate::store::{StoreReader, StoreWriter};
use crate::tokenizer::{
    BoxTokenStream, FacetTokenizer, PreTokenizedStream, TextAnalyzer, TokenStreamChain, Tokenizer,
};
use crate::{DocId, Document, Opstamp, SegmentComponent};

/// Computes the initial size of the hash table.
///
/// Returns the recommended initial table size as a power of 2.
///
/// Note this is a very dumb way to compute log2, but it is easier to proofread that way.
fn compute_initial_table_size(per_thread_memory_budget: usize) -> crate::Result<usize> {
    let table_memory_upper_bound = per_thread_memory_budget / 3;
    (10..20) // We cap it at 2^19 = 512K capacity.
        .map(|power| 1 << power)
        .take_while(|capacity| compute_table_size(*capacity) < table_memory_upper_bound)
        .last()
        .ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!(
                "per thread memory budget (={per_thread_memory_budget}) is too small. Raise the \
                 memory budget or lower the number of threads."
            ))
        })
}

fn remap_doc_opstamps(
    opstamps: Vec<Opstamp>,
    doc_id_mapping_opt: Option<&DocIdMapping>,
) -> Vec<Opstamp> {
    if let Some(doc_id_mapping_opt) = doc_id_mapping_opt {
        doc_id_mapping_opt
            .iter_old_doc_ids()
            .map(|doc| opstamps[doc as usize])
            .collect()
    } else {
        opstamps
    }
}

/// A `SegmentWriter` is in charge of creating segment index from a
/// set of documents.
///
/// They creates the postings list in anonymous memory.
/// The segment is layed on disk when the segment gets `finalized`.
pub struct SegmentWriter {
    pub(crate) max_doc: DocId,
    pub(crate) indexing_context: IndexingContext,
    pub(crate) per_field_postings_writers: PerFieldPostingsWriter,
    pub(crate) segment_serializer: SegmentSerializer,
    pub(crate) fast_field_writers: FastFieldsWriter,
    pub(crate) fieldnorms_writer: FieldNormsWriter,
    pub(crate) doc_opstamps: Vec<Opstamp>,
    tokenizers: Vec<Option<TextAnalyzer>>,
    term_buffer: Term,
    schema: Schema,
}

impl SegmentWriter {
    /// Creates a new `SegmentWriter`
    ///
    /// The arguments are defined as follows
    ///
    /// - memory_budget: most of the segment writer data (terms, and postings lists recorders)
    /// is stored in a memory arena. This makes it possible for the user to define
    /// the flushing behavior as a memory limit.
    /// - segment: The segment being written
    /// - schema
    pub fn for_segment(
        memory_budget_in_bytes: usize,
        segment: Segment,
        schema: Schema,
    ) -> crate::Result<SegmentWriter> {
        let tokenizer_manager = segment.index().tokenizers().clone();
        let table_size = compute_initial_table_size(memory_budget_in_bytes)?;
        let segment_serializer = SegmentSerializer::for_segment(segment, false)?;
        let per_field_postings_writers = PerFieldPostingsWriter::for_schema(&schema);
        let tokenizers = schema
            .fields()
            .map(
                |(_, field_entry): (Field, &FieldEntry)| match field_entry.field_type() {
                    FieldType::Str(ref text_options) => text_options
                        .get_indexing_options()
                        .and_then(|text_index_option| {
                            let tokenizer_name = &text_index_option.tokenizer();
                            tokenizer_manager.get(tokenizer_name)
                        }),
                    _ => None,
                },
            )
            .collect();
        Ok(SegmentWriter {
            max_doc: 0,
            indexing_context: IndexingContext::new(table_size),
            per_field_postings_writers,
            fieldnorms_writer: FieldNormsWriter::for_schema(&schema),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema(&schema),
            doc_opstamps: Vec::with_capacity(1_000),
            tokenizers,
            term_buffer: Term::new(),
            schema,
        })
    }

    /// Lay on disk the current content of the `SegmentWriter`
    ///
    /// Finalize consumes the `SegmentWriter`, so that it cannot
    /// be used afterwards.
    pub fn finalize(mut self) -> crate::Result<Vec<u64>> {
        self.fieldnorms_writer.fill_up_to_max_doc(self.max_doc);
        let mapping: Option<DocIdMapping> = self
            .segment_serializer
            .segment()
            .index()
            .settings()
            .sort_by_field
            .clone()
            .map(|sort_by_field| get_doc_id_mapping_from_field(sort_by_field, &self))
            .transpose()?;
        remap_and_write(
            &self.per_field_postings_writers,
            self.indexing_context,
            &self.fast_field_writers,
            &self.fieldnorms_writer,
            &self.schema,
            self.segment_serializer,
            mapping.as_ref(),
        )?;
        let doc_opstamps = remap_doc_opstamps(self.doc_opstamps, mapping.as_ref());
        Ok(doc_opstamps)
    }

    pub fn mem_usage(&self) -> usize {
        self.indexing_context.mem_usage()
            + self.fieldnorms_writer.mem_usage()
            + self.fast_field_writers.mem_usage()
            + self.segment_serializer.mem_usage()
    }

    fn index_document(&mut self, doc: &Document) -> crate::Result<()> {
        let doc_id = self.max_doc;
        for (field, values) in doc.get_sorted_field_values() {
            let field_entry = self.schema.get_field_entry(field);
            let make_schema_error = || {
                crate::TantivyError::SchemaError(format!(
                    "Expected a {:?} for field {:?}",
                    field_entry.field_type().value_type(),
                    field_entry.name()
                ))
            };
            if !field_entry.is_indexed() {
                continue;
            }
            let (term_buffer, indexing_context) =
                (&mut self.term_buffer, &mut self.indexing_context);
            let postings_writer: &mut dyn PostingsWriter =
                self.per_field_postings_writers.get_for_field_mut(field);
            match *field_entry.field_type() {
                FieldType::Facet(_) => {
                    term_buffer.set_field(Type::Facet, field);
                    for value in values {
                        let facet = value.as_facet().ok_or_else(make_schema_error)?;
                        let facet_str = facet.encoded_str();
                        let mut unordered_term_id_opt = None;
                        FacetTokenizer
                            .token_stream(facet_str)
                            .process(&mut |token| {
                                term_buffer.set_text(&token.text);
                                let unordered_term_id = postings_writer.subscribe(
                                    doc_id,
                                    0u32,
                                    term_buffer,
                                    indexing_context,
                                );
                                // TODO pass indexing context directly in subscribe function
                                unordered_term_id_opt = Some(unordered_term_id);
                            });
                        if let Some(unordered_term_id) = unordered_term_id_opt {
                            self.fast_field_writers
                                .get_multivalue_writer_mut(field)
                                .expect("writer for facet missing")
                                .add_val(unordered_term_id);
                        }
                    }
                }
                FieldType::Str(_) => {
                    let mut token_streams: Vec<BoxTokenStream> = vec![];
                    let mut offsets = vec![];
                    let mut total_offset = 0;

                    for value in values {
                        match value {
                            Value::PreTokStr(tok_str) => {
                                offsets.push(total_offset);
                                if let Some(last_token) = tok_str.tokens.last() {
                                    total_offset += last_token.offset_to;
                                }
                                token_streams
                                    .push(PreTokenizedStream::from(tok_str.clone()).into());
                            }
                            Value::Str(ref text) => {
                                if let Some(ref mut tokenizer) =
                                    self.tokenizers[field.field_id() as usize]
                                {
                                    offsets.push(total_offset);
                                    total_offset += text.len();
                                    token_streams.push(tokenizer.token_stream(text));
                                }
                            }
                            _ => (),
                        }
                    }

                    let num_tokens = if token_streams.is_empty() {
                        0
                    } else {
                        let mut token_stream = TokenStreamChain::new(offsets, token_streams);
                        postings_writer.index_text(
                            doc_id,
                            field,
                            &mut token_stream,
                            term_buffer,
                            indexing_context,
                        )
                    };
                    self.fieldnorms_writer.record(doc_id, field, num_tokens);
                }
                FieldType::U64(_) => {
                    for value in values {
                        term_buffer.set_field(Type::U64, field);
                        let u64_val = value.as_u64().ok_or_else(make_schema_error)?;
                        term_buffer.set_u64(u64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, indexing_context);
                    }
                }
                FieldType::Date(_) => {
                    for value in values {
                        term_buffer.set_field(Type::Date, field);
                        let date_val = value.as_date().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(date_val.timestamp());
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, indexing_context);
                    }
                }
                FieldType::I64(_) => {
                    for value in values {
                        term_buffer.set_field(Type::I64, field);
                        let i64_val = value.as_i64().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(i64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, indexing_context);
                    }
                }
                FieldType::F64(_) => {
                    for value in values {
                        term_buffer.set_field(Type::F64, field);
                        let f64_val = value.as_f64().ok_or_else(make_schema_error)?;
                        term_buffer.set_f64(f64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, indexing_context);
                    }
                }
                FieldType::Bytes(_) => {
                    for value in values {
                        term_buffer.set_field(Type::Bytes, field);
                        let bytes = value.as_bytes().ok_or_else(make_schema_error)?;
                        term_buffer.set_bytes(bytes);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, indexing_context);
                    }
                }
            }
        }
        Ok(())
    }

    /// Indexes a new document
    ///
    /// As a user, you should rather use `IndexWriter`'s add_document.
    pub fn add_document(&mut self, add_operation: AddOperation) -> crate::Result<()> {
        let doc = add_operation.document;
        self.doc_opstamps.push(add_operation.opstamp);
        self.fast_field_writers.add_document(&doc);
        self.index_document(&doc)?;
        let prepared_doc = prepare_doc_for_store(doc, &self.schema);
        let doc_writer = self.segment_serializer.get_store_writer();
        doc_writer.store(&prepared_doc)?;
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

/// This method is used as a trick to workaround the borrow checker
/// Writes a view of a segment by pushing information
/// to the `SegmentSerializer`.
///
/// `doc_id_map` is used to map to the new doc_id order.
fn remap_and_write(
    per_field_postings_writers: &PerFieldPostingsWriter,
    indexing_context: IndexingContext,
    fast_field_writers: &FastFieldsWriter,
    fieldnorms_writer: &FieldNormsWriter,
    schema: &Schema,
    mut serializer: SegmentSerializer,
    doc_id_map: Option<&DocIdMapping>,
) -> crate::Result<()> {
    if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
        fieldnorms_writer.serialize(fieldnorms_serializer, doc_id_map)?;
    }
    let fieldnorm_data = serializer
        .segment()
        .open_read(SegmentComponent::FieldNorms)?;
    let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
    let term_ord_map = serialize_postings(
        indexing_context,
        per_field_postings_writers,
        fieldnorm_readers,
        doc_id_map,
        schema,
        serializer.get_postings_serializer(),
    )?;
    fast_field_writers.serialize(
        serializer.get_fast_field_serializer(),
        &term_ord_map,
        doc_id_map,
    )?;

    // finalize temp docstore and create version, which reflects the doc_id_map
    if let Some(doc_id_map) = doc_id_map {
        let store_write = serializer
            .segment_mut()
            .open_write(SegmentComponent::Store)?;
        let compressor = serializer.segment().index().settings().docstore_compression;
        let old_store_writer = std::mem::replace(
            &mut serializer.store_writer,
            StoreWriter::new(store_write, compressor),
        );
        old_store_writer.close()?;
        let store_read = StoreReader::open(
            serializer
                .segment()
                .open_read(SegmentComponent::TempStore)?,
        )?;
        for old_doc_id in doc_id_map.iter_old_doc_ids() {
            let doc_bytes = store_read.get_document_bytes(old_doc_id)?;
            serializer.get_store_writer().store_bytes(&doc_bytes)?;
        }
    }

    serializer.close()?;

    Ok(())
}

/// Prepares Document for being stored in the document store
///
/// Method transforms PreTokenizedString values into String
/// values.
pub fn prepare_doc_for_store(doc: Document, schema: &Schema) -> Document {
    Document::from(
        doc.into_iter()
            .filter(|field_value| schema.get_field_entry(field_value.field()).is_stored())
            .map(|field_value| match field_value {
                FieldValue {
                    field,
                    value: Value::PreTokStr(pre_tokenized_text),
                } => FieldValue {
                    field,
                    value: Value::Str(pre_tokenized_text.text),
                },
                field_value => field_value,
            })
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::compute_initial_table_size;
    use crate::schema::{Schema, STORED, TEXT};
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::Document;

    #[test]
    fn test_hashmap_size() {
        assert_eq!(compute_initial_table_size(100_000).unwrap(), 1 << 11);
        assert_eq!(compute_initial_table_size(1_000_000).unwrap(), 1 << 14);
        assert_eq!(compute_initial_table_size(10_000_000).unwrap(), 1 << 17);
        assert_eq!(compute_initial_table_size(1_000_000_000).unwrap(), 1 << 19);
        assert_eq!(compute_initial_table_size(4_000_000_000).unwrap(), 1 << 19);
    }

    #[test]
    fn test_prepare_for_store() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT | STORED);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        let pre_tokenized_text = PreTokenizedString {
            text: String::from("A"),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 1,
                position: 0,
                text: String::from("A"),
                position_length: 1,
            }],
        };

        doc.add_pre_tokenized_text(text_field, pre_tokenized_text);
        doc.add_text(text_field, "title");
        let prepared_doc = super::prepare_doc_for_store(doc, &schema);

        assert_eq!(prepared_doc.field_values().len(), 2);
        assert_eq!(prepared_doc.field_values()[0].value().as_text(), Some("A"));
        assert_eq!(
            prepared_doc.field_values()[1].value().as_text(),
            Some("title")
        );
    }
}
