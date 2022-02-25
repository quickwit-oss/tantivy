use super::doc_id_mapping::{get_doc_id_mapping_from_field, DocIdMapping};
use super::operation::AddOperation;
use crate::core::Segment;
use crate::fastfield::FastFieldsWriter;
use crate::fieldnorm::{FieldNormReaders, FieldNormsWriter};
use crate::indexer::json_term_writer::index_json_values;
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::postings::{
    compute_table_size, serialize_postings, IndexingContext, IndexingPosition,
    PerFieldPostingsWriter, PostingsWriter,
};
use crate::schema::{FieldEntry, FieldType, FieldValue, Schema, Term, Value};
use crate::store::{StoreReader, StoreWriter};
use crate::tokenizer::{
    BoxTokenStream, FacetTokenizer, PreTokenizedStream, TextAnalyzer, Tokenizer,
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
    pub(crate) ctx: IndexingContext,
    pub(crate) per_field_postings_writers: PerFieldPostingsWriter,
    pub(crate) segment_serializer: SegmentSerializer,
    pub(crate) fast_field_writers: FastFieldsWriter,
    pub(crate) fieldnorms_writer: FieldNormsWriter,
    pub(crate) doc_opstamps: Vec<Opstamp>,
    per_field_text_analyzers: Vec<TextAnalyzer>,
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
        let per_field_text_analyzers = schema
            .fields()
            .map(|(_, field_entry): (_, &FieldEntry)| {
                let text_options = match field_entry.field_type() {
                    FieldType::Str(ref text_options) => text_options.get_indexing_options(),
                    FieldType::JsonObject(ref json_object_options) => {
                        json_object_options.get_text_indexing_options()
                    }
                    _ => None,
                };
                text_options
                    .and_then(|text_index_option| {
                        let tokenizer_name = &text_index_option.tokenizer();
                        tokenizer_manager.get(tokenizer_name)
                    })
                    .unwrap_or_default()
            })
            .collect();
        Ok(SegmentWriter {
            max_doc: 0,
            ctx: IndexingContext::new(table_size),
            per_field_postings_writers,
            fieldnorms_writer: FieldNormsWriter::for_schema(&schema),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema(&schema),
            doc_opstamps: Vec::with_capacity(1_000),
            per_field_text_analyzers,
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
            self.ctx,
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
        self.ctx.mem_usage()
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
            let (term_buffer, ctx) = (&mut self.term_buffer, &mut self.ctx);
            let postings_writer: &mut dyn PostingsWriter =
                self.per_field_postings_writers.get_for_field_mut(field);
            term_buffer.set_field(field_entry.field_type().value_type(), field);
            match *field_entry.field_type() {
                FieldType::Facet(_) => {
                    for value in values {
                        let facet = value.as_facet().ok_or_else(make_schema_error)?;
                        let facet_str = facet.encoded_str();
                        let mut unordered_term_id_opt = None;
                        FacetTokenizer
                            .token_stream(facet_str)
                            .process(&mut |token| {
                                term_buffer.set_text(&token.text);
                                let unordered_term_id =
                                    postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
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
                                let text_analyzer =
                                    &self.per_field_text_analyzers[field.field_id() as usize];
                                offsets.push(total_offset);
                                total_offset += text.len();
                                token_streams.push(text_analyzer.token_stream(text));
                            }
                            _ => (),
                        }
                    }

                    let mut indexing_position = IndexingPosition::default();
                    for mut token_stream in token_streams {
                        assert_eq!(term_buffer.as_slice().len(), 5);
                        postings_writer.index_text(
                            doc_id,
                            &mut *token_stream,
                            term_buffer,
                            ctx,
                            &mut indexing_position,
                        );
                    }
                    self.fieldnorms_writer
                        .record(doc_id, field, indexing_position.num_tokens);
                }
                FieldType::U64(_) => {
                    for value in values {
                        let u64_val = value.as_u64().ok_or_else(make_schema_error)?;
                        term_buffer.set_u64(u64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                FieldType::Date(_) => {
                    for value in values {
                        let date_val = value.as_date().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(date_val.timestamp());
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                FieldType::I64(_) => {
                    for value in values {
                        let i64_val = value.as_i64().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(i64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                FieldType::F64(_) => {
                    for value in values {
                        let f64_val = value.as_f64().ok_or_else(make_schema_error)?;
                        term_buffer.set_f64(f64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                FieldType::Bytes(_) => {
                    for value in values {
                        let bytes = value.as_bytes().ok_or_else(make_schema_error)?;
                        term_buffer.set_bytes(bytes);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                FieldType::JsonObject(_) => {
                    let text_analyzer = &self.per_field_text_analyzers[field.field_id() as usize];
                    let json_values_it = values
                        .iter()
                        .map(|value| value.as_json().ok_or_else(make_schema_error));
                    index_json_values(
                        doc_id,
                        json_values_it,
                        text_analyzer,
                        term_buffer,
                        postings_writer,
                        ctx,
                    )?;
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
    ctx: IndexingContext,
    fast_field_writers: &FastFieldsWriter,
    fieldnorms_writer: &FieldNormsWriter,
    schema: &Schema,
    mut serializer: SegmentSerializer,
    doc_id_map: Option<&DocIdMapping>,
) -> crate::Result<()> {
    debug!("remap-and-write");
    if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
        fieldnorms_writer.serialize(fieldnorms_serializer, doc_id_map)?;
    }
    let fieldnorm_data = serializer
        .segment()
        .open_read(SegmentComponent::FieldNorms)?;
    let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
    let term_ord_map = serialize_postings(
        ctx,
        per_field_postings_writers,
        fieldnorm_readers,
        doc_id_map,
        schema,
        serializer.get_postings_serializer(),
    )?;
    debug!("fastfield-serialize");
    fast_field_writers.serialize(
        serializer.get_fast_field_serializer(),
        &term_ord_map,
        doc_id_map,
    )?;

    debug!("resort-docstore");
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

    debug!("serializer-close");
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
    use chrono::Utc;

    use super::compute_initial_table_size;
    use crate::collector::Count;
    use crate::indexer::json_term_writer::JsonTermWriter;
    use crate::postings::TermInfo;
    use crate::query::PhraseQuery;
    use crate::schema::{IndexRecordOption, Schema, Type, STORED, STRING, TEXT};
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::{DocAddress, DocSet, Document, Index, Postings, Term, TERMINATED};

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

    #[test]
    fn test_json_indexing() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | TEXT);
        let schema = schema_builder.build();
        let json_val: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{
            "toto": "titi",
            "float": -0.2,
            "unsigned": 1,
            "signed": -2,
            "complexobject": {
                "field.with.dot": 1
            },
            "date": "1985-04-12T23:20:50.52Z",
            "my_arr": [2, 3, {"my_key": "two tokens"}, 4]
        }"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val.clone());
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let doc = searcher
            .doc(DocAddress {
                segment_ord: 0u32,
                doc_id: 0u32,
            })
            .unwrap();
        let serdeser_json_val = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
            &schema.to_json(&doc),
        )
        .unwrap()
        .get("json")
        .unwrap()[0]
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(json_val, serdeser_json_val);
        let segment_reader = searcher.segment_reader(0u32);
        let inv_idx = segment_reader.inverted_index(json_field).unwrap();
        let term_dict = inv_idx.terms();

        let mut term = Term::new();
        term.set_field(Type::Json, json_field);
        let mut term_stream = term_dict.stream().unwrap();

        let mut json_term_writer = JsonTermWriter::wrap(&mut term);
        json_term_writer.push_path_segment("complexobject");
        json_term_writer.push_path_segment("field.with.dot");
        json_term_writer.set_fast_value(1u64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.pop_path_segment();
        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("date");
        json_term_writer.set_fast_value(
            chrono::DateTime::parse_from_rfc3339("1985-04-12T23:20:50.52Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("float");
        json_term_writer.set_fast_value(-0.2f64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("my_arr");
        json_term_writer.set_fast_value(2u64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.set_fast_value(3u64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.set_fast_value(4u64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.push_path_segment("my_key");
        json_term_writer.set_str("tokens");
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.set_str("two");
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.pop_path_segment();
        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("signed");
        json_term_writer.set_fast_value(-2i64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("toto");
        json_term_writer.set_str("titi");
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("unsigned");
        json_term_writer.set_fast_value(1u64);
        assert!(term_stream.advance());
        assert_eq!(term_stream.key(), json_term_writer.term().value_bytes());
        assert!(!term_stream.advance());
    }

    #[test]
    fn test_json_tokenized_with_position() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | TEXT);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        let json_val: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"mykey": "repeated token token"}"#).unwrap();
        doc.add_json_object(json_field, json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inv_index = segment_reader.inverted_index(json_field).unwrap();
        let mut term = Term::new();
        term.set_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term);
        json_term_writer.push_path_segment("mykey");
        json_term_writer.set_str("token");
        let term_info = inv_index
            .get_term_info(json_term_writer.term())
            .unwrap()
            .unwrap();
        assert_eq!(
            term_info,
            TermInfo {
                doc_freq: 1,
                postings_range: 2..4,
                positions_range: 2..5
            }
        );
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0);
        assert_eq!(postings.term_freq(), 2);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(&positions[..], &[1, 2]);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_json_raw_no_position() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STRING);
        let schema = schema_builder.build();
        let json_val: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"mykey": "two tokens"}"#).unwrap();
        let doc = doc!(json_field=>json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inv_index = segment_reader.inverted_index(json_field).unwrap();
        let mut term = Term::new();
        term.set_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term);
        json_term_writer.push_path_segment("mykey");
        json_term_writer.set_str("two tokens");
        let term_info = inv_index
            .get_term_info(json_term_writer.term())
            .unwrap()
            .unwrap();
        assert_eq!(
            term_info,
            TermInfo {
                doc_freq: 1,
                postings_range: 0..1,
                positions_range: 0..0
            }
        );
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqs)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0);
        assert_eq!(postings.term_freq(), 1);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_position_overlapping_path() {
        // This test checks that we do not end up detecting phrase query due
        // to several string literal in the same json object being overlapping.
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let json_val: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{"mykey": [{"field": "hello happy tax payer"}, {"field": "nothello"}]}"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let mut term = Term::new();
        term.set_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term);
        json_term_writer.push_path_segment("mykey");
        json_term_writer.push_path_segment("field");
        json_term_writer.set_str("hello");
        let hello_term = json_term_writer.term().clone();
        json_term_writer.set_str("nothello");
        let nothello_term = json_term_writer.term().clone();
        json_term_writer.set_str("happy");
        let happy_term = json_term_writer.term().clone();
        let phrase_query = PhraseQuery::new(vec![hello_term, happy_term.clone()]);
        assert_eq!(searcher.search(&phrase_query, &Count).unwrap(), 1);
        let phrase_query = PhraseQuery::new(vec![nothello_term, happy_term]);
        assert_eq!(searcher.search(&phrase_query, &Count).unwrap(), 0);
    }
}
