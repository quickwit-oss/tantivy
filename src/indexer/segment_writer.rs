use columnar::MonotonicallyMappableToU64;
use itertools::Itertools;
use tokenizer_api::BoxTokenStream;

use super::doc_id_mapping::{get_doc_id_mapping_from_field, DocIdMapping};
use super::operation::AddOperation;
use crate::core::json_utils::index_json_values;
use crate::core::Segment;
use crate::fastfield::FastFieldsWriter;
use crate::fieldnorm::{FieldNormReaders, FieldNormsWriter};
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::postings::{
    compute_table_memory_size, serialize_postings, IndexingContext, IndexingPosition,
    PerFieldPostingsWriter, PostingsWriter,
};
use crate::schema::{FieldEntry, FieldType, Schema, Term, Value, DATE_TIME_PRECISION_INDEXED};
use crate::store::{StoreReader, StoreWriter};
use crate::tokenizer::{FacetTokenizer, PreTokenizedStream, TextAnalyzer, Tokenizer};
use crate::{DocId, Document, Opstamp, SegmentComponent, TantivyError};

/// Computes the initial size of the hash table.
///
/// Returns the recommended initial table size as a power of 2.
///
/// Note this is a very dumb way to compute log2, but it is easier to proofread that way.
fn compute_initial_table_size(per_thread_memory_budget: usize) -> crate::Result<usize> {
    let table_memory_upper_bound = per_thread_memory_budget / 3;
    (10..20) // We cap it at 2^19 = 512K capacity.
        // TODO: There are cases where this limit causes a
        // reallocation in the hashmap. Check if this affects performance.
        .map(|power| 1 << power)
        .take_while(|capacity| compute_table_memory_size(*capacity) < table_memory_upper_bound)
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
/// The segment is laid on disk when the segment gets `finalized`.
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
    ) -> crate::Result<SegmentWriter> {
        let schema = segment.schema();
        let tokenizer_manager = segment.index().tokenizers().clone();
        let tokenizer_manager_fast_field = segment.index().fast_field_tokenizer().clone();
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
                let tokenizer_name = text_options
                    .map(|text_index_option| text_index_option.tokenizer())
                    .unwrap_or("default");

                tokenizer_manager.get(tokenizer_name).ok_or_else(|| {
                    TantivyError::SchemaError(format!(
                        "Error getting tokenizer for field: {}",
                        field_entry.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SegmentWriter {
            max_doc: 0,
            ctx: IndexingContext::new(table_size),
            per_field_postings_writers,
            fieldnorms_writer: FieldNormsWriter::for_schema(&schema),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema_and_tokenizer_manager(
                &schema,
                tokenizer_manager_fast_field,
            )?,
            doc_opstamps: Vec::with_capacity(1_000),
            per_field_text_analyzers,
            term_buffer: Term::with_capacity(16),
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
            self.fast_field_writers,
            &self.fieldnorms_writer,
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
        let vals_grouped_by_field = doc
            .field_values()
            .iter()
            .sorted_by_key(|el| el.field())
            .group_by(|el| el.field());
        for (field, field_values) in &vals_grouped_by_field {
            let values = field_values.map(|field_value| field_value.value());
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
            term_buffer.clear_with_field_and_type(field_entry.field_type().value_type(), field);

            match field_entry.field_type() {
                FieldType::Facet(_) => {
                    let mut facet_tokenizer = FacetTokenizer::default(); // this can be global
                    for value in values {
                        let facet = value.as_facet().ok_or_else(make_schema_error)?;
                        let facet_str = facet.encoded_str();
                        let mut facet_tokenizer = facet_tokenizer.token_stream(facet_str);
                        let mut indexing_position = IndexingPosition::default();
                        postings_writer.index_text(
                            doc_id,
                            &mut facet_tokenizer,
                            term_buffer,
                            ctx,
                            &mut indexing_position,
                        );
                    }
                }
                FieldType::Str(_) => {
                    let mut indexing_position = IndexingPosition::default();
                    for value in values {
                        let mut token_stream = match value {
                            Value::PreTokStr(tok_str) => {
                                BoxTokenStream::new(PreTokenizedStream::from(tok_str.clone()))
                            }
                            Value::Str(ref text) => {
                                let text_analyzer =
                                    &mut self.per_field_text_analyzers[field.field_id() as usize];
                                text_analyzer.token_stream(text)
                            }
                            _ => {
                                continue;
                            }
                        };

                        assert!(term_buffer.is_empty());
                        postings_writer.index_text(
                            doc_id,
                            &mut *token_stream,
                            term_buffer,
                            ctx,
                            &mut indexing_position,
                        );
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer
                            .record(doc_id, field, indexing_position.num_tokens);
                    }
                }
                FieldType::U64(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let u64_val = value.as_u64().ok_or_else(make_schema_error)?;
                        term_buffer.set_u64(u64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::Date(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let date_val = value.as_date().ok_or_else(make_schema_error)?;
                        term_buffer
                            .set_u64(date_val.truncate(DATE_TIME_PRECISION_INDEXED).to_u64());
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::I64(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let i64_val = value.as_i64().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(i64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::F64(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let f64_val = value.as_f64().ok_or_else(make_schema_error)?;
                        term_buffer.set_f64(f64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::Bool(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let bool_val = value.as_bool().ok_or_else(make_schema_error)?;
                        term_buffer.set_bool(bool_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::Bytes(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let bytes = value.as_bytes().ok_or_else(make_schema_error)?;
                        term_buffer.set_bytes(bytes);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::JsonObject(json_options) => {
                    let text_analyzer =
                        &mut self.per_field_text_analyzers[field.field_id() as usize];
                    let json_values_it =
                        values.map(|value| value.as_json().ok_or_else(make_schema_error));
                    index_json_values(
                        doc_id,
                        json_values_it,
                        text_analyzer,
                        json_options.is_expand_dots_enabled(),
                        term_buffer,
                        postings_writer,
                        ctx,
                    )?;
                }
                FieldType::IpAddr(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        num_vals += 1;
                        let ip_addr = value.as_ip_addr().ok_or_else(make_schema_error)?;
                        term_buffer.set_ip_addr(ip_addr);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
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
        let AddOperation { document, opstamp } = add_operation;
        self.doc_opstamps.push(opstamp);
        self.fast_field_writers.add_document(&document)?;
        self.index_document(&document)?;
        let doc_writer = self.segment_serializer.get_store_writer();
        doc_writer.store(&document, &self.schema)?;
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
    fast_field_writers: FastFieldsWriter,
    fieldnorms_writer: &FieldNormsWriter,
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
    serialize_postings(
        ctx,
        per_field_postings_writers,
        fieldnorm_readers,
        doc_id_map,
        serializer.get_postings_serializer(),
    )?;
    debug!("fastfield-serialize");
    fast_field_writers.serialize(serializer.get_fast_field_write(), doc_id_map)?;

    // finalize temp docstore and create version, which reflects the doc_id_map
    if let Some(doc_id_map) = doc_id_map {
        debug!("resort-docstore");
        let store_write = serializer
            .segment_mut()
            .open_write(SegmentComponent::Store)?;
        let settings = serializer.segment().index().settings();
        let store_writer = StoreWriter::new(
            store_write,
            settings.docstore_compression,
            settings.docstore_blocksize,
            settings.docstore_compress_dedicated_thread,
        )?;
        let old_store_writer = std::mem::replace(&mut serializer.store_writer, store_writer);
        old_store_writer.close()?;
        let store_read = StoreReader::open(
            serializer
                .segment()
                .open_read(SegmentComponent::TempStore)?,
            1, /* The docstore is configured to have one doc per block, and each doc is accessed
                * only once: we don't need caching. */
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::compute_initial_table_size;
    use crate::collector::Count;
    use crate::core::json_utils::JsonTermWriter;
    use crate::directory::RamDirectory;
    use crate::postings::TermInfo;
    use crate::query::PhraseQuery;
    use crate::schema::{
        IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Type, STORED, STRING, TEXT,
    };
    use crate::store::{Compressor, StoreReader, StoreWriter};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::OffsetDateTime;
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::{
        DateTime, Directory, DocAddress, DocSet, Document, Index, Postings, Term, TERMINATED,
    };

    #[test]
    fn test_hashmap_size() {
        assert_eq!(compute_initial_table_size(100_000).unwrap(), 1 << 11);
        assert_eq!(compute_initial_table_size(1_000_000).unwrap(), 1 << 14);
        assert_eq!(compute_initial_table_size(15_000_000).unwrap(), 1 << 18);
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

        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path).unwrap();

        let mut store_writer = StoreWriter::new(store_wrt, Compressor::None, 0, false).unwrap();
        store_writer.store(&doc, &schema).unwrap();
        store_writer.close().unwrap();

        let reader = StoreReader::open(directory.open_read(path).unwrap(), 0).unwrap();
        let doc = reader.get(0).unwrap();

        assert_eq!(doc.field_values().len(), 2);
        assert_eq!(doc.field_values()[0].value().as_text(), Some("A"));
        assert_eq!(doc.field_values()[1].value().as_text(), Some("title"));
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
            "bool": true,
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

        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut term_stream = term_dict.stream().unwrap();

        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);

        json_term_writer.push_path_segment("bool");
        json_term_writer.set_fast_value(true);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("complexobject");
        json_term_writer.push_path_segment("field.with.dot");
        json_term_writer.set_fast_value(1i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("date");
        json_term_writer.set_fast_value(DateTime::from_utc(
            OffsetDateTime::parse("1985-04-12T23:20:50.52Z", &Rfc3339).unwrap(),
        ));
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("float");
        json_term_writer.set_fast_value(-0.2f64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("my_arr");
        json_term_writer.set_fast_value(2i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.set_fast_value(3i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.set_fast_value(4i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.push_path_segment("my_key");
        json_term_writer.set_str("tokens");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.set_str("two");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("signed");
        json_term_writer.set_fast_value(-2i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("toto");
        json_term_writer.set_str("titi");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("unsigned");
        json_term_writer.set_fast_value(1i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );
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
        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);
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
        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);
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
        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);
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

    #[test]
    fn test_bug_regression_1629_position_when_array_with_a_field_value_that_does_not_contain_any_token(
    ) {
        // We experienced a bug where we would have a position underflow when computing position
        // delta in an horrible corner case.
        //
        // See the commit with this unit test if you want the details.
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let doc = schema
            .parse_document(r#"{"text": [ "bbb", "aaa", "", "aaa"]}"#)
            .unwrap();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        // On debug this did panic on the underflow
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "aaa");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        // On release this was [2, 1]. (< note the decreasing values)
        assert_eq!(positions, &[2, 5]);
    }

    #[test]
    fn test_multiple_field_value_and_long_tokens() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        // This is a bit of a contrived example.
        let tokens = PreTokenizedString {
            text: "roller-coaster".to_string(),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 14,
                position: 0,
                text: "rollercoaster".to_string(),
                position_length: 2,
            }],
        };
        doc.add_pre_tokenized_text(text, tokens.clone());
        doc.add_pre_tokenized_text(text, tokens);
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "rollercoaster");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, &[0, 3]); //< as opposed to 0, 2 if we had a position length of 1.
    }

    #[test]
    fn test_last_token_not_ending_last() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        // This is a bit of a contrived example.
        let tokens = PreTokenizedString {
            text: "contrived-example".to_string(), //< I can't think of a use case where this corner case happens in real life.
            tokens: vec![
                Token {
                    // Not the last token, yet ends after the last token.
                    offset_from: 0,
                    offset_to: 14,
                    position: 0,
                    text: "long_token".to_string(),
                    position_length: 3,
                },
                Token {
                    offset_from: 0,
                    offset_to: 14,
                    position: 1,
                    text: "short".to_string(),
                    position_length: 1,
                },
            ],
        };
        doc.add_pre_tokenized_text(text, tokens);
        doc.add_text(text, "hello");
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "hello");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, &[4]); //< as opposed to 3 if we had a position length of 1.
    }

    #[test]
    fn test_show_error_when_tokenizer_not_registered() {
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("custom_en")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        Index::create_in_dir(&tempdir_path, schema).unwrap();
        let index = Index::open_in_dir(tempdir_path).unwrap();
        let schema = index.schema();
        let mut index_writer = index.writer(50_000_000).unwrap();
        let title = schema.get_field("title").unwrap();
        let mut document = Document::default();
        document.add_text(title, "The Old Man and the Sea");
        index_writer.add_document(document).unwrap();
        let error = index_writer.commit().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Schema error: 'Error getting tokenizer for field: title'"
        );
    }
}
