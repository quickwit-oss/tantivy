//! The inverted index as a [`SegmentPlugin`] implementation.
//!
//! Field norms, the term dictionary, postings, and positions form a single subsystem:
//! field norms are produced by the same tokenization pass that feeds the postings, and
//! postings serialization reads the field norms back. This plugin therefore owns all four
//! files (`fieldnorm`, `term`, `idx`, `pos`) and drives them together, both while indexing
//! (`add_document`) and while merging.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use columnar::MonotonicallyMappableToU64;
use common::JsonPathWriter;
use itertools::Itertools;
use measure_time::debug_time;
use tokenizer_api::BoxTokenStream;

use crate::directory::{CompositeFile, Directory};
use crate::docset::{DocSet, TERMINATED};
use crate::error::DataCorruption;
use crate::fieldnorm::{FieldNormReader, FieldNormReaders, FieldNormsSerializer, FieldNormsWriter};
use crate::index::{Segment, SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::{DocIdMapping, SegmentDocIdMapping};
use crate::indexer::indexing_term::IndexingTerm;
use crate::json_utils::{index_json_value, IndexingPositionsPerPath};
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::postings::{
    compute_initial_table_size, serialize_postings, IndexingContext, IndexingPosition,
    InvertedIndexSerializer, PerFieldPostingsWriter, Postings, PostingsWriter, SegmentPostings,
};
use crate::schema::document::{Document, Value};
use crate::schema::{Field, FieldType, Schema, DATE_TIME_PRECISION_INDEXED};
use crate::space_usage::{ComponentSpaceUsage, FIELDNORMS, POSITIONS, POSTINGS, TERMDICT};
use crate::termdict::{TermMerger, TermOrdinal};
use crate::tokenizer::{FacetTokenizer, PreTokenizedStream, TextAnalyzer, Tokenizer};
use crate::{DocId, InvertedIndexReader, TantivyError};

pub struct InvertedIndexPlugin;

impl SegmentPlugin for InvertedIndexPlugin {
    fn extensions(&self) -> &[&str] {
        &["fieldnorm", "term", "idx", "pos"]
    }

    fn create_writer(&self, _ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        unimplemented!("InvertedIndexPlugin is a built-in; use InvertedIndexPluginWriter::new")
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        // Field norms first: postings merge reads them back from the target segment.
        merge_fieldnorms(&ctx)?;

        debug_time!("write-postings");
        debug!("write-postings");
        let target_segment = ctx.target_segment;
        let mut serializer = InvertedIndexSerializer::open(target_segment)?;
        let fieldnorm_data = target_segment.open_read(SegmentComponent::FieldNorms)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
        write_postings_merge(
            ctx.readers,
            ctx.schema,
            &mut serializer,
            fieldnorm_readers,
            ctx.doc_id_mapping,
        )?;
        serializer.close()?;
        Ok(())
    }

    fn space_usage(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<BTreeMap<String, ComponentSpaceUsage>> {
        let schema = segment_reader.schema();

        let fieldnorms =
            FieldNormReaders::open(segment_reader.open_read(SegmentComponent::FieldNorms)?)?
                .space_usage(schema);
        let termdict = CompositeFile::open(&segment_reader.open_read(SegmentComponent::Terms)?)?
            .space_usage(schema);
        let postings = CompositeFile::open(&segment_reader.open_read(SegmentComponent::Postings)?)?
            .space_usage(schema);
        let positions = match segment_reader.open_read(SegmentComponent::Positions) {
            Ok(file) => CompositeFile::open(&file)?.space_usage(schema),
            Err(_) => CompositeFile::empty().space_usage(schema),
        };
        Ok(BTreeMap::from([
            (
                FIELDNORMS.to_string(),
                ComponentSpaceUsage::PerField(fieldnorms),
            ),
            (
                TERMDICT.to_string(),
                ComponentSpaceUsage::PerField(termdict),
            ),
            (
                POSTINGS.to_string(),
                ComponentSpaceUsage::PerField(postings),
            ),
            (
                POSITIONS.to_string(),
                ComponentSpaceUsage::PerField(positions),
            ),
        ]))
    }
}

// --- Plugin writer ---

pub struct InvertedIndexPluginWriter {
    schema: Schema,
    per_field_postings_writers: PerFieldPostingsWriter,
    per_field_text_analyzers: Vec<TextAnalyzer>,
    fieldnorms_writer: FieldNormsWriter,
    term_buffer: IndexingTerm,
    json_path_writer: JsonPathWriter,
    json_positions_per_path: IndexingPositionsPerPath,
    ctx: IndexingContext,
    postings_serializer: InvertedIndexSerializer,
    fieldnorm_serializer: FieldNormsSerializer,
    max_doc: DocId,
}

impl InvertedIndexPluginWriter {
    pub(crate) fn new(ctx: &PluginWriterContext) -> crate::Result<Self> {
        let segment = ctx.segment;
        let schema = segment.schema();
        let tokenizer_manager = segment.index().tokenizers().clone();

        let per_field_text_analyzers = schema
            .fields()
            .map(|(_, field_entry)| {
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

        let fieldnorm_path = segment.relative_path(SegmentComponent::FieldNorms);
        let fieldnorm_write = segment.index().directory().open_write(&fieldnorm_path)?;

        let table_size = compute_initial_table_size(ctx.memory_budget_in_bytes)?;
        Ok(InvertedIndexPluginWriter {
            per_field_postings_writers: PerFieldPostingsWriter::for_schema(&schema),
            per_field_text_analyzers,
            fieldnorms_writer: FieldNormsWriter::for_schema(&schema),
            term_buffer: IndexingTerm::with_capacity(16),
            json_path_writer: JsonPathWriter::default(),
            json_positions_per_path: IndexingPositionsPerPath::default(),
            ctx: IndexingContext::new(table_size),
            postings_serializer: InvertedIndexSerializer::open(segment)?,
            fieldnorm_serializer: FieldNormsSerializer::from_write(fieldnorm_write)?,
            schema,
            max_doc: 0,
        })
    }

    /// Generic, zero-copy document ingestion. The `SegmentWriter` calls this directly on the
    /// built-in inverted-index writer, so a custom `Document` is tokenized and indexed in
    /// place — no `TantivyDocument` is materialized.
    pub(crate) fn index_document<D: Document>(
        &mut self,
        doc_id: DocId,
        doc: &D,
    ) -> crate::Result<()> {
        let vals_grouped_by_field = doc
            .iter_fields_and_values()
            .sorted_by_key(|(field, _)| *field)
            .chunk_by(|(field, _)| *field);

        for (field, field_values) in &vals_grouped_by_field {
            let values = field_values.map(|el| el.1);

            let field_entry = self.schema.get_field_entry(field);
            let make_schema_error = || {
                TantivyError::SchemaError(format!(
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
            term_buffer.clear_with_field(field);

            match field_entry.field_type() {
                FieldType::Facet(_) => {
                    let mut facet_tokenizer = FacetTokenizer::default(); // this can be global
                    for value in values {
                        let value = value.as_value();

                        let facet_str = value.as_facet().ok_or_else(make_schema_error)?;
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
                        let value = value.as_value();

                        let mut token_stream = if let Some(text) = value.as_str() {
                            let text_analyzer =
                                &mut self.per_field_text_analyzers[field.field_id() as usize];
                            text_analyzer.token_stream(text)
                        } else if let Some(tok_str) = value.into_pre_tokenized_text() {
                            BoxTokenStream::new(PreTokenizedStream::from(*tok_str.clone()))
                        } else {
                            continue;
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
                        let value = value.as_value();

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
                        let value = value.as_value();

                        num_vals += 1;
                        let date_val = value.as_datetime().ok_or_else(make_schema_error)?;
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
                        let value = value.as_value();

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
                        let value = value.as_value();
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
                        let value = value.as_value();
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
                        let value = value.as_value();
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

                    self.json_positions_per_path.clear();
                    self.json_path_writer
                        .set_expand_dots(json_options.is_expand_dots_enabled());
                    for json_value in values {
                        self.json_path_writer.clear();

                        index_json_value(
                            doc_id,
                            json_value,
                            text_analyzer,
                            term_buffer,
                            &mut self.json_path_writer,
                            postings_writer,
                            ctx,
                            &mut self.json_positions_per_path,
                        );
                    }
                }
                FieldType::IpAddr(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();

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
        self.max_doc = doc_id + 1;
        Ok(())
    }
}

impl PluginWriter for InvertedIndexPluginWriter {
    fn serialize(
        mut self: Box<Self>,
        segment: &Segment,
        doc_id_map: Option<&DocIdMapping>,
    ) -> crate::Result<()> {
        // Field norms first: postings serialization reads them back below.
        self.fieldnorms_writer.fill_up_to_max_doc(self.max_doc);
        self.fieldnorms_writer
            .serialize(self.fieldnorm_serializer, doc_id_map)
            .map_err(|e| crate::TantivyError::InternalError(e.to_string()))?;

        let fieldnorm_data = segment.open_read(SegmentComponent::FieldNorms)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
        serialize_postings(
            self.ctx,
            self.schema,
            &self.per_field_postings_writers,
            fieldnorm_readers,
            doc_id_map,
            &mut self.postings_serializer,
        )?;
        self.postings_serializer.close()?;
        Ok(())
    }

    fn mem_usage(&self) -> usize {
        self.ctx.mem_usage() + self.fieldnorms_writer.mem_usage()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// --- Field norm merge ---

fn merge_fieldnorms(ctx: &PluginMergeContext) -> crate::Result<()> {
    let path = ctx
        .target_segment
        .relative_path(SegmentComponent::FieldNorms);
    let write = ctx.target_segment.index().directory().open_write(&path)?;
    let mut serializer = FieldNormsSerializer::from_write(write)?;

    let schema = ctx.schema;
    let fields = FieldNormsWriter::fields_with_fieldnorm(schema);
    let max_doc: usize = ctx
        .readers
        .iter()
        .map(|reader| reader.num_docs() as usize)
        .sum();
    let mut fieldnorms_data = Vec::with_capacity(max_doc);

    for field in fields {
        fieldnorms_data.clear();
        let fieldnorms_readers: Vec<FieldNormReader> = ctx
            .readers
            .iter()
            .map(|reader| reader.get_fieldnorms_reader(field))
            .collect::<Result<_, _>>()?;
        for old_doc_addr in ctx.doc_id_mapping.iter_old_doc_addrs() {
            let reader = &fieldnorms_readers[old_doc_addr.segment_ord as usize];
            let fieldnorm_id = reader.fieldnorm_id(old_doc_addr.doc_id);
            fieldnorms_data.push(fieldnorm_id);
        }
        serializer.serialize_field(field, &fieldnorms_data)?;
    }
    serializer.close()?;
    Ok(())
}

// --- Postings merge helpers (moved from IndexMerger) ---

struct DeltaComputer {
    buffer: Vec<u32>,
}

impl DeltaComputer {
    fn new() -> DeltaComputer {
        DeltaComputer {
            buffer: vec![0u32; 512],
        }
    }

    fn compute_delta(&mut self, positions: &[u32]) -> &[u32] {
        if positions.len() > self.buffer.len() {
            self.buffer.resize(positions.len(), 0u32);
        }
        let mut last_pos = 0u32;
        for (cur_pos, dest) in positions.iter().cloned().zip(self.buffer.iter_mut()) {
            *dest = cur_pos - last_pos;
            last_pos = cur_pos;
        }
        &self.buffer[..positions.len()]
    }
}

fn estimate_total_num_tokens_in_single_segment(
    reader: &SegmentReader,
    field: Field,
) -> crate::Result<u64> {
    if !reader.has_deletes() {
        return Ok(reader.inverted_index(field)?.total_num_tokens());
    }
    if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(field)? {
        let mut count: [usize; 256] = [0; 256];
        for doc in reader.doc_ids_alive() {
            let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
            count[fieldnorm_id as usize] += 1;
        }
        let total_num_tokens = count
            .iter()
            .cloned()
            .enumerate()
            .map(|(fieldnorm_ord, count)| {
                count as u64 * u64::from(FieldNormReader::id_to_fieldnorm(fieldnorm_ord as u8))
            })
            .sum::<u64>();
        return Ok(total_num_tokens);
    }
    let segment_num_tokens = reader.inverted_index(field)?.total_num_tokens();
    if reader.max_doc() == 0 {
        return Ok(0u64);
    }
    let ratio = reader.num_docs() as f64 / reader.max_doc() as f64;
    Ok((segment_num_tokens as f64 * ratio) as u64)
}

fn estimate_total_num_tokens(readers: &[SegmentReader], field: Field) -> crate::Result<u64> {
    let mut total_num_tokens: u64 = 0;
    for reader in readers {
        total_num_tokens += estimate_total_num_tokens_in_single_segment(reader, field)?;
    }
    Ok(total_num_tokens)
}

fn write_postings_for_field(
    readers: &[SegmentReader],
    schema: &Schema,
    indexed_field: Field,
    serializer: &mut InvertedIndexSerializer,
    fieldnorm_reader: Option<FieldNormReader>,
    doc_id_mapping: &SegmentDocIdMapping,
) -> crate::Result<()> {
    debug_time!("write-postings-for-field");
    let mut positions_buffer: Vec<u32> = Vec::with_capacity(1_000);
    let mut delta_computer = DeltaComputer::new();

    let mut max_term_ords: Vec<TermOrdinal> = Vec::new();

    let field_readers: Vec<Arc<InvertedIndexReader>> = readers
        .iter()
        .map(|reader| reader.inverted_index(indexed_field))
        .collect::<crate::Result<Vec<_>>>()?;

    let mut field_term_streams = Vec::new();
    for field_reader in &field_readers {
        let terms = field_reader.terms();
        field_term_streams.push(terms.stream()?);
        max_term_ords.push(terms.num_terms() as u64);
    }

    let mut merged_terms = TermMerger::new(field_term_streams);

    let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> = readers
        .iter()
        .map(|reader| {
            let mut segment_local_map = vec![];
            segment_local_map.resize(reader.max_doc() as usize, None);
            segment_local_map
        })
        .collect();
    for (new_doc_id, old_doc_addr) in doc_id_mapping.iter_old_doc_addrs().enumerate() {
        let segment_map = &mut merged_doc_id_map[old_doc_addr.segment_ord as usize];
        segment_map[old_doc_addr.doc_id as usize] = Some(new_doc_id as DocId);
    }

    let total_num_tokens: u64 = estimate_total_num_tokens(readers, indexed_field)?;

    let mut field_serializer =
        serializer.new_field(indexed_field, total_num_tokens, fieldnorm_reader)?;

    let field_entry = schema.get_field_entry(indexed_field);

    let segment_postings_option = field_entry.field_type().get_index_record_option().expect(
        "Encountered a field that is not supposed to be
                     indexed. Have you modified the schema?",
    );

    let mut segment_postings_containing_the_term: Vec<(usize, SegmentPostings)> = vec![];
    let mut doc_id_and_positions = vec![];

    while merged_terms.advance() {
        segment_postings_containing_the_term.clear();
        let term_bytes: &[u8] = merged_terms.key();

        let mut total_doc_freq = 0;

        for (segment_ord, term_info) in merged_terms.current_segment_ords_and_term_infos() {
            let segment_reader = &readers[segment_ord];
            let inverted_index: &InvertedIndexReader = &field_readers[segment_ord];
            let segment_postings =
                inverted_index.read_postings_from_terminfo(&term_info, segment_postings_option)?;
            let alive_bitset_opt = segment_reader.alive_bitset();
            let doc_freq = if let Some(alive_bitset) = alive_bitset_opt {
                segment_postings.doc_freq_given_deletes(alive_bitset)
            } else {
                segment_postings.doc_freq()
            };
            if doc_freq > 0u32 {
                total_doc_freq += doc_freq;
                segment_postings_containing_the_term.push((segment_ord, segment_postings));
            }
        }

        if total_doc_freq == 0u32 {
            continue;
        }

        assert!(!segment_postings_containing_the_term.is_empty());

        let has_term_freq = {
            let has_term_freq = !segment_postings_containing_the_term[0]
                .1
                .block_cursor
                .freqs()
                .is_empty();
            for (_, postings) in &segment_postings_containing_the_term[1..] {
                if has_term_freq == postings.block_cursor.freqs().is_empty() {
                    return Err(DataCorruption::comment_only(
                        "Term freqs are inconsistent across segments",
                    )
                    .into());
                }
            }
            has_term_freq
        };

        field_serializer.new_term(term_bytes, total_doc_freq, has_term_freq)?;

        for (segment_ord, mut segment_postings) in segment_postings_containing_the_term.drain(..) {
            let old_to_new_doc_id = &merged_doc_id_map[segment_ord];

            let mut doc = segment_postings.doc();
            while doc != TERMINATED {
                if let Some(remapped_doc_id) = old_to_new_doc_id[doc as usize] {
                    let term_freq = if has_term_freq {
                        segment_postings.positions(&mut positions_buffer);
                        segment_postings.term_freq()
                    } else {
                        positions_buffer.clear();
                        0u32
                    };

                    if !doc_id_mapping.is_trivial() {
                        doc_id_and_positions.push((
                            remapped_doc_id,
                            term_freq,
                            positions_buffer.to_vec(),
                        ));
                    } else {
                        let delta_positions = delta_computer.compute_delta(&positions_buffer);
                        field_serializer.write_doc(remapped_doc_id, term_freq, delta_positions);
                    }
                }

                doc = segment_postings.advance();
            }
        }
        if !doc_id_mapping.is_trivial() {
            doc_id_and_positions.sort_unstable_by_key(|&(doc_id, _, _)| doc_id);

            for (doc_id, term_freq, positions) in &doc_id_and_positions {
                let delta_positions = delta_computer.compute_delta(positions);
                field_serializer.write_doc(*doc_id, *term_freq, delta_positions);
            }
            doc_id_and_positions.clear();
        }
        field_serializer.close_term()?;
    }
    field_serializer.close()?;
    Ok(())
}

fn write_postings_merge(
    readers: &[SegmentReader],
    schema: &Schema,
    serializer: &mut InvertedIndexSerializer,
    fieldnorm_readers: FieldNormReaders,
    doc_id_mapping: &SegmentDocIdMapping,
) -> crate::Result<()> {
    for (field, field_entry) in schema.fields() {
        let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
        if field_entry.is_indexed() {
            write_postings_for_field(
                readers,
                schema,
                field,
                serializer,
                fieldnorm_reader,
                doc_id_mapping,
            )?;
        }
    }
    Ok(())
}
