use crate::common::MAX_DOC_LIMIT;
use crate::core::Segment;
use crate::core::SegmentReader;
use crate::core::SerializableSegment;
use crate::docset::{DocSet, TERMINATED};
use crate::fastfield::BytesFastFieldReader;
use crate::fastfield::DeleteBitSet;
use crate::fastfield::FastFieldReader;
use crate::fastfield::FastFieldSerializer;
use crate::fastfield::MultiValuedFastFieldReader;
use crate::fieldnorm::FieldNormsSerializer;
use crate::fieldnorm::FieldNormsWriter;
use crate::fieldnorm::{FieldNormReader, FieldNormReaders};
use crate::indexer::SegmentSerializer;
use crate::postings::Postings;
use crate::postings::{InvertedIndexSerializer, SegmentPostings};
use crate::schema::Cardinality;
use crate::schema::FieldType;
use crate::schema::{Field, Schema};
use crate::store::StoreWriter;
use crate::termdict::TermMerger;
use crate::termdict::TermOrdinal;
use crate::{DocId, InvertedIndexReader, SegmentComponent};
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

fn compute_total_num_tokens(readers: &[SegmentReader], field: Field) -> crate::Result<u64> {
    let mut total_tokens = 0u64;
    let mut count: [usize; 256] = [0; 256];
    for reader in readers {
        if reader.has_deletes() {
            // if there are deletes, then we use an approximation
            // using the fieldnorm
            let fieldnorms_reader = reader.get_fieldnorms_reader(field)?;
            for doc in reader.doc_ids_alive() {
                let fieldnorm_id = fieldnorms_reader.fieldnorm_id(doc);
                count[fieldnorm_id as usize] += 1;
            }
        } else {
            total_tokens += reader.inverted_index(field)?.total_num_tokens();
        }
    }
    Ok(total_tokens
        + count
            .iter()
            .cloned()
            .enumerate()
            .map(|(fieldnorm_ord, count)| {
                count as u64 * u64::from(FieldNormReader::id_to_fieldnorm(fieldnorm_ord as u8))
            })
            .sum::<u64>())
}

pub struct IndexMerger {
    schema: Schema,
    readers: Vec<SegmentReader>,
    max_doc: u32,
}

fn compute_min_max_val(
    u64_reader: &FastFieldReader<u64>,
    max_doc: DocId,
    delete_bitset_opt: Option<&DeleteBitSet>,
) -> Option<(u64, u64)> {
    if max_doc == 0 {
        None
    } else {
        match delete_bitset_opt {
            Some(delete_bitset) => {
                // some deleted documents,
                // we need to recompute the max / min
                crate::common::minmax(
                    (0..max_doc)
                        .filter(|doc_id| delete_bitset.is_alive(*doc_id))
                        .map(|doc_id| u64_reader.get(doc_id)),
                )
            }
            None => {
                // no deleted documents,
                // we can use the previous min_val, max_val.
                Some((u64_reader.min_value(), u64_reader.max_value()))
            }
        }
    }
}

struct TermOrdinalMapping {
    per_segment_new_term_ordinals: Vec<Vec<TermOrdinal>>,
}

impl TermOrdinalMapping {
    fn new(max_term_ords: Vec<TermOrdinal>) -> TermOrdinalMapping {
        TermOrdinalMapping {
            per_segment_new_term_ordinals: max_term_ords
                .into_iter()
                .map(|max_term_ord| vec![TermOrdinal::default(); max_term_ord as usize])
                .collect(),
        }
    }

    fn register_from_to(&mut self, segment_ord: usize, from_ord: TermOrdinal, to_ord: TermOrdinal) {
        self.per_segment_new_term_ordinals[segment_ord][from_ord as usize] = to_ord;
    }

    fn get_segment(&self, segment_ord: usize) -> &[TermOrdinal] {
        &(self.per_segment_new_term_ordinals[segment_ord])[..]
    }

    fn max_term_ord(&self) -> TermOrdinal {
        self.per_segment_new_term_ordinals
            .iter()
            .flat_map(|term_ordinals| term_ordinals.iter().cloned().max())
            .max()
            .unwrap_or_else(TermOrdinal::default)
    }
}

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

impl IndexMerger {
    pub fn open(schema: Schema, segments: &[Segment]) -> crate::Result<IndexMerger> {
        let mut readers = vec![];
        let mut max_doc: u32 = 0u32;
        for segment in segments {
            if segment.meta().num_docs() > 0 {
                let reader = SegmentReader::open(segment)?;
                max_doc += reader.num_docs();
                readers.push(reader);
            }
        }
        if max_doc >= MAX_DOC_LIMIT {
            let err_msg = format!(
                "The segment resulting from this merge would have {} docs,\
                 which exceeds the limit {}.",
                max_doc, MAX_DOC_LIMIT
            );
            return Err(crate::TantivyError::InvalidArgument(err_msg));
        }
        Ok(IndexMerger {
            schema,
            readers,
            max_doc,
        })
    }

    fn write_fieldnorms(
        &self,
        mut fieldnorms_serializer: FieldNormsSerializer,
    ) -> crate::Result<()> {
        let fields = FieldNormsWriter::fields_with_fieldnorm(&self.schema);
        let mut fieldnorms_data = Vec::with_capacity(self.max_doc as usize);
        for field in fields {
            fieldnorms_data.clear();
            for reader in &self.readers {
                let fieldnorms_reader = reader.get_fieldnorms_reader(field)?;
                for doc_id in reader.doc_ids_alive() {
                    let fieldnorm_id = fieldnorms_reader.fieldnorm_id(doc_id);
                    fieldnorms_data.push(fieldnorm_id);
                }
            }
            fieldnorms_serializer.serialize_field(field, &fieldnorms_data[..])?;
        }
        fieldnorms_serializer.close()?;
        Ok(())
    }

    fn write_fast_fields(
        &self,
        fast_field_serializer: &mut FastFieldSerializer,
        mut term_ord_mappings: HashMap<Field, TermOrdinalMapping>,
    ) -> crate::Result<()> {
        for (field, field_entry) in self.schema.fields() {
            let field_type = field_entry.field_type();
            match field_type {
                FieldType::HierarchicalFacet(_) => {
                    let term_ordinal_mapping = term_ord_mappings
                        .remove(&field)
                        .expect("Logic Error in Tantivy (Please report). HierarchicalFact field should have required a\
                        `term_ordinal_mapping`.");
                    self.write_hierarchical_facet_field(
                        field,
                        &term_ordinal_mapping,
                        fast_field_serializer,
                    )?;
                }
                FieldType::U64(ref options)
                | FieldType::I64(ref options)
                | FieldType::F64(ref options)
                | FieldType::Date(ref options) => match options.get_fastfield_cardinality() {
                    Some(Cardinality::SingleValue) => {
                        self.write_single_fast_field(field, fast_field_serializer)?;
                    }
                    Some(Cardinality::MultiValues) => {
                        self.write_multi_fast_field(field, fast_field_serializer)?;
                    }
                    None => {}
                },
                FieldType::Str(_) => {
                    // We don't handle str fast field for the moment
                    // They can be implemented using what is done
                    // for facets in the future.
                }
                FieldType::Bytes(byte_options) => {
                    if byte_options.is_fast() {
                        self.write_bytes_fast_field(field, fast_field_serializer)?;
                    }
                }
            }
        }
        Ok(())
    }

    // used both to merge field norms, `u64/i64` single fast fields.
    fn write_single_fast_field(
        &self,
        field: Field,
        fast_field_serializer: &mut FastFieldSerializer,
    ) -> crate::Result<()> {
        let mut u64_readers = vec![];
        let mut min_value = u64::max_value();
        let mut max_value = u64::min_value();

        for reader in &self.readers {
            let u64_reader: FastFieldReader<u64> = reader
                .fast_fields()
                .typed_fast_field_reader(field)
                .expect("Failed to find a reader for single fast field. This is a tantivy bug and it should never happen.");
            if let Some((seg_min_val, seg_max_val)) =
                compute_min_max_val(&u64_reader, reader.max_doc(), reader.delete_bitset())
            {
                // the segment has some non-deleted documents
                min_value = cmp::min(min_value, seg_min_val);
                max_value = cmp::max(max_value, seg_max_val);
                u64_readers.push((reader.max_doc(), u64_reader, reader.delete_bitset()));
            } else {
                // all documents have been deleted.
            }
        }

        if min_value > max_value {
            // There is not a single document remaining in the index.
            min_value = 0;
            max_value = 0;
        }

        let mut fast_single_field_serializer =
            fast_field_serializer.new_u64_fast_field(field, min_value, max_value)?;
        for (max_doc, u64_reader, delete_bitset_opt) in u64_readers {
            for doc_id in 0u32..max_doc {
                let is_deleted = delete_bitset_opt
                    .map(|delete_bitset| delete_bitset.is_deleted(doc_id))
                    .unwrap_or(false);
                if !is_deleted {
                    let val = u64_reader.get(doc_id);
                    fast_single_field_serializer.add_val(val)?;
                }
            }
        }

        fast_single_field_serializer.close_field()?;
        Ok(())
    }

    fn write_fast_field_idx(
        &self,
        field: Field,
        fast_field_serializer: &mut FastFieldSerializer,
    ) -> crate::Result<()> {
        let mut total_num_vals = 0u64;
        let mut u64s_readers: Vec<MultiValuedFastFieldReader<u64>> = Vec::new();

        // In the first pass, we compute the total number of vals.
        //
        // This is required by the bitpacker, as it needs to know
        // what should be the bit length use for bitpacking.
        for reader in &self.readers {
            let u64s_reader = reader.fast_fields()
                .typed_fast_field_multi_reader(field)
                .expect("Failed to find index for multivalued field. This is a bug in tantivy, please report.");
            if let Some(delete_bitset) = reader.delete_bitset() {
                for doc in 0u32..reader.max_doc() {
                    if delete_bitset.is_alive(doc) {
                        let num_vals = u64s_reader.num_vals(doc) as u64;
                        total_num_vals += num_vals;
                    }
                }
            } else {
                total_num_vals += u64s_reader.total_num_vals();
            }
            u64s_readers.push(u64s_reader);
        }

        // We can now create our `idx` serializer, and in a second pass,
        // can effectively push the different indexes.
        let mut serialize_idx =
            fast_field_serializer.new_u64_fast_field_with_idx(field, 0, total_num_vals, 0)?;
        let mut idx = 0;
        for (segment_reader, u64s_reader) in self.readers.iter().zip(&u64s_readers) {
            for doc in segment_reader.doc_ids_alive() {
                serialize_idx.add_val(idx)?;
                idx += u64s_reader.num_vals(doc) as u64;
            }
        }
        serialize_idx.add_val(idx)?;
        serialize_idx.close_field()?;
        Ok(())
    }

    fn write_hierarchical_facet_field(
        &self,
        field: Field,
        term_ordinal_mappings: &TermOrdinalMapping,
        fast_field_serializer: &mut FastFieldSerializer,
    ) -> crate::Result<()> {
        // Multifastfield consists in 2 fastfields.
        // The first serves as an index into the second one and is stricly increasing.
        // The second contains the actual values.

        // First we merge the idx fast field.
        self.write_fast_field_idx(field, fast_field_serializer)?;

        // We can now write the actual fast field values.
        // In the case of hierarchical facets, they are actually term ordinals.
        let max_term_ord = term_ordinal_mappings.max_term_ord();
        {
            let mut serialize_vals =
                fast_field_serializer.new_u64_fast_field_with_idx(field, 0u64, max_term_ord, 1)?;
            let mut vals = Vec::with_capacity(100);
            for (segment_ord, segment_reader) in self.readers.iter().enumerate() {
                let term_ordinal_mapping: &[TermOrdinal] =
                    term_ordinal_mappings.get_segment(segment_ord);
                let ff_reader: MultiValuedFastFieldReader<u64> = segment_reader
                    .fast_fields()
                    .u64s(field)
                    .expect("Could not find multivalued u64 fast value reader.");
                // TODO optimize if no deletes
                for doc in segment_reader.doc_ids_alive() {
                    ff_reader.get_vals(doc, &mut vals);
                    for &prev_term_ord in &vals {
                        let new_term_ord = term_ordinal_mapping[prev_term_ord as usize];
                        serialize_vals.add_val(new_term_ord)?;
                    }
                }
            }
            serialize_vals.close_field()?;
        }
        Ok(())
    }

    fn write_multi_fast_field(
        &self,
        field: Field,
        fast_field_serializer: &mut FastFieldSerializer,
    ) -> crate::Result<()> {
        // Multifastfield consists in 2 fastfields.
        // The first serves as an index into the second one and is stricly increasing.
        // The second contains the actual values.

        // First we merge the idx fast field.
        self.write_fast_field_idx(field, fast_field_serializer)?;

        let mut min_value = u64::max_value();
        let mut max_value = u64::min_value();

        let mut vals = Vec::with_capacity(100);

        let mut ff_readers = Vec::new();

        // Our values are bitpacked and we need to know what should be
        // our bitwidth and our minimum value before serializing any values.
        //
        // Computing those is non-trivial if some documents are deleted.
        // We go through a complete first pass to compute the minimum and the
        // maximum value and initialize our Serializer.
        for reader in &self.readers {
            let ff_reader: MultiValuedFastFieldReader<u64> = reader
                .fast_fields()
                .typed_fast_field_multi_reader(field)
                .expect(
                    "Failed to find multivalued fast field reader. This is a bug in \
                     tantivy. Please report.",
                );
            for doc in reader.doc_ids_alive() {
                ff_reader.get_vals(doc, &mut vals);
                for &val in &vals {
                    min_value = cmp::min(val, min_value);
                    max_value = cmp::max(val, max_value);
                }
            }
            ff_readers.push(ff_reader);
            // TODO optimize when no deletes
        }

        if min_value > max_value {
            min_value = 0;
            max_value = 0;
        }

        // We can now initialize our serializer, and push it the different values
        {
            let mut serialize_vals = fast_field_serializer
                .new_u64_fast_field_with_idx(field, min_value, max_value, 1)?;
            for (reader, ff_reader) in self.readers.iter().zip(ff_readers) {
                // TODO optimize if no deletes
                for doc in reader.doc_ids_alive() {
                    ff_reader.get_vals(doc, &mut vals);
                    for &val in &vals {
                        serialize_vals.add_val(val)?;
                    }
                }
            }
            serialize_vals.close_field()?;
        }
        Ok(())
    }

    fn write_bytes_fast_field(
        &self,
        field: Field,
        fast_field_serializer: &mut FastFieldSerializer,
    ) -> crate::Result<()> {
        let mut total_num_vals = 0u64;
        let mut bytes_readers: Vec<BytesFastFieldReader> = Vec::new();

        for reader in &self.readers {
            let bytes_reader = reader.fast_fields().bytes(field)?;
            if let Some(delete_bitset) = reader.delete_bitset() {
                for doc in 0u32..reader.max_doc() {
                    if delete_bitset.is_alive(doc) {
                        let num_vals = bytes_reader.get_bytes(doc).len() as u64;
                        total_num_vals += num_vals;
                    }
                }
            } else {
                total_num_vals += bytes_reader.total_num_bytes() as u64;
            }
            bytes_readers.push(bytes_reader);
        }

        {
            // We can now create our `idx` serializer, and in a second pass,
            // can effectively push the different indexes.
            let mut serialize_idx =
                fast_field_serializer.new_u64_fast_field_with_idx(field, 0, total_num_vals, 0)?;
            let mut idx = 0;
            for (segment_reader, bytes_reader) in self.readers.iter().zip(&bytes_readers) {
                for doc in segment_reader.doc_ids_alive() {
                    serialize_idx.add_val(idx)?;
                    idx += bytes_reader.get_bytes(doc).len() as u64;
                }
            }
            serialize_idx.add_val(idx)?;
            serialize_idx.close_field()?;
        }

        let mut serialize_vals = fast_field_serializer.new_bytes_fast_field_with_idx(field, 1);
        for segment_reader in &self.readers {
            let bytes_reader = segment_reader.fast_fields().bytes(field)
                .expect("Failed to find bytes field in fast field reader. This is a bug in tantivy. Please report.");
            // TODO: optimize if no deletes
            for doc in segment_reader.doc_ids_alive() {
                let val = bytes_reader.get_bytes(doc);
                serialize_vals.write_all(val)?;
            }
        }
        serialize_vals.flush()?;
        Ok(())
    }

    fn write_postings_for_field(
        &self,
        indexed_field: Field,
        field_type: &FieldType,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> crate::Result<Option<TermOrdinalMapping>> {
        let mut positions_buffer: Vec<u32> = Vec::with_capacity(1_000);
        let mut delta_computer = DeltaComputer::new();

        let mut max_term_ords: Vec<TermOrdinal> = Vec::new();

        let field_readers: Vec<Arc<InvertedIndexReader>> = self
            .readers
            .iter()
            .map(|reader| reader.inverted_index(indexed_field))
            .collect::<crate::Result<Vec<_>>>()?;

        let mut field_term_streams = Vec::new();
        for field_reader in &field_readers {
            let terms = field_reader.terms();
            field_term_streams.push(terms.stream()?);
            max_term_ords.push(terms.num_terms() as u64);
        }

        let mut term_ord_mapping_opt = match field_type {
            FieldType::HierarchicalFacet(_) => Some(TermOrdinalMapping::new(max_term_ords)),
            _ => None,
        };

        let mut merged_terms = TermMerger::new(field_term_streams);
        let mut max_doc = 0;

        // map from segment doc ids to the resulting merged segment doc id.
        let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> = Vec::with_capacity(self.readers.len());

        for reader in &self.readers {
            let mut segment_local_map = Vec::with_capacity(reader.max_doc() as usize);
            for doc_id in 0..reader.max_doc() {
                if reader.is_deleted(doc_id) {
                    segment_local_map.push(None);
                } else {
                    segment_local_map.push(Some(max_doc));
                    max_doc += 1u32;
                }
            }
            merged_doc_id_map.push(segment_local_map);
        }

        // The total number of tokens will only be exact when there has been no deletes.
        //
        // Otherwise, we approximate by removing deleted documents proportionally.
        let total_num_tokens: u64 = compute_total_num_tokens(&self.readers, indexed_field)?;

        // Create the total list of doc ids
        // by stacking the doc ids from the different segment.
        //
        // In the new segments, the doc id from the different
        // segment are stacked so that :
        // - Segment 0's doc ids become doc id [0, seg.max_doc]
        // - Segment 1's doc ids become  [seg0.max_doc, seg0.max_doc + seg.max_doc]
        // - Segment 2's doc ids become  [seg0.max_doc + seg1.max_doc,
        //                                seg0.max_doc + seg1.max_doc + seg2.max_doc]
        // ...
        let mut field_serializer =
            serializer.new_field(indexed_field, total_num_tokens, fieldnorm_reader)?;

        let field_entry = self.schema.get_field_entry(indexed_field);

        // ... set segment postings option the new field.
        let segment_postings_option = field_entry.field_type().get_index_record_option().expect(
            "Encountered a field that is not supposed to be
                         indexed. Have you modified the schema?",
        );

        let mut segment_postings_containing_the_term: Vec<(usize, SegmentPostings)> = vec![];

        while merged_terms.advance() {
            segment_postings_containing_the_term.clear();
            let term_bytes: &[u8] = merged_terms.key();

            let mut total_doc_freq = 0;

            // Let's compute the list of non-empty posting lists
            for heap_item in merged_terms.current_kvs() {
                let segment_ord = heap_item.segment_ord;
                let term_info = heap_item.streamer.value();
                let segment_reader = &self.readers[heap_item.segment_ord];
                let inverted_index: &InvertedIndexReader = &*field_readers[segment_ord];
                let segment_postings = inverted_index
                    .read_postings_from_terminfo(term_info, segment_postings_option)?;
                let delete_bitset_opt = segment_reader.delete_bitset();
                let doc_freq = if let Some(delete_bitset) = delete_bitset_opt {
                    segment_postings.doc_freq_given_deletes(delete_bitset)
                } else {
                    segment_postings.doc_freq()
                };
                if doc_freq > 0u32 {
                    total_doc_freq += doc_freq;
                    segment_postings_containing_the_term.push((segment_ord, segment_postings));
                }
            }

            // At this point, `segment_postings` contains the posting list
            // of all of the segments containing the given term (and that are non-empty)
            //
            // These segments are non-empty and advance has already been called.
            if total_doc_freq == 0u32 {
                // All docs that used to contain the term have been deleted. The `term` will be
                // entirely removed.
                continue;
            }

            let to_term_ord = field_serializer.new_term(term_bytes, total_doc_freq)?;

            if let Some(ref mut term_ord_mapping) = term_ord_mapping_opt {
                for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                    term_ord_mapping.register_from_to(segment_ord, from_term_ord, to_term_ord);
                }
            }

            // We can now serialize this postings, by pushing each document to the
            // postings serializer.
            for (segment_ord, mut segment_postings) in
                segment_postings_containing_the_term.drain(..)
            {
                let old_to_new_doc_id = &merged_doc_id_map[segment_ord];

                let mut doc = segment_postings.doc();
                while doc != TERMINATED {
                    // deleted doc are skipped as they do not have a `remapped_doc_id`.
                    if let Some(remapped_doc_id) = old_to_new_doc_id[doc as usize] {
                        // we make sure to only write the term iff
                        // there is at least one document.
                        let term_freq = segment_postings.term_freq();
                        segment_postings.positions(&mut positions_buffer);

                        let delta_positions = delta_computer.compute_delta(&positions_buffer);
                        field_serializer.write_doc(remapped_doc_id, term_freq, delta_positions)?;
                    }

                    doc = segment_postings.advance();
                }
            }

            // closing the term.
            field_serializer.close_term()?;
        }
        field_serializer.close()?;
        Ok(term_ord_mapping_opt)
    }

    fn write_postings(
        &self,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_readers: FieldNormReaders,
    ) -> crate::Result<HashMap<Field, TermOrdinalMapping>> {
        let mut term_ordinal_mappings = HashMap::new();
        for (field, field_entry) in self.schema.fields() {
            let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
            if field_entry.is_indexed() {
                if let Some(term_ordinal_mapping) = self.write_postings_for_field(
                    field,
                    field_entry.field_type(),
                    serializer,
                    fieldnorm_reader,
                )? {
                    term_ordinal_mappings.insert(field, term_ordinal_mapping);
                }
            }
        }
        Ok(term_ordinal_mappings)
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> crate::Result<()> {
        for reader in &self.readers {
            let store_reader = reader.get_store_reader()?;
            if reader.num_deleted_docs() > 0 {
                for doc_id in reader.doc_ids_alive() {
                    let doc = store_reader.get(doc_id)?;
                    store_writer.store(&doc)?;
                }
            } else {
                store_writer.stack(&store_reader)?;
            }
        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> crate::Result<u32> {
        if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
            self.write_fieldnorms(fieldnorms_serializer)?;
        }
        let fieldnorm_data = serializer
            .segment()
            .open_read(SegmentComponent::FIELDNORMS)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
        let term_ord_mappings =
            self.write_postings(serializer.get_postings_serializer(), fieldnorm_readers)?;
        self.write_fast_fields(serializer.get_fast_field_serializer(), term_ord_mappings)?;
        self.write_storable_fields(serializer.get_store_writer())?;
        serializer.close()?;
        Ok(self.max_doc)
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_nearly_equals;
    use crate::collector::tests::TEST_COLLECTOR_WITH_SCORE;
    use crate::collector::tests::{BytesFastFieldTestCollector, FastFieldTestCollector};
    use crate::collector::{Count, FacetCollector};
    use crate::core::Index;
    use crate::query::AllQuery;
    use crate::query::BooleanQuery;
    use crate::query::Scorer;
    use crate::query::TermQuery;
    use crate::schema::Document;
    use crate::schema::Facet;
    use crate::schema::IndexRecordOption;
    use crate::schema::IntOptions;
    use crate::schema::Term;
    use crate::schema::TextFieldIndexing;
    use crate::schema::INDEXED;
    use crate::schema::{Cardinality, TEXT};
    use crate::DocAddress;
    use crate::IndexWriter;
    use crate::Searcher;
    use crate::{schema, DocSet, SegmentId};
    use byteorder::{BigEndian, ReadBytesExt};
    use futures::executor::block_on;
    use schema::FAST;

    #[test]
    fn test_index_merger_no_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let score_fieldtype = schema::IntOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader()?;
        let curr_time = chrono::Utc::now();
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                score_field => 3u64,
                date_field => curr_time,
                bytes_score_field => 3u32.to_be_bytes().as_ref()
            ));

            index_writer.add_document(doc!(
                text_field => "a b c",
                score_field => 5u64,
                bytes_score_field => 5u32.to_be_bytes().as_ref()
            ));
            index_writer.add_document(doc!(
                text_field => "a b c d",
                score_field => 7u64,
                bytes_score_field => 7u32.to_be_bytes().as_ref()
            ));
            index_writer.commit()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                date_field => curr_time,
                score_field => 11u64,
                bytes_score_field => 11u32.to_be_bytes().as_ref()
            ));
            index_writer.add_document(doc!(
                text_field => "a b c g",
                score_field => 13u64,
                bytes_score_field => 13u32.to_be_bytes().as_ref()
            ));
            index_writer.commit()?;
        }
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests()?;
            block_on(index_writer.merge(&segment_ids))?;
            index_writer.wait_merging_threads()?;
        }
        {
            reader.reload()?;
            let searcher = reader.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = BooleanQuery::new_multiterms_query(terms);
                searcher
                    .search(&query, &TEST_COLLECTOR_WITH_SCORE)
                    .map(|top_docs| top_docs.docs().to_vec())
            };
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "a")])?,
                    vec![
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "af")])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "g")])?,
                    vec![DocAddress::new(0, 4)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "b")])?,
                    vec![
                        DocAddress::new(0, 0),
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 3),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_date(date_field, &curr_time)])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 0))?;
                assert_eq!(doc.get_first(text_field).unwrap().text(), Some("af b"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 1))?;
                assert_eq!(doc.get_first(text_field).unwrap().text(), Some("a b c"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 2))?;
                assert_eq!(doc.get_first(text_field).unwrap().text(), Some("a b c d"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 3))?;
                assert_eq!(doc.get_first(text_field).unwrap().text(), Some("af b"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 4))?;
                assert_eq!(doc.get_first(text_field).unwrap().text(), Some("a b c g"));
            }
            {
                let get_fast_vals = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(&query, &FastFieldTestCollector::for_field(score_field))
                };
                let get_fast_vals_bytes = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(
                        &query,
                        &BytesFastFieldTestCollector::for_field(bytes_score_field),
                    )
                };
                assert_eq!(
                    get_fast_vals(vec![Term::from_field_text(text_field, "a")])?,
                    vec![5, 7, 13]
                );
                assert_eq!(
                    get_fast_vals_bytes(vec![Term::from_field_text(text_field, "a")])?,
                    vec![0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 13]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_index_merger_with_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::IntOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests()?;
        let reader = index.reader().unwrap();
        let search_term = |searcher: &Searcher, term: Term| {
            let collector = FastFieldTestCollector::for_field(score_field);
            let bytes_collector = BytesFastFieldTestCollector::for_field(bytes_score_field);
            let term_query = TermQuery::new(term, IndexRecordOption::Basic);
            searcher
                .search(&term_query, &(collector, bytes_collector))
                .map(|(scores, bytes)| {
                    let mut score_bytes = &bytes[..];
                    for &score in &scores {
                        assert_eq!(score as u32, score_bytes.read_u32::<BigEndian>().unwrap());
                    }
                    scores
                })
        };

        let empty_vec = Vec::<u64>::new();
        {
            // a first commit
            index_writer.add_document(doc!(
                text_field => "a b d",
                score_field => 1u64,
                bytes_score_field => vec![0u8, 0, 0, 1],
            ));
            index_writer.add_document(doc!(
                text_field => "b c",
                score_field => 2u64,
                bytes_score_field => vec![0u8, 0, 0, 2],
            ));
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.add_document(doc!(
                text_field => "c d",
                score_field => 3u64,
                bytes_score_field => vec![0u8, 0, 0, 3],
            ));
            index_writer.commit()?;
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![1, 3]
            );
        }
        {
            // a second commit
            index_writer.add_document(doc!(
                text_field => "a d e",
                score_field => 4_000u64,
                bytes_score_field => vec![0u8, 0, 0, 4],
            ));
            index_writer.add_document(doc!(
                text_field => "e f",
                score_field => 5_000u64,
                bytes_score_field => vec![0u8, 0, 0, 5],
            ));
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.delete_term(Term::from_field_text(text_field, "f"));
            index_writer.add_document(doc!(
                text_field => "f g",
                score_field => 6_000u64,
                bytes_score_field => vec![0u8, 0, 23, 112],
            ));
            index_writer.add_document(doc!(
                text_field => "g h",
                score_field => 7_000u64,
                bytes_score_field => vec![0u8, 0, 27, 88],
            ));
            index_writer.commit()?;
            reader.reload()?;
            let searcher = reader.searcher();

            assert_eq!(searcher.segment_readers().len(), 2);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 4);
            assert_eq!(searcher.segment_readers()[1].num_docs(), 1);
            assert_eq!(searcher.segment_readers()[1].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );

            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 4000);
            assert_eq!(score_field_reader.max_value(), 7000);

            let score_field_reader = searcher
                .segment_reader(1)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 1);
            assert_eq!(score_field_reader.max_value(), 3);
        }
        {
            // merging the segments
            let segment_ids = index.searchable_segment_ids()?;
            block_on(index_writer.merge(&segment_ids))?;
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // test a commit with only deletes
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.commit()?;

            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // Test merging a single segment in order to remove deletes.
            let segment_ids = index.searchable_segment_ids()?;
            block_on(index_writer.merge(&segment_ids))?;
            reader.reload()?;

            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 2);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 6000);
            assert_eq!(score_field_reader.max_value(), 7000);
        }

        {
            // Test removing all docs
            index_writer.delete_term(Term::from_field_text(text_field, "g"));
            index_writer.commit()?;
            let segment_ids = index.searchable_segment_ids()?;
            reader.reload()?;

            let searcher = reader.searcher();
            assert!(segment_ids.is_empty());
            assert!(searcher.segment_readers().is_empty());
            assert_eq!(searcher.num_docs(), 0);
        }
        Ok(())
    }

    #[test]
    fn test_merge_facets() {
        let mut schema_builder = schema::Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader().unwrap();
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let index_doc = |index_writer: &mut IndexWriter, doc_facets: &[&str]| {
                let mut doc = Document::default();
                for facet in doc_facets {
                    doc.add_facet(facet_field, Facet::from(facet));
                }
                index_writer.add_document(doc);
            };

            index_doc(&mut index_writer, &["/top/a/firstdoc", "/top/b"]);
            index_doc(&mut index_writer, &["/top/a/firstdoc", "/top/b", "/top/c"]);
            index_doc(&mut index_writer, &["/top/a", "/top/b"]);
            index_doc(&mut index_writer, &["/top/a"]);

            index_doc(&mut index_writer, &["/top/b", "/top/d"]);
            index_doc(&mut index_writer, &["/top/d"]);
            index_doc(&mut index_writer, &["/top/e"]);
            index_writer.commit().expect("committed");

            index_doc(&mut index_writer, &["/top/a"]);
            index_doc(&mut index_writer, &["/top/b"]);
            index_doc(&mut index_writer, &["/top/c"]);
            index_writer.commit().expect("committed");

            index_doc(&mut index_writer, &["/top/e", "/top/f"]);
            index_writer.commit().expect("committed");
        }

        reader.reload().unwrap();
        let test_searcher = |expected_num_docs: usize, expected: &[(&str, u64)]| {
            let searcher = reader.searcher();
            let mut facet_collector = FacetCollector::for_field(facet_field);
            facet_collector.add_facet(Facet::from("/top"));
            let (count, facet_counts) = searcher
                .search(&AllQuery, &(Count, facet_collector))
                .unwrap();
            assert_eq!(count, expected_num_docs);
            let facets: Vec<(String, u64)> = facet_counts
                .get("/top")
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(
                facets,
                expected
                    .iter()
                    .map(|&(facet_str, count)| (String::from(facet_str), count))
                    .collect::<Vec<_>>()
            );
        };
        test_searcher(
            11,
            &[
                ("/top/a", 5),
                ("/top/b", 5),
                ("/top/c", 2),
                ("/top/d", 2),
                ("/top/e", 2),
                ("/top/f", 1),
            ],
        );
        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            block_on(index_writer.merge(&segment_ids)).expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();
            reader.reload().unwrap();
            test_searcher(
                11,
                &[
                    ("/top/a", 5),
                    ("/top/b", 5),
                    ("/top/c", 2),
                    ("/top/d", 2),
                    ("/top/e", 2),
                    ("/top/f", 1),
                ],
            );
        }

        // Deleting one term
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let facet = Facet::from_path(vec!["top", "a", "firstdoc"]);
            let facet_term = Term::from_facet(facet_field, &facet);
            index_writer.delete_term(facet_term);
            index_writer.commit().unwrap();
            reader.reload().unwrap();
            test_searcher(
                9,
                &[
                    ("/top/a", 3),
                    ("/top/b", 3),
                    ("/top/c", 1),
                    ("/top/d", 2),
                    ("/top/e", 2),
                    ("/top/f", 1),
                ],
            );
        }
    }

    #[test]
    fn test_bug_merge() {
        let mut schema_builder = schema::Schema::builder();
        let int_field = schema_builder.add_u64_field("intvals", INDEXED);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(int_field => 1u64));
        index_writer.commit().expect("commit failed");
        index_writer.add_document(doc!(int_field => 1u64));
        index_writer.commit().expect("commit failed");
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
        index_writer.delete_term(Term::from_field_u64(int_field, 1));
        let segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");
        block_on(index_writer.merge(&segment_ids)).expect("Merging failed");
        reader.reload().unwrap();
        // commit has not been called yet. The document should still be
        // there.
        assert_eq!(reader.searcher().num_docs(), 2);
    }

    #[test]
    fn test_merge_multivalued_int_fields_all_deleted() {
        let mut schema_builder = schema::Schema::builder();
        let int_options = IntOptions::default()
            .set_fast(Cardinality::MultiValues)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader().unwrap();
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let mut doc = Document::default();
            doc.add_u64(int_field, 1);
            index_writer.add_document(doc.clone());
            assert!(index_writer.commit().is_ok());
            index_writer.add_document(doc);
            assert!(index_writer.commit().is_ok());
            index_writer.delete_term(Term::from_field_u64(int_field, 1));

            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            assert!(block_on(index_writer.merge(&segment_ids)).is_ok());

            // assert delete has not been committed
            assert!(reader.reload().is_ok());
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);

            index_writer.commit().unwrap();

            index_writer.wait_merging_threads().unwrap();
        }

        reader.reload().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 0);
    }

    #[test]
    fn test_merge_multivalued_int_fields_simple() {
        let mut schema_builder = schema::Schema::builder();
        let int_options = IntOptions::default()
            .set_fast(Cardinality::MultiValues)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let index_doc = |index_writer: &mut IndexWriter, int_vals: &[u64]| {
                let mut doc = Document::default();
                for &val in int_vals {
                    doc.add_u64(int_field, val);
                }
                index_writer.add_document(doc);
            };
            index_doc(&mut index_writer, &[1, 2]);
            index_doc(&mut index_writer, &[1, 2, 3]);
            index_doc(&mut index_writer, &[4, 5]);
            index_doc(&mut index_writer, &[1, 2]);
            index_doc(&mut index_writer, &[1, 5]);
            index_doc(&mut index_writer, &[3]);
            index_doc(&mut index_writer, &[17]);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, &[20]);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, &[28, 27]);
            index_doc(&mut index_writer, &[1_000]);
            assert!(index_writer.commit().is_ok());
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let mut vals: Vec<u64> = Vec::new();

        {
            let segment = searcher.segment_reader(0u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();

            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1, 2, 3]);

            ff_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[4, 5]);

            ff_reader.get_vals(3, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(4, &mut vals);
            assert_eq!(&vals, &[1, 5]);

            ff_reader.get_vals(5, &mut vals);
            assert_eq!(&vals, &[3]);

            ff_reader.get_vals(6, &mut vals);
            assert_eq!(&vals, &[17]);
        }

        {
            let segment = searcher.segment_reader(1u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();
            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[28, 27]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1_000]);
        }

        {
            let segment = searcher.segment_reader(2u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();
            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[20]);
        }

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            assert!(block_on(index_writer.merge(&segment_ids)).is_ok());
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        assert!(reader.reload().is_ok());

        {
            let searcher = reader.searcher();
            let segment = searcher.segment_reader(0u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();

            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1, 2, 3]);

            ff_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[4, 5]);

            ff_reader.get_vals(3, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(4, &mut vals);
            assert_eq!(&vals, &[1, 5]);

            ff_reader.get_vals(5, &mut vals);
            assert_eq!(&vals, &[3]);

            ff_reader.get_vals(6, &mut vals);
            assert_eq!(&vals, &[17]);

            ff_reader.get_vals(7, &mut vals);
            assert_eq!(&vals, &[28, 27]);

            ff_reader.get_vals(8, &mut vals);
            assert_eq!(&vals, &[1_000]);

            ff_reader.get_vals(9, &mut vals);
            assert_eq!(&vals, &[20]);
        }
    }

    #[test]
    fn merges_f64_fast_fields_correctly() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let fast_multi = IntOptions::default().set_fast(Cardinality::MultiValues);

        let field = builder.add_f64_field("f64", schema::FAST);
        let multi_field = builder.add_f64_field("f64s", fast_multi);

        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_for_tests()?;

        // Make sure we'll attempt to merge every created segment
        let mut policy = crate::indexer::LogMergePolicy::default();
        policy.set_min_merge_size(2);
        writer.set_merge_policy(Box::new(policy));

        for i in 0..100 {
            let mut doc = Document::new();
            doc.add_f64(field, 42.0);
            doc.add_f64(multi_field, 0.24);
            doc.add_f64(multi_field, 0.27);
            writer.add_document(doc);
            if i % 5 == 0 {
                writer.commit()?;
            }
        }

        writer.commit()?;
        writer.wait_merging_threads()?;

        // If a merging thread fails, we should end up with more
        // than one segment here
        assert_eq!(1, index.searchable_segments()?.len());
        Ok(())
    }

    #[test]
    fn test_merged_index_has_blockwand() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();
        let text = builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_for_tests()?;
        let happy_term = Term::from_field_text(text, "happy");
        let term_query = TermQuery::new(happy_term, IndexRecordOption::WithFreqs);
        for _ in 0..62 {
            writer.add_document(doc!(text=>"hello happy tax payer"));
        }
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut term_scorer = term_query
            .specialized_weight(&searcher, true)?
            .specialized_scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(term_scorer.doc(), 0);
        assert_nearly_equals!(term_scorer.block_max_score(), 0.0079681855);
        assert_nearly_equals!(term_scorer.score(), 0.0079681855);
        for _ in 0..81 {
            writer.add_document(doc!(text=>"hello happy tax payer"));
        }
        writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();

        assert_eq!(searcher.segment_readers().len(), 2);
        for segment_reader in searcher.segment_readers() {
            let mut term_scorer = term_query
                .specialized_weight(&searcher, true)?
                .specialized_scorer(segment_reader, 1.0)?;
            // the difference compared to before is instrinsic to the bm25 formula. no worries there.
            for doc in segment_reader.doc_ids_alive() {
                assert_eq!(term_scorer.doc(), doc);
                assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
                assert_nearly_equals!(term_scorer.score(), 0.003478312);
                term_scorer.advance();
            }
        }

        let segment_ids: Vec<SegmentId> = searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect();
        block_on(writer.merge(&segment_ids[..]))?;

        reader.reload()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0u32);
        let mut term_scorer = term_query
            .specialized_weight(&searcher, true)?
            .specialized_scorer(segment_reader, 1.0)?;
        // the difference compared to before is instrinsic to the bm25 formula. no worries there.
        for doc in segment_reader.doc_ids_alive() {
            assert_eq!(term_scorer.doc(), doc);
            assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
            assert_nearly_equals!(term_scorer.score(), 0.003478312);
            term_scorer.advance();
        }

        Ok(())
    }
}
