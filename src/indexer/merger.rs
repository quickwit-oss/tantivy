use error::{ErrorKind, Result};
use core::SegmentReader;
use core::Segment;
use DocId;
use core::SerializableSegment;
use indexer::SegmentSerializer;
use postings::InvertedIndexSerializer;
use itertools::Itertools;
use postings::Postings;
use docset::DocSet;
use fastfield::DeleteBitSet;
use schema::{Field, Schema};
use termdict::TermMerger;
use fastfield::FastFieldSerializer;
use fastfield::FastFieldReader;
use store::StoreWriter;
use termdict::TermDictionary;
use termdict::TermStreamer;
use schema::FieldType;
use termdict::TermOrdinal;
use schema::Cardinality;
use std::collections::HashMap;
use fastfield::MultiValueIntFastFieldReader;
use std::cmp;

pub struct IndexMerger {
    schema: Schema,
    readers: Vec<SegmentReader>,
    max_doc: u32,
}

fn compute_min_max_val(
    u64_reader: &FastFieldReader<u64>,
    max_doc: DocId,
    delete_bitset: &DeleteBitSet,
) -> Option<(u64, u64)> {
    if max_doc == 0 {
        None
    } else if !delete_bitset.has_deletes() {
        // no deleted documents,
        // we can use the previous min_val, max_val.
        Some((u64_reader.min_value(), u64_reader.max_value()))
    } else {
        // some deleted documents,
        // we need to recompute the max / min
        (0..max_doc)
            .filter(|doc_id| !delete_bitset.is_deleted(*doc_id))
            .map(|doc_id| u64_reader.get(doc_id))
            .minmax()
            .into_option()
    }
}

fn extract_fieldnorm_reader(
    segment_reader: &SegmentReader,
    field: Field,
) -> Option<FastFieldReader<u64>> {
    segment_reader.get_fieldnorms_reader(field)
}

fn extract_fast_field_reader(
    segment_reader: &SegmentReader,
    field: Field,
) -> Option<FastFieldReader<u64>> {
    segment_reader.fast_field_reader(field).ok()
}


struct TermOrdinalMapping {
    per_segment_new_term_ordinals: Vec<Vec<TermOrdinal>>
}

impl TermOrdinalMapping {
    fn new(max_term_ords: Vec<TermOrdinal>) -> TermOrdinalMapping {
        TermOrdinalMapping {
            per_segment_new_term_ordinals: max_term_ords
                .into_iter()
                .map(|max_term_ord| vec![TermOrdinal::default(); max_term_ord as usize])
                .collect()
        }
    }

    fn register_from_to(&mut self,
                        segment_ord: usize,
                        from_ord: TermOrdinal,
                        to_ord: TermOrdinal) {
        self.per_segment_new_term_ordinals[segment_ord][from_ord as usize] = to_ord;
    }

    fn get_segment(&self, segment_ord: usize) -> &[TermOrdinal] {
        &(self.per_segment_new_term_ordinals[segment_ord])[..]
    }

    fn max_term_ord(&self) -> TermOrdinal {
        self.per_segment_new_term_ordinals
            .iter()
            .flat_map(|term_ordinals| {
                term_ordinals.iter().cloned().max()
            })
            .max()
            .unwrap_or(TermOrdinal::default())
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
    pub fn open(schema: Schema, segments: &[Segment]) -> Result<IndexMerger> {
        let mut readers = vec![];
        let mut max_doc: u32 = 0u32;
        for segment in segments {
            if segment.meta().num_docs() > 0 {
                let reader = SegmentReader::open(segment)?;
                max_doc += reader.num_docs();
                readers.push(reader);
            }
        }
        Ok(IndexMerger {
            schema,
            readers,
            max_doc,
        })
    }

    fn write_fieldnorms(&self, fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        for (field_id, field_entry) in self.schema.fields().iter().enumerate() {
            let field = Field(field_id as u32);
            if let FieldType::Str(ref text_options) = *field_entry.field_type() {
                if text_options.get_indexing_options().is_some() {
                    self.write_single_fast_field(
                        field,
                        &extract_fieldnorm_reader,
                        fast_field_serializer,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn write_fast_fields(&self,
                         fast_field_serializer: &mut FastFieldSerializer,
                         mut term_ord_mappings: HashMap<Field, TermOrdinalMapping>) -> Result<()> {
        for (field_id, field_entry) in self.schema.fields().iter().enumerate() {
            let field = Field(field_id as u32);
            let field_type = field_entry.field_type();
            match *field_type {
                FieldType::HierarchicalFacet => {
                    let term_ordinal_mapping = term_ord_mappings
                        .remove(&field)
                        .expect("Logic Error in Tantivy (Please report). HierarchicalFact field should have required a\
                        `term_ordinal_mapping`.");
                    self.write_hierarchical_facet_field(
                            field,
                            term_ordinal_mapping,
                            fast_field_serializer)?;
                }
                FieldType::U64(ref options) | FieldType::I64(ref options) => {
                    match options.get_fastfield_cardinality() {
                        Some(Cardinality::SingleValue) => {
                            self.write_single_fast_field(
                                field,
                                &extract_fast_field_reader,
                                fast_field_serializer
                            )?;
                        }
                        Some(Cardinality::MultiValues) => {
                            self.write_multi_fast_field(field, fast_field_serializer)?;
                        }
                        None => {}
                    }
                }
                FieldType::Str(_) => {
                    // We don't handle str fast field for the moment
                    // They can be implemented using what is done
                    // for facets in the future.
                }
            }
        }
        Ok(())
    }


    // used both to merge field norms, `u64/i64` single fast fields.
    fn write_single_fast_field(
        &self,
        field: Field,
        field_reader_extractor: &Fn(&SegmentReader, Field) -> Option<FastFieldReader<u64>>,
        fast_field_serializer: &mut FastFieldSerializer,
    ) -> Result<()> {

        let mut u64_readers = vec![];
        let mut min_value = u64::max_value();
        let mut max_value = u64::min_value();

        for reader in &self.readers {
            match field_reader_extractor(reader, field) {
                Some(u64_reader) => {
                    if let Some((seg_min_val, seg_max_val)) = compute_min_max_val(
                        &u64_reader,
                        reader.max_doc(),
                        reader.delete_bitset(),
                    ) {
                        // the segment has some non-deleted documents
                        min_value = cmp::min(min_value, seg_min_val);
                        max_value = cmp::max(max_value, seg_max_val);
                        u64_readers.push((
                            reader.max_doc(),
                            u64_reader,
                             reader.delete_bitset(),
                        ));
                    } else {
                        // all documents have been deleted.
                    }
                }
                None => {
                    let error_msg =
                        format!("Failed to find a u64_reader for field {:?}", field);
                    bail!(ErrorKind::SchemaError(error_msg));
                }
            }
        }   

        if min_value > max_value {
            // There is not a single document remaining in the index.
            min_value = 0;
            max_value = 0;
        }

        let mut fast_single_field_serializer =
            fast_field_serializer.new_u64_fast_field(field, min_value, max_value)?;
        for (max_doc, u64_reader, delete_bitset) in u64_readers {
            for doc_id in 0u32..max_doc {
                if !delete_bitset.is_deleted(doc_id) {
                    let val = u64_reader.get(doc_id);
                    fast_single_field_serializer.add_val(val)?;
                }
            }
        }

        fast_single_field_serializer.close_field()?;
        Ok(())
    }

    fn write_multi_fast_field_idx(&self,
                                  field: Field,
                                  fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        let mut total_num_vals = 0u64;

        // In the first pass, we compute the total number of vals.
        //
        // This is required by the bitpacker, as it needs to know
        // what should be the bit length use for bitpacking.
        for reader in &self.readers {
            let multi_ff_reader = reader.multi_fast_field_reader::<u64>(field)?;
            let delete_bitset = reader.delete_bitset();
            if delete_bitset.has_deletes() {
                for doc in 0u32..reader.max_doc() {
                    if !delete_bitset.is_deleted(doc) {
                        total_num_vals += multi_ff_reader.num_vals(doc) as u64;
                    }
                }
            } else {
                total_num_vals += multi_ff_reader.total_num_vals();
            }
        }

        // We can now create our `idx` serializer, and in a second pass,
        // can effectively push the different indexes.
        let mut serialize_idx = fast_field_serializer.new_u64_fast_field_with_idx(field, 0, total_num_vals, 0)?;
        let mut idx = 0;
        for reader in &self.readers {
            let multi_ff_reader = reader.multi_fast_field_reader::<u64>(field)?;
            let delete_bitset = reader.delete_bitset();
            for doc in 0u32..reader.max_doc() {
                if !delete_bitset.is_deleted(doc) {
                    serialize_idx.add_val(idx)?;
                    idx += multi_ff_reader.num_vals(doc) as u64;
                }
            }
        }
        serialize_idx.add_val(idx)?;
        serialize_idx.close_field()?;
        Ok(())
    }

    fn write_hierarchical_facet_field(&self,
                                      field: Field,
                                      term_ordinal_mappings: TermOrdinalMapping,
                                      fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {

        // Multifastfield consists in 2 fastfields.
        // The first serves as an index into the second one and is stricly increasing.
        // The second contains the actual values.

        // First we merge the idx fast field.
        self.write_multi_fast_field_idx(field, fast_field_serializer)?;

        // We can now write the actual fast field values.
        // In the case of hierarchical facets, they are actually term ordinals.
        let max_term_ord = term_ordinal_mappings.max_term_ord();
        {
            let mut serialize_vals = fast_field_serializer.new_u64_fast_field_with_idx(field, 0u64, max_term_ord, 1)?;
            let mut vals = Vec::with_capacity(100);
            for (segment_ord, segment_reader) in self.readers.iter().enumerate() {
                let delete_bitset = segment_reader.delete_bitset();
                let term_ordinal_mapping: &[TermOrdinal] = term_ordinal_mappings.get_segment(segment_ord);
                let ff_reader: MultiValueIntFastFieldReader<u64> = segment_reader.multi_fast_field_reader(field)?;
                // TODO optimize if no deletes
                for doc in 0..segment_reader.max_doc() {
                    if !delete_bitset.is_deleted(doc) {
                        ff_reader.get_vals(doc, &mut vals);
                        for &prev_term_ord in &vals {
                            let new_term_ord = term_ordinal_mapping[prev_term_ord as usize];
                            serialize_vals.add_val(new_term_ord)?;
                        }
                    }
                }
            }
            serialize_vals.close_field()?;
        }
        Ok(())
    }

    fn write_multi_fast_field(&self, field: Field, fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {

        // Multifastfield consists in 2 fastfields.
        // The first serves as an index into the second one and is stricly increasing.
        // The second contains the actual values.

        // First we merge the idx fast field.
        self.write_multi_fast_field_idx(field, fast_field_serializer)?;

        let mut min_value = u64::max_value();
        let mut max_value = u64::min_value();

        let mut vals = Vec::with_capacity(100);


        // Our values are bitpacked and we need to know what should be
        // our bitwidth and our minimum value before serializing any values.
        //
        // Computing those is non-trivial if some documents are deleted.
        // We go through a complete first pass to compute the minimum and the
        // maximum value and initialize our Serializer.
        for reader in &self.readers {
            let ff_reader: MultiValueIntFastFieldReader<u64> = reader.multi_fast_field_reader(field)?;
            let delete_bitset = reader.delete_bitset();
            for doc in 0u32..reader.max_doc() {
                if !delete_bitset.is_deleted(doc) {
                    ff_reader.get_vals(doc, &mut vals);
                    for &val in &vals {
                        min_value = cmp::min(val, min_value);
                        max_value = cmp::max(val, max_value);
                    }
                }
            }
            // TODO optimize when no deletes
        }

        if min_value > max_value {
            min_value = 0;
            max_value = 0;
        }

        // We can now initialize our serializer, and push it the different values
        {
            let mut serialize_vals = fast_field_serializer.new_u64_fast_field_with_idx(field, min_value, max_value, 1)?;
            for reader in &self.readers {
                let delete_bitset = reader.delete_bitset();
                let ff_reader: MultiValueIntFastFieldReader<u64> = reader.multi_fast_field_reader(field)?;
                // TODO optimize if no deletes
                for doc in 0..reader.max_doc() {
                    if !delete_bitset.is_deleted(doc) {
                        ff_reader.get_vals(doc, &mut vals);
                        for &val in &vals {
                            serialize_vals.add_val(val)?;
                        }
                    }
                }
            }
            serialize_vals.close_field()?;
        }
        Ok(())
    }

    fn write_postings_for_field(&self,
                                indexed_field: Field,
                                field_type: &FieldType,
                                serializer: &mut InvertedIndexSerializer) -> Result<Option<TermOrdinalMapping>> {
        let mut delta_computer= DeltaComputer::new();
        let field_readers = self.readers
            .iter()
            .map(|reader| reader.inverted_index(indexed_field))
            .collect::<Vec<_>>();

        let mut field_term_streams = Vec::new();
        let mut max_term_ords: Vec<TermOrdinal> = Vec::new();

        for field_reader in &field_readers {
            let terms = field_reader.terms();
            field_term_streams.push(terms.stream());
            max_term_ords.push(terms.num_terms() as u64);
        }

        let mut term_ord_mapping_opt =
            if *field_type == FieldType::HierarchicalFacet {
                Some(TermOrdinalMapping::new(max_term_ords))
            } else {
                None
            };


        let mut merged_terms = TermMerger::new(field_term_streams);
        let mut max_doc = 0;

        // map from segment doc ids to the resulting merged segment doc id.
        let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> =
            Vec::with_capacity(self.readers.len());

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
        let mut field_serializer = serializer.new_field(indexed_field)?;

        let field_entry = self.schema.get_field_entry(indexed_field);

        // ... set segment postings option the new field.
        let segment_postings_option =
            field_entry.field_type().get_index_record_option().expect(
                "Encountered a field that is not supposed to be
                         indexed. Have you modified the schema?",
            );

        while merged_terms.advance() {
            let term_bytes: &[u8] = merged_terms.key();

            // Let's compute the list of non-empty posting lists
            let segment_postings: Vec<_> = merged_terms
                .current_kvs()
                .iter()
                .flat_map(|heap_item| {
                    let segment_ord = heap_item.segment_ord;
                    let term_info = heap_item.streamer.value();
                    let segment_reader = &self.readers[heap_item.segment_ord];
                    let inverted_index = segment_reader.inverted_index(indexed_field);
                    let mut segment_postings = inverted_index
                        .read_postings_from_terminfo(term_info, segment_postings_option);
                    if segment_postings.advance() {
                        Some((segment_ord, segment_postings))
                    } else {
                        None
                    }
                })
                .collect();

            // At this point, `segment_postings` contains the posting list
            // of all of the segments containing the given term.
            //
            // These segments are non-empty and advance has already been called.

            if !segment_postings.is_empty() {
                // If not, the `term` will be entirely removed.

                // We know that there is at least one document containing
                // the term, so we add it.
                let to_term_ord = field_serializer.new_term(term_bytes)?;

                if let Some(ref mut term_ord_mapping) = term_ord_mapping_opt {
                    for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                        term_ord_mapping.register_from_to(segment_ord,from_term_ord, to_term_ord);
                    }
                }

                // We can now serialize this postings, by pushing each document to the
                // postings serializer.
                for (segment_ord, mut segment_postings) in segment_postings {
                    let old_to_new_doc_id = &merged_doc_id_map[segment_ord];
                    loop {
                        // `.advance()` has been called once before the loop.
                        // Hence we cannot use a `while segment_postings.advance()` loop.
                        if let Some(remapped_doc_id) =
                        old_to_new_doc_id[segment_postings.doc() as usize]
                            {
                                // we make sure to only write the term iff
                                // there is at least one document.
                                let positions: &[u32] = segment_postings.positions();
                                let term_freq = segment_postings.term_freq();
                                let delta_positions = delta_computer.compute_delta(positions);
                                field_serializer.write_doc(
                                    remapped_doc_id,
                                    term_freq,
                                    delta_positions,
                                )?;
                            }
                        if !segment_postings.advance() {
                            break;
                        }
                    }
                }

                // closing the term.
                field_serializer.close_term()?;
            }
        }
        field_serializer.close()?;
        Ok(term_ord_mapping_opt)
    }

    fn write_postings(&self, serializer: &mut InvertedIndexSerializer) -> Result<HashMap<Field, TermOrdinalMapping>> {
        let mut term_ordinal_mappings = HashMap::new();
        for (field_ord, field_entry) in self.schema.fields().iter().enumerate() {
            if field_entry.is_indexed() {
                let indexed_field = Field(field_ord as u32);
                if let Some(term_ordinal_mapping) = self.write_postings_for_field(indexed_field, field_entry.field_type(), serializer)? {
                    term_ordinal_mappings.insert(indexed_field, term_ordinal_mapping);
                }
            }
        }
        Ok(term_ordinal_mappings)
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> Result<()> {
        for reader in &self.readers {
            let store_reader = reader.get_store_reader();
            if reader.num_deleted_docs() > 0 {
                for doc_id in 0..reader.max_doc() {
                    if !reader.is_deleted(doc_id) {
                        let doc = store_reader.get(doc_id)?;
                        store_writer.store(&doc)?;
                    }
                }
            } else {
                store_writer.stack(store_reader)?;
            }
        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> Result<u32> {
        let term_ord_mappings = self.write_postings(serializer.get_postings_serializer())?;
        self.write_fieldnorms(serializer.get_fieldnorms_serializer())?;
        self.write_fast_fields(serializer.get_fast_field_serializer(), term_ord_mappings)?;
        self.write_storable_fields(serializer.get_store_writer())?;
        serializer.close()?;
        Ok(self.max_doc)
    }
}

#[cfg(test)]
mod tests {
    use schema;
    use schema::Document;
    use schema::Term;
    use schema::TextFieldIndexing;
    use query::TermQuery;
    use schema::Field;
    use core::Index;
    use Searcher;
    use DocAddress;
    use collector::tests::FastFieldTestCollector;
    use collector::tests::TestCollector;
    use query::BooleanQuery;
    use schema::IndexRecordOption;
    use schema::Cardinality;
    use futures::Future;
    use IndexWriter;
    use query::AllQuery;
    use collector::FacetCollector;
    use schema::IntOptions;

    #[test]
    fn test_index_merger_no_deletes() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::IntOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                // writing the segment
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "af b");
                    doc.add_u64(score_field, 3);
                    index_writer.add_document(doc);
                }
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c");
                    doc.add_u64(score_field, 5);
                    index_writer.add_document(doc);
                }
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c d");
                    doc.add_u64(score_field, 7);
                    index_writer.add_document(doc);
                }
                index_writer.commit().expect("committed");
            }

            {
                // writing the segment
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "af b");
                    doc.add_u64(score_field, 11);
                    index_writer.add_document(doc);
                }
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c g");
                    doc.add_u64(score_field, 13);
                    index_writer.add_document(doc);
                }
                index_writer.commit().expect("Commit failed");
            }
        }
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let mut collector = TestCollector::default();
                let query = BooleanQuery::new_multiterms_query(terms);
                assert!(searcher.search(&query, &mut collector).is_ok());
                collector.docs()
            };
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "a")]),
                    vec![1, 2, 4]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "af")]),
                    vec![0, 3]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "g")]),
                    vec![4]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "b")]),
                    vec![0, 1, 2, 3, 4]
                );
            }
            {
                let doc = searcher.doc(&DocAddress(0, 0)).unwrap();
                assert_eq!(doc.get_first(text_field).unwrap().text(), "af b");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 1)).unwrap();
                assert_eq!(doc.get_first(text_field).unwrap().text(), "a b c");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 2)).unwrap();
                assert_eq!(doc.get_first(text_field).unwrap().text(), "a b c d");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 3)).unwrap();
                assert_eq!(doc.get_first(text_field).unwrap().text(), "af b");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 4)).unwrap();
                assert_eq!(doc.get_first(text_field).unwrap().text(), "a b c g");
            }
            {
                let get_fast_vals = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    let mut collector = FastFieldTestCollector::for_field(score_field);
                    assert!(searcher.search(&query, &mut collector).is_ok());
                    collector.vals()
                };
                assert_eq!(
                    get_fast_vals(vec![Term::from_field_text(text_field, "a")]),
                    vec![5, 7, 13]
                );
            }
        }
    }

    fn search_term(searcher: &Searcher, term: Term) -> Vec<u64> {
        let mut collector = FastFieldTestCollector::for_field(Field(1));
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        searcher.search(&term_query, &mut collector).unwrap();
        collector.vals()
    }

    #[test]
    fn test_index_merger_with_deletes() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::IntOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();

        let empty_vec = Vec::<u64>::new();

        {
            // a first commit
            index_writer.add_document(doc!(
                    text_field => "a b d",
                    score_field => 1u64
                ));
            index_writer.add_document(doc!(
                    text_field => "b c",
                    score_field => 2u64
                ));
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.add_document(doc!(
                    text_field => "c d",
                    score_field => 3u64
                ));
            index_writer.commit().expect("committed");
            index.load_searchers().unwrap();
            let ref searcher = *index.searcher();
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a")),
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b")),
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c")),
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d")),
                vec![1, 3]
            );
        }
        {
            // a second commit
            index_writer.add_document(doc!(
                    text_field => "a d e",
                    score_field => 4_000u64
                ));
            index_writer.add_document(doc!(
                    text_field => "e f",
                    score_field => 5_000u64
                ));
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.delete_term(Term::from_field_text(text_field, "f"));
            index_writer.add_document(doc!(
                    text_field => "f g",
                    score_field => 6_000u64
                ));
            index_writer.add_document(doc!(
                    text_field => "g h",
                    score_field => 7_000u64
                ));
            index_writer.commit().expect("committed");
            index.load_searchers().unwrap();
            let searcher = index.searcher();

            assert_eq!(searcher.segment_readers().len(), 2);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 1);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(searcher.segment_readers()[1].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[1].max_doc(), 4);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c")),
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d")),
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f")),
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g")),
                vec![6_000, 7_000]
            );

            let score_field_reader = searcher
                .segment_reader(0)
                .fast_field_reader::<u64>(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 1);
            assert_eq!(score_field_reader.max_value(), 3);

            let score_field_reader = searcher
                .segment_reader(1)
                .fast_field_reader::<u64>(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 4000);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // merging the segments
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index.load_searchers().unwrap();
            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c")),
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d")),
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f")),
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g")),
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_field_reader::<u64>(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // test a commit with only deletes
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.commit().unwrap();

            index.load_searchers().unwrap();
            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f")),
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g")),
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_field_reader::<u64>(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // Test merging a single segment in order to remove deletes.
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index.load_searchers().unwrap();

            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 2);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e")),
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f")),
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g")),
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_field_reader::<u64>(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 6000);
            assert_eq!(score_field_reader.max_value(), 7000);
        }

        {
            // Test removing all docs
            index_writer.delete_term(Term::from_field_text(text_field, "g"));
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index.load_searchers().unwrap();

            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 0);
        }
    }


    #[test]
    fn test_merge_facets() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let facet_field = schema_builder.add_facet_field("facet");
        let index = Index::create_in_ram(schema_builder.build());
        use schema::Facet;
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
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
        index.load_searchers().unwrap();
        let test_searcher = |expected_num_docs: usize, expected: &[(&str, u64)]| {
            let searcher = index.searcher();
            let mut facet_collector = FacetCollector::for_field(facet_field);
            facet_collector.add_facet(Facet::from("/top"));
            use collector::{MultiCollector, CountCollector};
            let mut count_collector = CountCollector::default();
            {
                let mut multi_collectors = MultiCollector::from(vec![&mut count_collector, &mut facet_collector]);
                searcher.search(&AllQuery, &mut multi_collectors).unwrap();
            }
            assert_eq!(count_collector.count(), expected_num_docs);
            let facet_counts = facet_collector.harvest();
            let facets: Vec<(String, u64)> = facet_counts.get("/top")
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
        test_searcher(11, &[
            ("/top/a", 5),
            ("/top/b", 5),
            ("/top/c", 2),
            ("/top/d", 2),
            ("/top/e", 2),
            ("/top/f", 1)
        ]);

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();

            index.load_searchers().unwrap();
            test_searcher(11, &[
                ("/top/a", 5),
                ("/top/b", 5),
                ("/top/c", 2),
                ("/top/d", 2),
                ("/top/e", 2),
                ("/top/f", 1)
            ]);
        }

        // Deleting one term
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            let facet = Facet::from_path(vec!["top", "a", "firstdoc"]);
            let facet_term = Term::from_facet(facet_field, &facet);
            index_writer.delete_term(facet_term);
            index_writer.commit().unwrap();
            index.load_searchers().unwrap();
            test_searcher(9, &[
                ("/top/a", 3),
                ("/top/b", 3),
                ("/top/c", 1),
                ("/top/d", 2),
                ("/top/e", 2),
                ("/top/f", 1)
            ]);
        }

    }


    #[test]
    fn test_merge_multivalued_int_fields_all_deleted() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let int_options = IntOptions::default()
            .set_fast(Cardinality::MultiValues)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            let mut doc = Document::default();
            doc.add_u64(int_field,  1);
            index_writer.add_document(doc.clone());
            index_writer.commit().expect("commit failed");
            index_writer.add_document(doc);
            index_writer.commit().expect("commit failed");
            index_writer.delete_term(Term::from_field_u64(int_field, 1));
            index_writer.commit().expect("commit failed");
        }
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        assert_eq!(searcher.num_docs(), 0);
        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();
        }
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        assert_eq!(searcher.num_docs(), 0);
    }

    #[test]
    fn test_merge_multivalued_int_fields() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let int_options = IntOptions::default()
            .set_fast(Cardinality::MultiValues)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());


        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            let index_doc = |index_writer: &mut IndexWriter, int_vals: &[u64]| {
                let mut doc = Document::default();
                for &val in int_vals {
                    doc.add_u64(int_field,  val);
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
            index_writer.commit().expect("committed");

            index_doc(&mut index_writer, &[20]);
            index_writer.commit().expect("committed");

            index_doc(&mut index_writer, &[28, 27]);
            index_doc(&mut index_writer, &[1_000]);

            index_writer.commit().expect("committed");
        }
        index.load_searchers().unwrap();

        let searcher = index.searcher();

        let mut vals: Vec<u64> = Vec::new();

        {
            let segment = searcher.segment_reader(0u32);
            let ff_reader = segment.multi_fast_field_reader(int_field).unwrap();

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
            let ff_reader = segment.multi_fast_field_reader(int_field).unwrap();
            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[20]);
        }

        {
            let segment = searcher.segment_reader(2u32);
            let ff_reader = segment.multi_fast_field_reader(int_field).unwrap();
            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[28, 27]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1_000]);
        }

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();
        }

        index.load_searchers().unwrap();

        {
            let searcher = index.searcher();
            let segment = searcher.segment_reader(0u32);
            let ff_reader = segment.multi_fast_field_reader(int_field).unwrap();

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
            assert_eq!(&vals, &[20]);

            ff_reader.get_vals(8, &mut vals);
            assert_eq!(&vals, &[28, 27]);

            ff_reader.get_vals(9, &mut vals);
            assert_eq!(&vals, &[1_000]);
        }

    }
}

