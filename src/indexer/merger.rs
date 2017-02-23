use Result;
use core::SegmentReader;
use core::Segment;
use DocId;
use core::SerializableSegment;
use schema::FieldValue;
use indexer::SegmentSerializer;
use postings::PostingsSerializer;
use fastfield::U32FastFieldReader;
use itertools::Itertools;
use postings::Postings;
use postings::DocSet;
use core::TermIterator;
use fastfield::delete::DeleteBitSet;
use schema::{Schema, Field};
use fastfield::FastFieldSerializer;
use store::StoreWriter;
use core::SegmentInfo;
use std::cmp::{min, max};
use std::iter;


pub struct IndexMerger {
    schema: Schema,
    readers: Vec<SegmentReader>,
    segment_info: SegmentInfo,
}


struct DeltaPositionComputer {
    buffer: Vec<u32>,
}

impl DeltaPositionComputer {
    fn new() -> DeltaPositionComputer {
        DeltaPositionComputer { buffer: iter::repeat(0u32).take(512).collect::<Vec<u32>>() }
    }

    fn compute_delta_positions(&mut self, positions: &[u32]) -> &[u32] {
        if positions.len() > self.buffer.len() {
            self.buffer.resize(positions.len(), 0u32);
        }
        let mut last_pos = 0u32;
        for (i, position) in positions.iter().cloned().enumerate() {
            self.buffer[i] = position - last_pos;
            last_pos = position;
        }
        &self.buffer[..positions.len()]
    }
}


fn compute_min_max_val(u32_reader: &U32FastFieldReader, max_doc: DocId, delete_bitset: &DeleteBitSet) -> Option<(u32, u32)> {
    if max_doc == 0 {
        None
    }
    else if !delete_bitset.has_deletes() {
        // no deleted documents, 
        // we can use the previous min_val, max_val.
        Some((u32_reader.min_val(), u32_reader.max_val()))
    }
    else {
        // some deleted documents,
        // we need to recompute the max / min
        (0..max_doc)
            .filter(|doc_id| !delete_bitset.is_deleted(*doc_id))
            .map(|doc_id| u32_reader.get(doc_id))
            .minmax()
            .into_option()
    }
}

fn extract_fieldnorm_reader(segment_reader: &SegmentReader, field: Field) -> Result<U32FastFieldReader> {
    Ok(segment_reader.get_fieldnorms_reader(field)?)
}

fn extract_fast_field_reader(segment_reader: &SegmentReader, field: Field) -> Result<U32FastFieldReader> {
    segment_reader.get_fast_field_reader(field)
}

impl IndexMerger {
    pub fn open(schema: Schema, segments: &[Segment]) -> Result<IndexMerger> {
        let mut readers = vec!();
        let mut max_doc = 0;
        for segment in segments {
            if segment.meta().num_docs() > 0 {
                let reader = SegmentReader::open(segment.clone())?;
                max_doc += reader.num_docs();
                readers.push(reader);
            }
        }
        Ok(IndexMerger {
            schema: schema,
            readers: readers,
            segment_info: SegmentInfo { max_doc: max_doc },
        })
    }

    fn write_fieldnorms(&self,
        fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        let fieldnorm_fastfields: Vec<Field> = self.schema
                         .fields()
                         .iter()
                         .enumerate()
                         .filter(|&(_, field_entry)| field_entry.is_indexed())
                         .map(|(field_id, _)| Field(field_id as u8))
                         .collect();
        self.generic_write_fast_field(fieldnorm_fastfields, &extract_fieldnorm_reader, fast_field_serializer)
    }

    fn write_fast_fields(&self, fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        let fast_fields: Vec<Field> = self.schema
                         .fields()
                         .iter()
                         .enumerate()
                         .filter(|&(_, field_entry)| field_entry.is_u32_fast())
                         .map(|(field_id, _)| Field(field_id as u8))
                         .collect();
        self.generic_write_fast_field(fast_fields, &extract_fast_field_reader, fast_field_serializer)
    }


    // used both to merge field norms and regular u32 fast fields.
    fn generic_write_fast_field(&self,
        fields: Vec<Field>,
        field_reader_extractor: &Fn(&SegmentReader, Field) -> Result<U32FastFieldReader>,
        fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        
        for field in fields {
            
            let mut u32_readers = vec!();
            let mut min_val = u32::max_value();
            let mut max_val = u32::min_value();
            
            for reader in &self.readers {
                let u32_reader = field_reader_extractor(reader, field)?;
                if let Some((seg_min_val, seg_max_val)) = compute_min_max_val(&u32_reader, reader.max_doc(), reader.delete_bitset()) {
                    // the segment has some non-deleted documents
                    min_val = min(min_val, seg_min_val);
                    max_val = max(max_val, seg_max_val);
                    u32_readers.push((reader.max_doc(), u32_reader, reader.delete_bitset()));
                }
            }

            if u32_readers.is_empty() {
                // we have actually zero documents.
                min_val = 0;
                max_val = 0;
            }

            assert!(min_val <= max_val);

            // TODO test deleting all documents off the index.

            try!(fast_field_serializer.new_u32_fast_field(field, min_val, max_val));
            for (max_doc, u32_reader, delete_bitset) in u32_readers {
                for doc_id in 0..max_doc {
                    if !delete_bitset.is_deleted(doc_id) {
                        let val = u32_reader.get(doc_id);
                        try!(fast_field_serializer.add_val(val));
                    }
                }
            }

            try!(fast_field_serializer.close_field());
        }
        Ok(())
    }

    fn write_postings(&self,

        postings_serializer: &mut PostingsSerializer) -> Result<()> {
        
        let mut merged_terms = TermIterator::from(&self.readers[..]);
        let mut delta_position_computer = DeltaPositionComputer::new();
        
        let mut max_doc = 0;

        // map from segment doc ids to the resulting merged segment doc id.
        let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> = Vec::with_capacity(self.readers.len());
        
        for reader in &self.readers {
            let mut segment_local_map = Vec::with_capacity(reader.max_doc() as usize);
            for doc_id in 0..reader.max_doc() {
                if reader.is_deleted(doc_id) {
                    segment_local_map.push(None);
                }
                else {
                    segment_local_map.push(Some(max_doc));
                    max_doc += 1u32;
                }
            }
            merged_doc_id_map.push(segment_local_map);
        }
        
        while merged_terms.advance() {
            // Create the total list of doc ids
            // by stacking the doc ids from the different segment.
            //
            // In the new segments, the doc id from the different
            // segment are stacked so that :
            // - Segment 0's doc ids become doc id [0, seg.max_doc]
            // - Segment 1's doc ids become  [seg0.max_doc, seg0.max_doc + seg.max_doc]
            // - Segment 2's doc ids become  [seg0.max_doc + seg1.max_doc, seg0.max_doc + seg1.max_doc + seg2.max_doc]
            // ...
            let term = merged_terms.term();
            let mut term_written = false;
            let segment_postings = merged_terms
                    .segment_ords()
                    .iter()
                    .cloned()
                    .flat_map(|segment_ord| {
                        self.readers[segment_ord]
                            .read_postings_all_info(&term)
                            .map(|segment_postings| (segment_ord, segment_postings))
                    })
                    .collect::<Vec<_>>();

            // We can remove the term if all documents which
            // contained it have been deleted.
            if segment_postings.len() > 0 {

                // We can now serialize this postings, by pushing each document to the
                // postings serializer.                
                
                for (segment_ord, mut segment_postings) in segment_postings {
                    let old_to_new_doc_id = &merged_doc_id_map[segment_ord];
                    while segment_postings.advance() {
                        if let Some(remapped_doc_id) = old_to_new_doc_id[segment_postings.doc() as usize] {
                            if !term_written {
                                // we make sure to only write the term iff
                                // there is at least one document.
                                postings_serializer.new_term(&term)?;
                                term_written = true;
                            }
                            let delta_positions: &[u32] =
                                delta_position_computer.compute_delta_positions(segment_postings.positions());
                            try!(postings_serializer.write_doc(
                                    remapped_doc_id,
                                    segment_postings.term_freq(),
                                    delta_positions));
                        }
                    }
                }

                if term_written {
                    try!(postings_serializer.close_term());
                }
            }
            
        }
        Ok(())
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> Result<()> {
        for reader in &self.readers {
            let store_reader = reader.get_store_reader();
            for doc_id in 0..reader.max_doc() {
                if !reader.is_deleted(doc_id) {
                    let doc = try!(store_reader.get(doc_id));
                    let field_values: Vec<&FieldValue> = doc.field_values()
                        .iter()
                        .collect();
                    try!(store_writer.store(&field_values));
                }
            }   
        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> Result<u32> {
        try!(self.write_postings(serializer.get_postings_serializer()));
        try!(self.write_fieldnorms(serializer.get_fieldnorms_serializer()));
        try!(self.write_fast_fields(serializer.get_fast_field_serializer()));
        try!(self.write_storable_fields(serializer.get_store_writer()));
        try!(serializer.write_segment_info(&self.segment_info));
        try!(serializer.close());
        Ok(self.segment_info.max_doc)
    }
}

#[cfg(test)]
mod tests {
    use schema;
    use schema::Document;
    use schema::Term;
    use query::TermQuery;
    use schema::{Field, FieldValue};
    use core::Index;
    use Searcher;
    use DocAddress;
    use collector::tests::FastFieldTestCollector;
    use collector::tests::TestCollector;
    use query::BooleanQuery;
    use postings::SegmentPostingsOption;
    use schema::TextIndexingOptions;
    use futures::Future;

    #[test]
    fn test_index_merger() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let text_fieldtype = schema::TextOptions::default()
                                 .set_indexing_options(TextIndexingOptions::TokenizedWithFreq)
                                 .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::U32Options::default().set_fast();
        let score_field = schema_builder.add_u32_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                // writing the segment
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "af b");
                    doc.add_u32(score_field, 3);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c");
                    doc.add_u32(score_field, 5);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c d");
                    doc.add_u32(score_field, 7);
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.commit().expect("committed");
            }

            {
                // writing the segment
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "af b");
                    doc.add_u32(score_field, 11);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c g");
                    doc.add_u32(score_field, 13);
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.commit().expect("Commit failed");
            }
        }
        {
            let segment_ids = index.searchable_segment_ids().expect("Searchable segments failed.");
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer.merge(&segment_ids)
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
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "a")]),
                           vec!(1, 2, 4,));
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "af")]),
                           vec!(0, 3,));
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "g")]),
                           vec!(4,));
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "b")]),
                           vec!(0, 1, 2, 3, 4,));
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
                assert_eq!(get_fast_vals(vec![Term::from_field_text(text_field, "a")]),
                           vec!(5, 7, 13,));
            }
        }
    }

    fn search_term(searcher: &Searcher, term: Term) ->  Vec<u32> {
        let mut collector = FastFieldTestCollector::for_field(Field(1));
        let term_query = TermQuery::new(term, SegmentPostingsOption::NoFreq);
        searcher.search(&term_query, &mut collector).unwrap();
        collector.vals()
    }

    #[test]
    fn test_index_merger_with_deletes() {
        let mut schema_builder = schema::SchemaBuilder::default();
        let text_fieldtype = schema::TextOptions::default()
                                 .set_indexing_options(TextIndexingOptions::TokenizedWithFreq)
                                 .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::U32Options::default().set_fast();
        let score_field = schema_builder.add_u32_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            
        {   // a first commit
            index_writer.add_document(
                doc!(
                    text_field => "a b d",
                    score_field => 1
                )).unwrap();
            index_writer.add_document(
                doc!(
                    text_field => "b c",
                    score_field => 2
                )).unwrap();
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.add_document(
                doc!(
                    text_field => "c d",
                    score_field => 3
                )).unwrap();
            index_writer.commit().expect("committed");
            index.load_searchers().unwrap();
            let ref searcher = *index.searcher();
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "a")), vec!(1));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "b")), vec!(1));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "c")), vec!(3));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "d")), vec!(1, 3));
        }
        {   // a second commit
            index_writer.add_document(
                doc!(
                    text_field => "a d e",
                    score_field => 4_000
                )).unwrap();
            index_writer.add_document(
                doc!(
                    text_field => "e f",
                    score_field => 5_000
                )).unwrap();
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.delete_term(Term::from_field_text(text_field, "f"));
            index_writer.add_document(
                doc!(
                    text_field => "f g",
                    score_field => 6_000
                )).unwrap();
            index_writer.add_document(
                doc!(
                    text_field => "g h",
                    score_field => 7_000
                )).unwrap();
            index_writer.commit().expect("committed");
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            assert_eq!(searcher.segment_readers().len(), 2);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 1);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(searcher.segment_readers()[1].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[1].max_doc(), 4);
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "a")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "b")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "c")), vec!(3));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "d")), vec!(3));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "e")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "f")), vec!(6_000));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "g")), vec!(6_000, 7_000));
            
            let score_field_reader = searcher.segment_reader(0).get_fast_field_reader(score_field).unwrap();
            assert_eq!(score_field_reader.min_val(), 1);
            assert_eq!(score_field_reader.max_val(), 3);

            let score_field_reader = searcher.segment_reader(1).get_fast_field_reader(score_field).unwrap();
            assert_eq!(score_field_reader.min_val(), 4000);
            assert_eq!(score_field_reader.max_val(), 7000);
        }
        {   // merging the segments
            let segment_ids = index.searchable_segment_ids().expect("Searchable segments failed.");
            index_writer.merge(&segment_ids)
                        .wait()
                        .expect("Merging failed");
            index.load_searchers().unwrap();
            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "a")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "b")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "c")), vec!(3));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "d")), vec!(3));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "e")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "f")), vec!(6_000));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "g")), vec!(6_000, 7_000));
            let score_field_reader = searcher.segment_reader(0).get_fast_field_reader(score_field).unwrap();
            assert_eq!(score_field_reader.min_val(), 3);
            assert_eq!(score_field_reader.max_val(), 7000);
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
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "a")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "b")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "c")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "d")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "e")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "f")), vec!(6_000));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "g")), vec!(6_000, 7_000));
            let score_field_reader = searcher.segment_reader(0).get_fast_field_reader(score_field).unwrap();
            assert_eq!(score_field_reader.min_val(), 3);
            assert_eq!(score_field_reader.max_val(), 7000);
        }
        {   // Test merging a single segment in order to remove deletes.
            let segment_ids = index.searchable_segment_ids().expect("Searchable segments failed.");
            index_writer.merge(&segment_ids)
                        .wait()
                        .expect("Merging failed");
            index.load_searchers().unwrap();

            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 2);
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "a")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "b")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "c")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "d")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "e")), vec!());
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "f")), vec!(6_000));
            assert_eq!(search_term(&searcher, Term::from_field_text(text_field, "g")), vec!(6_000, 7_000));
            let score_field_reader = searcher.segment_reader(0).get_fast_field_reader(score_field).unwrap();
            assert_eq!(score_field_reader.min_val(), 6000);
            assert_eq!(score_field_reader.max_val(), 7000);
        }

        {   // Test removing all docs
            index_writer.delete_term(Term::from_field_text(text_field, "g"));
            let segment_ids = index.searchable_segment_ids().expect("Searchable segments failed.");
            index_writer.merge(&segment_ids)
                        .wait()
                        .expect("Merging failed");
            index.load_searchers().unwrap();

            let ref searcher = *index.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 0);
        }
        
        
    }
}
