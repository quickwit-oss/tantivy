use Result;
use core::SegmentReader;
use core::Segment;
use core::SegmentId;
use DocId;
use core::SerializableSegment;
use indexer::SegmentSerializer;
use postings::PostingsSerializer;
use postings::Postings;
use postings::DocSet;
use core::TermIterator;
use schema::{Schema, Field};
use fastfield::FastFieldSerializer;
use store::StoreWriter;
use postings::ChainedPostings;
use postings::HasLen;
use futures::Future;
use postings::OffsetPostings;
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

impl IndexMerger {
    pub fn open(schema: Schema, segments: &[Segment]) -> Result<IndexMerger> {
        let mut readers = Vec::new();
        let mut max_doc = 0;
        for segment in segments {
            let reader = try!(SegmentReader::open(segment.clone()));
            max_doc += reader.max_doc();
            readers.push(reader);
        }
        Ok(IndexMerger {
            schema: schema,
            readers: readers,
            segment_info: SegmentInfo { max_doc: max_doc },
        })
    }


    fn write_fieldnorms(&self, fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        // TODO make sure that works even if the field is never here.
        for field in self.schema
                         .fields()
                         .iter()
                         .enumerate()
                         .filter(|&(_, field_entry)| field_entry.is_indexed())
                         .map(|(field_id, _)| Field(field_id as u8)) {
            let mut u32_readers = Vec::new();
            let mut min_val = u32::min_value();
            let mut max_val = 0;
            for reader in &self.readers {
                let u32_reader = try!(reader.get_fieldnorms_reader(field));
                min_val = min(min_val, u32_reader.min_val());
                max_val = max(max_val, u32_reader.max_val());
                u32_readers.push((reader.max_doc(), u32_reader));
            }
            try!(fast_field_serializer.new_u32_fast_field(field, min_val, max_val));
            for (max_doc, u32_reader) in u32_readers {
                for doc_id in 0..max_doc {
                    let val = u32_reader.get(doc_id);
                    try!(fast_field_serializer.add_val(val));
                }
            }
            try!(fast_field_serializer.close_field());
        }
        Ok(())
    }

    fn write_fast_fields(&self, fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        for field in self.schema
                         .fields()
                         .iter()
                         .enumerate()
                         .filter(|&(_, field_entry)| field_entry.is_u32_fast())
                         .map(|(field_id, _)| Field(field_id as u8)) {
            let mut u32_readers = Vec::new();
            let mut min_val = u32::min_value();
            let mut max_val = 0;
            for reader in &self.readers {
                let u32_reader = try!(reader.get_fast_field_reader(field));
                min_val = min(min_val, u32_reader.min_val());
                max_val = max(max_val, u32_reader.max_val());
                u32_readers.push((reader.max_doc(), u32_reader));
            }
            try!(fast_field_serializer.new_u32_fast_field(field, min_val, max_val));
            for (max_doc, u32_reader) in u32_readers {
                for doc_id in 0..max_doc {
                    let val = u32_reader.get(doc_id);
                    try!(fast_field_serializer.add_val(val));
                }
            }
            try!(fast_field_serializer.close_field());
        }
        Ok(())
    }

    fn write_postings(&self, postings_serializer: &mut PostingsSerializer) -> Result<()> {
        let mut merged_terms = TermIterator::from(&self.readers[..]);
        let mut delta_position_computer = DeltaPositionComputer::new();
        let mut offsets: Vec<DocId> = Vec::new();
        let mut max_doc = 0;
        for reader in &self.readers {
            offsets.push(max_doc);
            max_doc += reader.max_doc();
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
            let mut merged_postings =
                ChainedPostings::from(
                    merged_terms
                        .segment_ords()
                        .iter()
                        .cloned()
                        .flat_map(|segment_ord| {
                            let offset = offsets[segment_ord];
                            self.readers[segment_ord]
                                .read_postings_all_info(&term)
                                .map(|segment_postings| OffsetPostings::new(segment_postings, offset))
                        })
                        .collect::<Vec<_>>()
            );

            // We can now serialize this postings, by pushing each document to the
            // postings serializer.
            try!(postings_serializer.new_term(&term, merged_postings.len() as DocId));
            while merged_postings.advance() {
                let delta_positions: &[u32] =
                    delta_position_computer.compute_delta_positions(merged_postings.positions());
                try!(postings_serializer.write_doc(merged_postings.doc(),
                                                   merged_postings.term_freq(),
                                                   delta_positions));
            }
            try!(postings_serializer.close_term());
        }

        Ok(())
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> Result<()> {
        for reader in &self.readers {
            let store_reader = reader.get_store_reader();
            try!(store_writer.stack_reader(store_reader));
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
    use core::Index;
    use DocAddress;
    use collector::tests::FastFieldTestCollector;
    use collector::tests::TestCollector;
    use query::BooleanQuery;
    use schema::TextIndexingOptions;
    use eventual::Async;
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
                index_writer.commit().unwrap();
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
                    collector.vals().clone()
                };
                assert_eq!(get_fast_vals(vec![Term::from_field_text(text_field, "a")]),
                           vec!(5, 7, 13,));
            }
        }
    }
}
