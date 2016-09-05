use Result;
use core::SegmentReader;
use core::Segment;
use DocId;
use core::SerializableSegment;

use indexer::SegmentSerializer;
use postings::PostingsSerializer;
use postings::TermInfo;
use postings::Postings;
use postings::DocSet;
use std::collections::BinaryHeap;
use datastruct::FstKeyIter;
use schema::{Term, Schema, Field};
use fastfield::FastFieldSerializer;
use store::StoreWriter;
use postings::ChainedPostings;
use postings::HasLen;
use postings::OffsetPostings;
use core::SegmentInfo;
use std::cmp::{min, max, Ordering};
use std::iter;


struct PostingsMerger<'a> {
    doc_offsets: Vec<DocId>,
    heap: BinaryHeap<HeapItem>,
    term_streams: Vec<FstKeyIter<'a, TermInfo>>,
    readers: &'a Vec<SegmentReader>,
}

#[derive(PartialEq, Eq, Debug)]
struct HeapItem {
    term: Term,
    segment_ord: usize,
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &HeapItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &HeapItem) -> Ordering {
        return (&other.term, &other.segment_ord).cmp(&(&self.term, &self.segment_ord))
    }
}

impl<'a> PostingsMerger<'a> {
    fn new(readers: &'a Vec<SegmentReader>) -> PostingsMerger<'a> {
        let mut doc_offsets: Vec<DocId> = Vec::new();
        let mut max_doc = 0;
        for reader in readers {
            doc_offsets.push(max_doc);
            max_doc += reader.max_doc();
        };
        let term_streams = readers
            .iter()
            .map(|reader| reader.term_infos().keys())
            .collect();
        let mut postings_merger = PostingsMerger {
            heap: BinaryHeap::new(),
            term_streams: term_streams,
            doc_offsets: doc_offsets,
            readers: readers,
        };
        for segment_ord in 0..readers.len() {
            postings_merger.push_next_segment_el(segment_ord);
        }
        postings_merger
    }

    // pushes the term_reader associated with the given segment ordinal
    // into the heap.
    fn push_next_segment_el(&mut self, segment_ord: usize) {
        match self.term_streams[segment_ord].next() {
            Some(term) => {
                let it = HeapItem {
                    term: Term::from(term),
                    segment_ord: segment_ord,
                };
                self.heap.push(it);
            }
            None => {}
        }
    }

    fn append_segment(&mut self,
                      heap_item: &HeapItem,
                      segment_postings_list: &mut Vec<OffsetPostings<'a>>) {
        {
            
            let offset = self.doc_offsets[heap_item.segment_ord];
            let reader = &self.readers[heap_item.segment_ord];
            let segment_postings = reader.read_postings_all_info(&heap_item.term);
            let offset_postings = OffsetPostings::new(segment_postings, offset);
            segment_postings_list.push(offset_postings);
        }
        self.push_next_segment_el(heap_item.segment_ord);
    }

    fn next(&mut self,) -> Option<(Term, ChainedPostings<'a>)> {
        // TODO remove the Vec<u8> allocations
        match self.heap.pop() {
            Some(heap_it) => {
                let mut segment_postings_list = Vec::new();
                self.append_segment(&heap_it, &mut segment_postings_list);
                loop {
                    match self.heap.peek() {
                        Some(&ref next_heap_it) if next_heap_it.term == heap_it.term => {},
                        _ => { break; }
                    }
                    let next_heap_it = self.heap.pop().expect("This is only reached if an element was peeked beforehand.");
                    self.append_segment(&next_heap_it, &mut segment_postings_list);
                }
                let chained_posting = ChainedPostings::new(segment_postings_list);
                Some((heap_it.term, chained_posting))
            },
            None => None
        }
    }
}

pub struct IndexMerger {
    schema: Schema,
    readers: Vec<SegmentReader>,
    segment_info: SegmentInfo,
}


struct DeltaPositionComputer {
    buffer: Vec<u32>
}

impl DeltaPositionComputer {
    fn new() -> DeltaPositionComputer {
        DeltaPositionComputer {
            buffer: iter::repeat(0u32).take(512).collect::<Vec<u32>>(),
        }
    }
    
    fn compute_delta_positions(&mut self, positions: &[u32],) -> &[u32] {
        if positions.len() > self.buffer.len() {
            self.buffer.resize(positions.len(), 0u32);
        }
        let mut last_pos = 0u32;
        let num_positions = positions.len();
        for i in 0..num_positions {
            let position = positions[i];
            self.buffer[i] = position - last_pos;
            last_pos = position;
        }
        &self.buffer[..num_positions]
    }
}



impl IndexMerger {
    pub fn open(schema: Schema, segments: &Vec<Segment>) -> Result<IndexMerger> {
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
            segment_info: SegmentInfo {
                max_doc: max_doc
            },
        })
    }

    
    fn write_fieldnorms(&self, fast_field_serializer: &mut FastFieldSerializer) -> Result<()> {
        // TODO make sure that works even if the field is never here.
        for field in self.schema.fields()
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
        for field in self.schema.fields()
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
        let mut postings_merger = PostingsMerger::new(&self.readers);
        let mut delta_position_computer = DeltaPositionComputer::new();
        loop {
            match postings_merger.next() {
                Some((term, mut merged_doc_ids)) => {
                    try!(postings_serializer.new_term(&term, merged_doc_ids.len() as DocId));
                    while merged_doc_ids.advance() {
                        let delta_positions: &[u32] = delta_position_computer.compute_delta_positions(merged_doc_ids.positions());
                        try!(postings_serializer.write_doc(merged_doc_ids.doc(), merged_doc_ids.term_freq(), delta_positions));
                    }
                    try!(postings_serializer.close_term());
                }
                None => { break; }
            }
        }
        Ok(())
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> Result<()> {
        for reader in self.readers.iter() {
            let store_reader = reader.get_store_reader();
            try!(store_writer.stack_reader(store_reader));
        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> Result<()> {
        try!(self.write_postings(serializer.get_postings_serializer()));
        try!(self.write_fieldnorms(serializer.get_fieldnorms_serializer()));
        try!(self.write_fast_fields(serializer.get_fast_field_serializer()));
        try!(self.write_storable_fields(serializer.get_store_writer()));
        try!(serializer.write_segment_info(&self.segment_info));
        serializer.close()
    }
}

#[cfg(test)]
mod tests {
    use schema;
    use schema::Document;
    use schema::Term;
    use core::Index;
    use DocAddress;
    use collector::FastFieldTestCollector;
    use collector::TestCollector;
    use query::MultiTermQuery;
    use schema::TextIndexingOptions;

    #[test]
    fn test_index_merger() {
        let mut schema_builder = schema::SchemaBuilder::new();
        let text_fieldtype = schema::TextOptions::new().set_indexing_options(TextIndexingOptions::TokenizedWithFreq).set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::U32Options::new().set_fast();
        let score_field = schema_builder.add_u32_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());

        {
            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1, 30_000_000).unwrap();
                {
                    let mut doc = Document::new();
                    doc.add_text(text_field, "af b");
                    doc.add_u32(score_field, 3);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.add_text(text_field, "a b c");
                    doc.add_u32(score_field, 5);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.add_text(text_field, "a b c d");
                    doc.add_u32(score_field, 7);
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.commit().unwrap();
            }

            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1, 30_000_000).unwrap();
                {
                    let mut doc = Document::new();
                    doc.add_text(text_field, "af b");
                    doc.add_u32(score_field, 11);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.add_text(text_field, "a b c g");
                    doc.add_u32(score_field, 13);
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.commit().unwrap();
            }
        }
        {
            let segments = index.segments().unwrap();
            let mut index_writer = index.writer_with_num_threads(1, 30_000_000).unwrap();
            index_writer.merge(&segments).unwrap();
        }
        {
            let searcher = index.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let mut collector = TestCollector::new();
                let query = MultiTermQuery::from(terms);
                assert!(searcher.search(&query, &mut collector).is_ok());
                collector.docs()
            };
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "a"))),
                    vec!(1, 2, 4,)
                );
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "af"))),
                    vec!(0, 3,)
                );
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "g"))),
                    vec!(4,)
                );
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "b"))),
                    vec!(0, 1, 2, 3, 4,)
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
                    let query = MultiTermQuery::from(terms);
                    let mut collector = FastFieldTestCollector::for_field(score_field);
                    assert!(searcher.search(&query, &mut collector).is_ok());
                    collector.vals().clone()
                };
                assert_eq!(
                    get_fast_vals(vec!(Term::from_field_text(text_field, "a"))),
                    vec!(5, 7, 13,)
                );
            }
        }
    }
}
