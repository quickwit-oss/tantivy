use std::io;
use core::reader::SegmentReader;
use core::index::Segment;
use DocId;
use core::index::SerializableSegment;
use core::codec::SegmentSerializer;

use postings::PostingsSerializer;
use postings::TermInfo;
use postings::Postings;
use std::collections::BinaryHeap;
use datastruct::FstMapIter;
use schema::{Term, Schema, U32Field};
use fastfield::FastFieldSerializer;
use store::StoreWriter;
use postings::ChainedPostings;
use postings::OffsetPostings;
use core::index::SegmentInfo;
use std::cmp::{min, max, Ordering};

struct PostingsMerger<'a> {
    doc_offsets: Vec<DocId>,
    heap: BinaryHeap<HeapItem>,
    term_streams: Vec<FstMapIter<'a, TermInfo>>,
    readers: &'a Vec<SegmentReader>,
}

#[derive(PartialEq, Eq, Debug)]
struct HeapItem {
    term: Vec<u8>,
    segment_ord: usize,
    term_info: TermInfo,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &HeapItem) -> Ordering {
        match other.term.cmp(&self.term) {
            Ordering::Equal => {
                other.segment_ord.cmp(&self.segment_ord)
            }
            e @ _ => {
                e
            }
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &HeapItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PostingsMerger<'a> {
    fn new(readers: &'a Vec<SegmentReader>) -> PostingsMerger<'a> {
        let mut doc_offsets: Vec<DocId> = Vec::new();
        let mut max_doc = 0;
        for reader in readers.iter() {
            doc_offsets.push(max_doc);
            max_doc += reader.max_doc();
        };
        let term_streams = readers
            .iter()
            .map(|reader| reader.term_infos().stream())
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
            Some((term, term_info)) => {
                let it = HeapItem {
                    term: Vec::from(term),
                    segment_ord: segment_ord,
                    term_info: term_info.clone(),
                };
                self.heap.push(it);
            }
            None => {}
        }
    }

    fn append_segment(&mut self, heap_item: &HeapItem, segment_postings_list: &mut Vec<OffsetPostings<'a>>) {
        {
            let offset = self.doc_offsets[heap_item.segment_ord];
            let reader = &self.readers[heap_item.segment_ord];
            let segment_postings = reader.read_postings(&heap_item.term_info);
            let offset_postings = OffsetPostings::new(segment_postings, offset); 
            segment_postings_list.push(offset_postings);
        }
        self.push_next_segment_el(heap_item.segment_ord);
    }

    fn next(&mut self,) -> Option<(Vec<u8>, ChainedPostings<'a>)> {
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
                    let next_heap_it = self.heap.pop().unwrap();
                    self.append_segment(&next_heap_it, &mut segment_postings_list);
                }
                let chained_posting = ChainedPostings::new(segment_postings_list);
                Some((heap_it.term, chained_posting))
            },
            None => None
        }
    }
}

const EMPTY_ARRAY: [u32; 0] = [];

pub struct IndexMerger {
    schema: Schema,
    readers: Vec<SegmentReader>,
    segment_info: SegmentInfo,
}

impl IndexMerger {
    pub fn open(schema: Schema, segments: &Vec<Segment>) -> io::Result<IndexMerger> {
        let mut readers = Vec::new();
        let mut max_doc = 0;
        for segment in segments.iter() {
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

    fn write_fast_fields(&self, fast_field_serializer: &mut FastFieldSerializer) -> io::Result<()> {
        for field in self.schema
            .get_u32_fields()
            .iter()
            .enumerate()
            .filter(|&(_, field_entry)| field_entry.option.is_fast())
            .map(|(field_id, _)| U32Field(field_id as u8)) {
            let mut u32_readers = Vec::new();
            let mut min_val = u32::min_value();
            let mut max_val = 0;
            for reader in self.readers.iter() {
                let u32_reader = try!(reader.get_fast_field_reader(&field));
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

    fn write_postings(&self, postings_serializer: &mut PostingsSerializer) -> io::Result<()> {
        let mut postings_merger = PostingsMerger::new(&self.readers);
        loop {
            match postings_merger.next() {
                Some((term, mut merged_doc_ids)) => {
                    try!(postings_serializer.new_term(&Term::from(&term), merged_doc_ids.doc_freq() as DocId));
                    while merged_doc_ids.next() {
                        try!(postings_serializer.write_doc(merged_doc_ids.doc(), 0, &EMPTY_ARRAY));
                    }
                    try!(postings_serializer.close_term());
                }
                None => { break; }
            }
        }
        Ok(())
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> io::Result<()> {
        for reader in self.readers.iter() {
            let store_reader = reader.get_store_reader();
            try!(store_writer.stack_reader(store_reader));
        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> io::Result<()> {
        try!(self.write_postings(serializer.get_postings_serializer()));
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
    use core::index::Index;
    use core::searcher::DocAddress;
    use collector::FastFieldTestCollector;
    use collector::TestCollector;

    #[test]
    fn test_index_merger() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed().set_stored();
        let text_field = schema.add_text_field("text", &text_fieldtype);
        let score_fieldtype = schema::U32Options::new().set_fast();
        let score_field = schema.add_u32_field("score", &score_fieldtype);
        let index = Index::create_in_ram(schema);

        {
            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1).unwrap();
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "af b");
                    doc.set_u32(&score_field, 3);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c");
                    doc.set_u32(&score_field, 5);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c d");
                    doc.set_u32(&score_field, 7);
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.wait().unwrap();
            }

            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1).unwrap();
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "af b");
                    doc.set_u32(&score_field, 11);
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c g");
                    doc.set_u32(&score_field, 13);
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.wait().unwrap();
            }
        }
        {
            let segments = index.segments();
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            index_writer.merge(&segments).unwrap();
        }
        {
            let searcher = index.searcher().unwrap();
            let get_doc_ids = |terms: Vec<Term>| {
                let mut collector = TestCollector::new();
                assert!(searcher.search(&terms, &mut collector).is_ok());
                collector.docs()
            };
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "a"))),
                    vec!(1, 2, 4,)
                );
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "af"))),
                    vec!(0, 3,)
                );
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "g"))),
                    vec!(4,)
                );
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "b"))),
                    vec!(0, 1, 2, 3, 4,)
                );
            }
            {
                let doc = searcher.doc(&DocAddress(0, 0)).unwrap();
                assert_eq!(doc.get_first_text(&text_field).unwrap(), "af b");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 1)).unwrap();
                assert_eq!(doc.get_first_text(&text_field).unwrap(), "a b c");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 2)).unwrap();
                assert_eq!(doc.get_first_text(&text_field).unwrap(), "a b c d");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 3)).unwrap();
                assert_eq!(doc.get_first_text(&text_field).unwrap(), "af b");
            }
            {
                let doc = searcher.doc(&DocAddress(0, 4)).unwrap();
                assert_eq!(doc.get_first_text(&text_field).unwrap(), "a b c g");
            }
            {
                let get_fast_vals = |terms: Vec<Term>| {
                    let mut collector = FastFieldTestCollector::for_field(score_field);
                    assert!(searcher.search(&terms, &mut collector).is_ok());
                    collector.vals().clone()
                };
                assert_eq!(
                    get_fast_vals(vec!(Term::from_field_text(&text_field, "a"))),
                    vec!(5, 7, 13,)
                );
            }
        }
    }
}
