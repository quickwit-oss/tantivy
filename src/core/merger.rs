use std::io;
use core::reader::SegmentReader;
use core::index::Segment;
use core::schema::DocId;
use core::index::SerializableSegment;
use core::codec::SegmentSerializer;
use core::postings::PostingsSerializer;
use core::postings::TermInfo;
use std::collections::BinaryHeap;
use core::fstmap::FstMapIter;
use core::schema::Term;
use core::schema::Schema;
use core::fastfield::FastFieldSerializer;
use std::cmp::Ordering;
use core::schema::U32Field;
use std::cmp::min;
use std::cmp::max;

struct PostingsMerger<'a> {
    doc_ids: Vec<DocId>,
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
        let doc_offsets: Vec<DocId> = readers
            .iter()
            .map(|reader| reader.max_doc())
            .collect();
        let term_streams = readers
            .iter()
            .map(|reader| reader.term_infos().stream())
            .collect();
        let mut postings_merger = PostingsMerger {
            heap: BinaryHeap::new(),
            term_streams: term_streams,
            doc_ids: Vec::new(),
            doc_offsets: doc_offsets,
            readers: readers,
        };
        for segment_ord in 0..readers.len() {
            postings_merger.push_next_segment_el(segment_ord);
        }
        postings_merger
    }

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

    fn append_segment(&mut self, heap_item: &HeapItem) {
        {
            let offset = self.doc_offsets[heap_item.segment_ord];
            let reader = &self.readers[heap_item.segment_ord];
            for doc_id in reader.read_postings(heap_item.term_info.postings_offset) {
                self.doc_ids.push(offset + doc_id);
            }
        }
        self.push_next_segment_el(heap_item.segment_ord);
    }

    fn next(&mut self,) -> Option<(Vec<u8>, &Vec<DocId>)> {
        // TODO remove the Vec<u8> allocations
        match self.heap.pop() {
            Some(heap_it) => {
                self.doc_ids.clear();
                self.append_segment(&heap_it);
                loop {
                    match self.heap.peek() {
                        Some(&ref next_heap_it) if next_heap_it.term == heap_it.term => {},
                        _ => { break; }
                    }
                    let next_heap_it = self.heap.pop().unwrap();
                    self.append_segment(&next_heap_it);
                }
                Some((heap_it.term, &self.doc_ids))
            },
            None => None
        }
    }
}

pub struct IndexMerger {
    schema: Schema,
    readers: Vec<SegmentReader>,
}

impl IndexMerger {
    pub fn open(schema: Schema, segments: &Vec<Segment>) -> io::Result<IndexMerger> {
        let mut readers = Vec::new();
        for segment in segments.iter() {
            let reader = try!(SegmentReader::open(segment.clone()));
            readers.push(reader);
        }
        Ok(IndexMerger {
            schema: schema,
            readers: readers,
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
            fast_field_serializer.new_u32_fast_field(field, min_val, max_val);
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
                Some((term, doc_ids)) => {
                    try!(postings_serializer.new_term(&Term::from(&term), doc_ids.len() as DocId));
                    try!(postings_serializer.write_docs(doc_ids));
                }
                None => { break; }
            }
        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> io::Result<()> {
        try!(self.write_postings(serializer.get_postings_serializer()));
        try!(self.write_fast_fields(serializer.get_fast_field_serializer()));
        serializer.close()
    }
}

#[cfg(test)]
mod tests {
    use core::schema;
    use core::schema::Document;
    use core::index::Index;
    use super::IndexMerger;

    #[test]
    fn test_index_merger() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed();
        let text_field = schema.add_text_field("text", &text_fieldtype);
        let index = Index::create_in_ram(schema);

        {
            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1).unwrap();
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "af b");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c d");
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.wait();
            }

            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1).unwrap();
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "af b");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a d c");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c g");
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.wait();
            }
        }
        let segments = index.segments();
        println!("before {:?}", index.segments());
        let mut index_writer = index.writer_with_num_threads(1).unwrap();
        index_writer.merge(&segments).unwrap();
        println!("after {:?}", index.segments());
        assert_eq!(2, 1);
    }
}
