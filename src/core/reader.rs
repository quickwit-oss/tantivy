use core::directory::Directory;
use core::directory::Segment;
use core::schema::Term;
use core::directory::SharedMmapMemory;
use fst::Streamer;
use fst;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::Cursor;
use core::global::DocId;
use core::serial::DocCursor;

pub struct SegmentIndexReader {
    segment: Segment,
    term_offsets: fst::Map,
    postings_data: SharedMmapMemory,
}

impl SegmentIndexReader {
    fn term_cursor<'a>(&'a self) -> SegmentTermCur<'a> {
        SegmentTermCur {
            segment: &self.segment,
            fst_streamer: self.term_offsets.stream(),
            postings_data: self.postings_data.borrow(),
        }
    }
}

pub struct SegmentDocCursor<'a> {
    postings_data: Cursor<&'a [u8]>,
    num_docs: DocId,
    current_doc: DocId,
}

impl<'a> Iterator for SegmentDocCursor<'a> {
    type Item = DocId;

    fn next(&mut self) -> Option<DocId> {
        if self.num_docs == 0 {
            None
        }
        else {
            self.num_docs -= 1;
            self.current_doc = self.postings_data.read_u32::<LittleEndian>().unwrap();
            Some(self.current_doc)
        }
    }
}

impl<'a> DocCursor for SegmentDocCursor<'a> {
    fn doc(&self) -> DocId{
        self.current_doc
    }

    fn len(&self) -> DocId {
        self.num_docs
    }
}

struct SegmentTermCur<'a> {
    segment: &'a Segment,
    fst_streamer: fst::map::Stream<'a>,
    postings_data: &'a [u8],
}

impl<'a> SegmentTermCur<'a> {

    fn next(&mut self,) -> Option<(Term, SegmentDocCursor<'a>)> {
        match self.fst_streamer.next() {
            Some((k, offset_u64)) => {
                let term = Term::from(k);
                let offset = offset_u64 as usize;
                let data = &self.postings_data[offset..];
                let mut cursor = Cursor::new(data);
                let num_docs = cursor.read_u32::<LittleEndian>().unwrap();
                let doc_cursor = SegmentDocCursor {
                    postings_data: cursor,
                    num_docs: num_docs,
                    current_doc: 0,
                };
                Some((term, doc_cursor))
            },
            None => None
        }
    }
}
