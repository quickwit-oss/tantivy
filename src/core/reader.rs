use core::directory::Directory;
use core::directory::Segment;
use core::schema::Term;
use core::directory::SharedMmapMemory;
use fst::Streamer;
use fst;
use std::borrow::Borrow;

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
    postings_data: &'a [u8],
    offset: usize,
}

struct SegmentTermCur<'a> {
    segment: &'a Segment,
    fst_streamer: fst::map::Stream<'a>,
    postings_data: &'a [u8],
}

impl<'a> SegmentTermCur<'a> {

    fn next(&mut self,) -> Option<(Term, SegmentDocCursor<'a>)> {
        match self.fst_streamer.next() {
            Some((k, offset)) => {
                let term = Term::from(k);
                let doc_cursor = SegmentDocCursor {
                    postings_data: self.postings_data,
                    offset: offset as usize,
                };
                Some((term, doc_cursor))
            },
            None => None
        }
    }
}
