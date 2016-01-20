use core::directory::Directory;
use core::directory::Segment;
use core::schema::Term;
use fst::Streamer;
use fst;
// use fst::raw::{Fst, FstData};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::Cursor;
use core::global::DocId;
use core::serial::{DocCursor, TermCursor};
use core::serial::SerializableSegment;
use core::directory::SegmentComponent;
use fst::raw::MmapReadOnly;
use core::error::{Result, Error};




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

// ------------------------

pub struct SegmentTermCur<'a> {
    segment: &'a Segment,
    fst_streamer: fst::map::Stream<'a>,
    postings_data: &'a [u8],
}

impl<'a> TermCursor for SegmentTermCur<'a> {

    type DocCur = SegmentDocCursor<'a>;

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


// ----------------------

// TODO file structure should be in codec

pub struct SegmentIndexReader {
    segment: Segment,
    term_offsets: fst::Map,
    postings_data: MmapReadOnly,
}

impl SegmentIndexReader {
    //
    // pub fn open(segment: Segment) -> Result<SegmentIndexReader> {
    //     let term_shared_mmap = try!(segment.mmap(SegmentComponent::TERMS));
    //     let term_offsets = match Fst::new(FstData::Mmap(term_shared_mmap)).map(fst::Map) {
    //         Ok(term_offsets) => term_offsets,
    //         Err(_) => {
    //             let filepath = segment.relative_path(SegmentComponent::TERMS);
    //             return Err(Error::FSTFormat(format!("The file {:?} does not seem to be a valid term to offset transducer.", filepath)));
    //         }
    //     };
    //     let postings_shared_mmap = try!(segment.mmap(SegmentComponent::POSTINGS));
    //     Ok(SegmentIndexReader {
    //         postings_data: postings_shared_mmap,
    //         term_offsets: term_offsets,
    //         segment: segment,
    //     })
    // }

}

impl<'a> SerializableSegment<'a> for SegmentIndexReader {

    type TermCur = SegmentTermCur<'a>;

    fn term_cursor(&'a self) -> SegmentTermCur<'a> {
        SegmentTermCur {
            segment: &self.segment,
            fst_streamer: self.term_offsets.stream(),
            postings_data: unsafe { self.postings_data.borrow().as_slice() },
        }
    }
}
