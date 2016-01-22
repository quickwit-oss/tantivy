use core::directory::Directory;
use core::directory::Segment;
use core::schema::Term;
use fst::Streamer;
use fst;
use std::io;
use fst::raw::Fst;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::Cursor;
use core::global::DocId;
use core::serial::*;
use core::directory::SegmentComponent;
use fst::raw::MmapReadOnly;
use core::error::{Result, Error};

// TODO file structure should be in codec

pub struct SegmentIndexReader {
    segment: Segment,
    term_offsets: fst::Map,
    postings_data: MmapReadOnly,
}

impl SegmentIndexReader {

    pub fn open(segment: Segment) -> Result<SegmentIndexReader> {
        let term_shared_mmap = try!(segment.mmap(SegmentComponent::TERMS));
        let term_offsets = match fst::Map::from_mmap(term_shared_mmap) {
            Ok(term_offsets) => term_offsets,
            Err(_) => {
                let filepath = segment.relative_path(&SegmentComponent::TERMS);
                return Err(Error::FSTFormat(format!("The file {:?} does not seem to be a valid term to offset transducer.", filepath)));
            }
        };
        let postings_shared_mmap = try!(segment.mmap(SegmentComponent::POSTINGS));
        Ok(SegmentIndexReader {
            postings_data: postings_shared_mmap,
            term_offsets: term_offsets,
            segment: segment,
        })
    }
}


fn write_postings<R: io::Read, Output, SegSer: SegmentSerializer<Output>>(mut cursor: R, num_docs: DocId, serializer: &mut SegSer) -> Result<()> {
    for i in 0..num_docs {
        let doc_id = cursor.read_u32::<LittleEndian>().unwrap();
        try!(serializer.add_doc(doc_id));
    }
    Ok(())
}

impl SerializableSegment for SegmentIndexReader {

    fn write<Output, SegSer: SegmentSerializer<Output>>(&self, mut serializer: SegSer) -> Result<Output> {
        let mut term_offsets_it = self.term_offsets.stream();
        loop {
            match term_offsets_it.next() {
                Some((term_data, offset_u64)) => {
                    let term = Term::from(term_data);
                    let offset = offset_u64 as usize;
                    let data = unsafe { &self.postings_data.as_slice()[offset..] };
                    let mut cursor = Cursor::new(data);
                    let num_docs = cursor.read_u32::<LittleEndian>().unwrap() as DocId;
                    try!(serializer.new_term(&term, num_docs));
                    try!(write_postings(cursor, num_docs, &mut serializer));
                },
                None => { break; }
            }
        }
        serializer.close()
    }

}
