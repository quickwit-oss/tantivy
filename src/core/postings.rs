use core::schema::DocId;
use std::ptr;
use std::collections::BTreeMap;
use core::schema::Term;
use core::fstmap::FstMapBuilder;
use core::index::Segment;
use core::directory::WritePtr;
use core::index::SegmentComponent;
use core::simdcompression;
use core::serialize::BinarySerializable;
use std::io::{Read, Write};
use std::io;

#[derive(Debug,Ord,PartialOrd,Eq,PartialEq,Clone)]
pub struct TermInfo {
    pub doc_freq: u32,
    pub postings_offset: u32,
}

impl BinarySerializable for TermInfo {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        Ok(
            try!(self.doc_freq.serialize(writer)) +
            try!(self.postings_offset.serialize(writer))
        )
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let doc_freq = try!(u32::deserialize(reader));
        let offset = try!(u32::deserialize(reader));
        Ok(TermInfo {
            doc_freq: doc_freq,
            postings_offset: offset,
        })
    }
}


pub trait U32sRecorder {
    fn new() -> Self;
    fn record(&mut self, val: u32);
}

pub struct VecRecorder(Vec<u32>);

impl U32sRecorder for VecRecorder {
    fn new() -> VecRecorder {
        VecRecorder(Vec::new())
    }
    fn record(&mut self, val: u32) {
        self.0.push(val);
    }
}

pub struct ObliviousRecorder;

impl U32sRecorder for ObliviousRecorder {
    fn new() -> ObliviousRecorder {
        ObliviousRecorder
    }
    fn record(&mut self, val: u32) {
    }
}

struct TermPostingsWriter<TermFreqsRec: U32sRecorder, PositionsRec: U32sRecorder> {
    doc_ids: Vec<DocId>,
    term_freqs: TermFreqsRec,
    positions: PositionsRec,
    current_position: u32,
    current_freq: u32,
}

impl<TermFreqsRec: U32sRecorder, PositionsRec: U32sRecorder> TermPostingsWriter<TermFreqsRec, PositionsRec> {
    pub fn new() -> TermPostingsWriter<TermFreqsRec, PositionsRec> {
        TermPostingsWriter {
            doc_ids: Vec::new(),
            term_freqs: TermFreqsRec::new(),
            positions: PositionsRec::new(),
            current_position: 0u32,
            current_freq: 0u32,
        }
    }

    fn close_doc(&mut self,) {
        self.term_freqs.record(self.current_freq);
        self.current_freq = 0;
        self.current_position = 0;
    }

    fn close(&mut self,) {
        if self.current_freq > 0 {
            self.close_doc();
        }
    }

    fn is_new_doc(&self, doc: &DocId) -> bool {
        match self.doc_ids.last() {
            Some(&last_doc) => last_doc != *doc,
            None => true,
        }
    }

    pub fn doc_freq(&self) -> u32 {
        self.doc_ids.len() as u32
    }

    pub fn suscribe(&mut self, doc: DocId, pos: u32) {
        if self.is_new_doc(&doc) {
            // this is the first time we meet this term for this document
            // first close the previous document, and write its doc_freq.
            self.close_doc();
            self.doc_ids.push(doc);
		}
        self.current_freq += 1;
        self.positions.record(pos - self.current_position);
        self.current_position = pos;
    }
}

pub struct PostingsWriter {
    postings: Vec<TermPostingsWriter<ObliviousRecorder, ObliviousRecorder>>,
    term_index: BTreeMap<Term, usize>,
}

impl PostingsWriter {

    pub fn new() -> PostingsWriter {
        PostingsWriter {
            postings: Vec::new(),
            term_index: BTreeMap::new(),
        }
    }

    pub fn suscribe(&mut self, doc: DocId, pos: u32, term: Term) {
        let doc_ids: &mut TermPostingsWriter<ObliviousRecorder, ObliviousRecorder> = self.get_term_postings(term);
        doc_ids.suscribe(doc, pos);
    }

    fn get_term_postings(&mut self, term: Term) -> &mut TermPostingsWriter<ObliviousRecorder, ObliviousRecorder> {
        match self.term_index.get(&term) {
            Some(unord_id) => {
                return &mut self.postings[*unord_id];
            },
            None => {}
        }
        let unord_id = self.term_index.len();
        self.postings.push(TermPostingsWriter::new());
        self.term_index.insert(term, unord_id.clone());
        &mut self.postings[unord_id]
    }

    pub fn serialize(&self, serializer: &mut PostingsSerializer) -> io::Result<()> {
        for (term, postings_id) in self.term_index.iter() {
            let term_postings_writer = &self.postings[postings_id.clone()];
            let term_docfreq = term_postings_writer.doc_freq();
            try!(serializer.new_term(&term, term_docfreq));
            for doc in term_postings_writer.doc_ids.iter() {
                try!(serializer.write_doc(doc.clone(), None));
            }
        }
        Ok(())
    }


}


//////////////////////////////////

pub trait Postings: Iterator<Item=DocId> {
    // after skipping position
    // the iterator in such a way that the
    // next call to next() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> Option<DocId>;
}


pub struct PostingsSerializer {
    terms_fst_builder: FstMapBuilder<WritePtr, TermInfo>, // TODO find an alternative to work around the "move"
    postings_write: WritePtr,
    positions_write: WritePtr,
    written_bytes_postings: usize,
    written_bytes_positions: usize,
    encoder: simdcompression::Encoder,
    doc_ids: Vec<DocId>,
}

impl PostingsSerializer {

    pub fn open(segment: &Segment) -> io::Result<PostingsSerializer> {
        let terms_write = try!(segment.open_write(SegmentComponent::TERMS));
        let terms_fst_builder = try!(FstMapBuilder::new(terms_write));
        let postings_write = try!(segment.open_write(SegmentComponent::POSTINGS));
        let positions_write = try!(segment.open_write(SegmentComponent::POSITIONS));
        Ok(PostingsSerializer {
            terms_fst_builder: terms_fst_builder,
            postings_write: postings_write,
            positions_write: positions_write,
            written_bytes_postings: 0,
            written_bytes_positions: 0,
            encoder: simdcompression::Encoder::new(),
            doc_ids: Vec::new(),
        })
    }

    pub fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()> {
        try!(self.close_term());
        self.doc_ids.clear();
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.terms_fst_builder
            .insert(term.as_slice(), &term_info)
    }

    pub fn close_term(&mut self,) -> io::Result<()> {
        if !self.doc_ids.is_empty() {
            let docs_data = self.encoder.encode_sorted(&self.doc_ids);
            self.written_bytes_postings += try!((docs_data.len() as u32).serialize(&mut self.postings_write));
            for num in docs_data {
                self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
            }
        }
        Ok(())
    }

    pub fn write_doc(&mut self, doc_id: DocId, positions: Option<&[u32]>) -> io::Result<()> {
        self.doc_ids.push(doc_id);
        Ok(())
    }


    pub fn close(mut self,) -> io::Result<()> {
        try!(self.close_term());
        try!(self.terms_fst_builder.finish());
        try!(self.postings_write.flush());
        Ok(())
    }
}



#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use core::schema::DocId;


    #[derive(Debug)]
    pub struct VecPostings {
        doc_ids: Vec<DocId>,
    	cursor: usize,
    }

    impl VecPostings {
        pub fn new(vals: Vec<DocId>) -> VecPostings {
            VecPostings {
                doc_ids: vals,
    			cursor: 0,
            }
        }
    }

    impl Postings for VecPostings {
        // after skipping position
        // the iterator in such a way that the
        // next call to next() will return a
        // value greater or equal to target.
        fn skip_next(&mut self, target: DocId) -> Option<DocId> {
            loop {
                match Iterator::next(self) {
                    Some(val) if val >= target => {
                        return Some(val);
                    },
                    None => {
                        return None;
                    },
                    _ => {}
                }
            }
        }
    }

    impl Iterator for VecPostings {
    	type Item = DocId;
    	fn next(&mut self,) -> Option<DocId> {
    		if self.cursor >= self.doc_ids.len() {
    			None
    		}
    		else {
                self.cursor += 1;
    			Some(self.doc_ids[self.cursor - 1])
    		}
    	}
    }
    //
    // #[test]
    // fn test_intersection() {
    //     {
    //         let left = VecPostings::new(vec!(1, 3, 9));
    //         let right = VecPostings::new(vec!(3, 4, 9, 18));
    //         let inter = IntersectionPostings::from_postings(vec!(left, right));
    //         let vals: Vec<DocId> = inter.collect();
    //         assert_eq!(vals, vec!(3, 9));
    //     }
    //     {
    //         let a = VecPostings::new(vec!(1, 3, 9));
    //         let b = VecPostings::new(vec!(3, 4, 9, 18));
    //         let c = VecPostings::new(vec!(1, 5, 9, 111));
    //         let inter = IntersectionPostings::from_postings(vec!(a, b, c));
    //         let vals: Vec<DocId> = inter.collect();
    //         assert_eq!(vals, vec!(9));
    //     }
    // }
    //
    // #[bench]
    // fn bench_single_intersection(b: &mut Bencher) {
    //     b.iter(|| {
    //         let docs = VecPostings::new((0..1_000_000).collect());
    //         let intersection = IntersectionPostings::from_postings(vec!(docs));
    //         intersection.count()
    //     });
    // }
}
