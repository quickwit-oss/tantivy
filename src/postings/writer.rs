use DocId;
use std::collections::BTreeMap;
use schema::Term;
use postings::PostingsSerializer;
use std::io;

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
    fn record(&mut self, _: u32) {
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
