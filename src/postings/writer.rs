use DocId;
use std::collections::BTreeMap;
use schema::Term;
use postings::PostingsSerializer;
use std::io;
use postings::Recorder;
use postings::TermFrequencyRecorder;



struct TermPostingsWriter<Rec: Recorder> {
    doc_ids: Vec<DocId>,
    recorder: Rec,
}

impl<Rec: Recorder> TermPostingsWriter<Rec> {
    pub fn new() -> TermPostingsWriter<Rec> {
        TermPostingsWriter {
            doc_ids: Vec::new(),
            recorder: Recorder::new(),
        }
    }

    fn close_doc(&mut self,) {
        self.recorder.close_doc();
    }
    
    pub fn doc_freq(&self) -> u32 {
        self.doc_ids.len() as u32
    }

    pub fn suscribe(&mut self, doc: DocId, pos: u32) {
         match self.doc_ids.last() {
            Some(&last_doc) => {
                if last_doc != doc {
                    self.close_doc();
                    self.doc_ids.push(doc);
                }
            },
            None => {
                self.doc_ids.push(doc)
            },
        }
        self.recorder.record_position(pos);
    }
    
    pub fn serialize(&self, serializer: &mut PostingsSerializer) -> io::Result<()> {
        for (i, doc) in self.doc_ids.iter().enumerate() {
            let (term_freq, position_deltas) = self.recorder.get_tf_and_posdeltas(i);
            try!(serializer.write_doc(doc.clone(), term_freq, position_deltas));
        }
        Ok(())
    }       
}

// TODO use something faster than the TermFrequencyRecorder when possible.

pub struct PostingsWriter {
    postings: Vec<TermPostingsWriter<TermFrequencyRecorder>>,
    term_index: BTreeMap<Term, usize>,
}

impl PostingsWriter {

    pub fn new() -> PostingsWriter {
        PostingsWriter {
            postings: Vec::new(),
            term_index: BTreeMap::new(),
        }
    }
    
    pub fn close(&mut self,) {
        for term_postings_writer in self.postings.iter_mut() {
            term_postings_writer.close_doc();
        }
    }

    pub fn suscribe(&mut self, doc: DocId, pos: u32, term: Term) {
        let doc_ids: &mut TermPostingsWriter<TermFrequencyRecorder> = self.get_term_postings(term);
        doc_ids.suscribe(doc, pos);
    }

    fn get_term_postings(&mut self, term: Term) -> &mut TermPostingsWriter<TermFrequencyRecorder> {
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
            try!(term_postings_writer.serialize(serializer));
        }
        Ok(())
    }


}
