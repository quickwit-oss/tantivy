use DocId;
use std::collections::HashMap;
use schema::Term;
use postings::PostingsSerializer;
use std::io;
use postings::Recorder;


struct TermPostingsWriter<Rec: Recorder + 'static> {
    doc_ids: Vec<DocId>,
    recorder: Rec,
}

impl<Rec: Recorder + 'static> TermPostingsWriter<Rec> {
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

pub trait PostingsWriter {
    
    fn close(&mut self,);

    fn suscribe(&mut self, doc: DocId, pos: u32, term: Term);

    fn serialize(&self, serializer: &mut PostingsSerializer) -> io::Result<()>;
}

pub struct SpecializedPostingsWriter<Rec: Recorder + 'static> {
    postings: Vec<TermPostingsWriter<Rec>>,
    term_index: HashMap<Term, usize>,
}

impl<Rec: Recorder + 'static> SpecializedPostingsWriter<Rec> {

    pub fn new() -> SpecializedPostingsWriter<Rec> {
        SpecializedPostingsWriter {
            postings: Vec::new(),
            term_index: HashMap::new(),
        }
    }

    pub fn new_boxed() -> Box<PostingsWriter> {
        Box::new(Self::new())
    }

    fn get_term_postings(&mut self, term: Term) -> &mut TermPostingsWriter<Rec> {
        let unord_id: usize = {
            let num_terms = self.term_index.len();
            let postings = &mut self.postings; 
            self.term_index
                .entry(term)
                .or_insert_with(|| {
                    let unord_id = num_terms;
                    postings.push(TermPostingsWriter::new());
                    unord_id
                }).clone()
        };
        &mut self.postings[unord_id]
    }

}

impl<Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<Rec> {
    
    fn close(&mut self,) {
        for term_postings_writer in self.postings.iter_mut() {
            term_postings_writer.close_doc();
        }
    }

    fn suscribe(&mut self, doc: DocId, pos: u32, term: Term) {
        let doc_ids: &mut TermPostingsWriter<Rec> = self.get_term_postings(term);
        doc_ids.suscribe(doc, pos);
    }

    fn serialize(&self, serializer: &mut PostingsSerializer) -> io::Result<()> {
        let mut term_offsets: Vec<(&Term, usize)>  = self.term_index
            .iter()
            .map(|(k,v)| (k, *v))
            .collect();
        term_offsets.sort();
        for (term, postings_id) in term_offsets {
            let term_postings_writer = &self.postings[postings_id];
            let term_docfreq = term_postings_writer.doc_freq();
            try!(serializer.new_term(term, term_docfreq));
            try!(term_postings_writer.serialize(serializer));
        }
        Ok(())
    }


}
