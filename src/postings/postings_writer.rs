use DocId;
use std::collections::HashMap;
use schema::Term;
use postings::PostingsSerializer;
use std::io;
use postings::Recorder;
use postings::block_store::BlockStore;


pub trait PostingsWriter {
    
    fn close(&mut self, block_store: &mut BlockStore);

    fn suscribe(&mut self, block_store: &mut BlockStore, doc: DocId, pos: u32, term: Term);

    fn serialize(&self, block_store: &BlockStore, serializer: &mut PostingsSerializer) -> io::Result<()>;
}

pub struct SpecializedPostingsWriter<Rec: Recorder + 'static> {
    term_index: HashMap<Term, Rec>,
}


fn get_or_create_recorder<'a, Rec: Recorder>(term: Term, term_index: &'a mut HashMap<Term, Rec>, block_store: &mut BlockStore) -> &'a mut Rec {
    term_index
        .entry(term)
        .or_insert_with(|| Rec::new(block_store))    
}

impl<Rec: Recorder + 'static> SpecializedPostingsWriter<Rec> {

    pub fn new() -> SpecializedPostingsWriter<Rec> {
        SpecializedPostingsWriter {
            term_index: HashMap::new(),
        }
    }

    pub fn new_boxed() -> Box<PostingsWriter> {
        Box::new(Self::new())
    }
    
}

impl<Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<Rec> {
    
    fn close(&mut self, block_store: &mut BlockStore) {
        for recorder in self.term_index.values_mut() {
            recorder.close_doc(block_store);
        }
    }
    
    fn suscribe(&mut self, block_store: &mut BlockStore, doc: DocId, position: u32, term: Term) {
        let mut recorder = get_or_create_recorder(term, &mut self.term_index, block_store);
        let current_doc = recorder.current_doc();
        if current_doc != doc {
            if current_doc != u32::max_value() {
                recorder.close_doc(block_store);
            }
            recorder.new_doc(block_store, doc);
        }
        recorder.record_position(block_store, position);
    }
    
    fn serialize(&self, block_store: &BlockStore, serializer: &mut PostingsSerializer) -> io::Result<()> {
        let mut term_offsets: Vec<(&Term, &Rec)>  = self.term_index
            .iter()
            .map(|(k,v)| (k, v))
            .collect();
        term_offsets.sort_by_key(|&(k, _v)| k);
        for (term, recorder) in term_offsets {
            try!(serializer.new_term(term, recorder.doc_freq()));
            try!(recorder.serialize(serializer, block_store));
            try!(serializer.close_term());
        }
        Ok(())
    }


}
