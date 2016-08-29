use DocId;
use std::collections::HashMap;
use schema::Term;
use schema::FieldValue;
use postings::PostingsSerializer;
use std::io;
use postings::Recorder;
use postings::block_store::BlockStore;
use analyzer::SimpleTokenizer;
use schema::Field;
use analyzer::StreamingIterator;

pub trait PostingsWriter {
    
    fn close(&mut self, block_store: &mut BlockStore);

    fn suscribe(&mut self, block_store: &mut BlockStore, doc: DocId, pos: u32, term: &Term);

    fn serialize(&self, block_store: &BlockStore, serializer: &mut PostingsSerializer) -> io::Result<()>;
    
    fn index_text<'a>(&mut self, block_store: &mut BlockStore, doc_id: DocId, field: Field, field_values: &Vec<&'a FieldValue>) -> u32  {
        let mut pos = 0u32;
        let mut num_tokens: u32 = 0u32;
        let mut term = Term::allocate(field, 100);
        for field_value in field_values {
            let mut tokens = SimpleTokenizer.tokenize(field_value.value().text());
            // right now num_tokens and pos are redundant, but it should
            // change when we get proper analyzers
            loop {
                match tokens.next() {
                    Some(token) => {
                        term.set_text(token);
                        self.suscribe(block_store, doc_id, pos, &term);
                        pos += 1u32;
                        num_tokens += 1u32;
                    },
                    None => { break; }
                }
            }
            pos += 1;
            // THIS is to avoid phrase query accross field repetition.
            // span queries might still match though :|
        }
        num_tokens
    }
}

pub struct SpecializedPostingsWriter<Rec: Recorder + 'static> {
    term_index: HashMap<Term, Rec>,
}

#[inline(always)]
fn get_or_create_recorder<'a, Rec: Recorder>(term: &Term, term_index: &'a mut HashMap<Term, Rec>, block_store: &mut BlockStore) -> &'a mut Rec {
    if term_index.contains_key(term) {
        term_index.get_mut(term).expect("The term should be here as we just checked it")
    }
    else {
        term_index
        .entry(term.clone())
        .or_insert_with(|| Rec::new(block_store))
    }
    
   
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
    
    #[inline(always)]
    fn suscribe(&mut self, block_store: &mut BlockStore, doc: DocId, position: u32, term: &Term) {
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
