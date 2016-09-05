use DocId;
use schema::Term;
use schema::FieldValue;
use postings::PostingsSerializer;
use std::io;
use postings::Recorder;
use analyzer::SimpleTokenizer;
use schema::Field;
use analyzer::StreamingIterator;
use datastruct::stacker::{HashMap, Heap};

pub trait PostingsWriter {
    
    fn close(&mut self, heap: &Heap);

    fn suscribe(&mut self,  doc: DocId, pos: u32, term: &Term, heap: &Heap);

    fn serialize(&self, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()>;
    
    fn index_text<'a>(&mut self, doc_id: DocId, field: Field, field_values: &Vec<&'a FieldValue>, heap: &Heap) -> u32  {
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
                        self.suscribe(doc_id, pos, &term, heap);
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

pub struct SpecializedPostingsWriter<'a, Rec: Recorder + 'static> {
    term_index: HashMap<'a, Rec>,
}


impl<'a, Rec: Recorder + 'static> SpecializedPostingsWriter<'a, Rec> {

    pub fn new(heap: &'a Heap) -> SpecializedPostingsWriter<'a, Rec> {
        SpecializedPostingsWriter {
            term_index: HashMap::new(25, heap), // TODO compute the size of the table as a % of the heap
        }
    }

    pub fn new_boxed(heap: &'a Heap) -> Box<PostingsWriter + 'a> {
        let res = SpecializedPostingsWriter::<Rec>::new(heap);
        Box::new(res)
    } 

}

impl<'a, Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<'a, Rec> {
    
    fn close(&mut self, heap: &Heap) {
        for recorder in self.term_index.values_mut() {
            recorder.close_doc(heap);
        }
    }
    
    #[inline(always)]
    fn suscribe(&mut self, doc: DocId, position: u32, term: &Term, heap: &Heap) {
        let mut recorder = self.term_index.get_or_create(term);
        let current_doc = recorder.current_doc();
        if current_doc != doc {
            if current_doc != u32::max_value() {
                recorder.close_doc(heap);
            }
            recorder.new_doc(doc, heap);
        }
        recorder.record_position(position, heap);
    }
    
    fn serialize(&self, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()> {
        let mut term_offsets: Vec<(&[u8], (u32, &Rec))>  = self.term_index
            .iter()
            .collect();
        term_offsets.sort_by_key(|&(k, _v)| k);
        let mut term = Term::allocate(Field(0), 100);
        for (term_bytes, (addr, recorder)) in term_offsets {
            // TODO remove copy
            term.set_content(term_bytes);
            try!(serializer.new_term(&term, recorder.doc_freq()));
            try!(recorder.serialize(addr, serializer, heap));
            try!(serializer.close_term());
        }
        Ok(())
    }
    

}
