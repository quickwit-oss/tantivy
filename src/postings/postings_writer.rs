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

/// The `PostingsWriter` is in charge of receiving documenting  
/// and building a `Segment` in anonymous memory.
///
/// `PostingsWriter` writes in a `Heap`.
pub trait PostingsWriter {
    
    /// Record that a document contains a term at a given position.
    ///
    /// * doc  - the document id
    /// * pos  - the term position (expressed in tokens)
    /// * term - the term
    /// * heap - heap used to store the postings informations as well as the terms
    /// in the hashmap.
    fn suscribe(&mut self,  doc: DocId, pos: u32, term: &Term, heap: &Heap);
    
    /// Serializes the postings on disk.
    /// The actual serialization format is handled by the `PostingsSerializer`.
    fn serialize(&self, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()>;
    
    /// Closes all of the currently open `Recorder`'s.
    fn close(&mut self, heap: &Heap);
        
    /// Tokenize a text and suscribe all of its token.
    fn index_text<'a>(&mut self, doc_id: DocId, field: Field, field_values: &[&'a FieldValue], heap: &Heap) -> u32  {
        let mut pos = 0u32;
        let mut num_tokens: u32 = 0u32;
        let mut term = Term::allocate(field, 100);
        for field_value in field_values {
            let mut tokens = SimpleTokenizer.tokenize(field_value.value().text());
            // right now num_tokens and pos are redundant, but it should
            // change when we get proper analyzers
            while let Some(token) = tokens.next() {
                term.set_text(token);
                self.suscribe(doc_id, pos, &term, heap);
                pos += 1u32;
                num_tokens += 1u32;
            }
            pos += 1;
            // THIS is to avoid phrase query accross field repetition.
            // span queries might still match though :|
        }
        num_tokens
    }
}

/// The `SpecializedPostingsWriter` is just here to remove dynamic
/// dispatch to the recorder information.
pub struct SpecializedPostingsWriter<'a, Rec: Recorder + 'static> {
    term_index: HashMap<'a, Rec>,
}

/// Given a `Heap` size, computes a relevant size for the `HashMap`.
fn hashmap_size_in_bits(heap_capacity: u32) -> usize {
    let num_buckets_usable = heap_capacity / 100;
    let hash_table_size = num_buckets_usable * 2;
    let mut pow = 512;
    for num_bits in 10 .. 32 {
        pow <<= 1;
        if pow > hash_table_size {
            return num_bits;
        }
    }
    32
}

impl<'a, Rec: Recorder + 'static> SpecializedPostingsWriter<'a, Rec> {
    
    /// constructor
    pub fn new(heap: &'a Heap) -> SpecializedPostingsWriter<'a, Rec> {
        let capacity = heap.capacity();
        let hashmap_size = hashmap_size_in_bits(capacity);
        SpecializedPostingsWriter {
            term_index: HashMap::new(hashmap_size, heap),
        }
    }
    
    /// Builds a `SpecializedPostingsWriter` storing its data in a heap.
    pub fn new_boxed(heap: &'a Heap) -> Box<PostingsWriter + 'a> {
        Box::new(SpecializedPostingsWriter::<Rec>::new(heap))
    } 

}

impl<'a, Rec: Recorder + 'static> PostingsWriter for SpecializedPostingsWriter<'a, Rec> {
    
    fn close(&mut self, heap: &Heap) {
        for recorder in self.term_index.values_mut() {
            recorder.close_doc(heap);
        }
    }
    
    #[inline]
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


#[test]
fn test_hashmap_size() {
    assert_eq!(hashmap_size_in_bits(10), 10);
    assert_eq!(hashmap_size_in_bits(0), 10);
    assert_eq!(hashmap_size_in_bits(100_000), 11);
    assert_eq!(hashmap_size_in_bits(300_000_000), 23);
}