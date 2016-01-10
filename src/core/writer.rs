
use std::io;
use core::schema::Document;
use core::schema::Term;
use core::analyzer::tokenize;
use std::collections::HashMap;
//
// struct TermDictionary {
//     map: HashMap<Term, usize>,
// }
//
// struct TermId(usize);
//
// impl TermDictionary {
//
//     pub fn new() -> TermDictionary {
//         TermDictionary {
//             map: HashMap::new(),
//         }
//     }
//
//     pub fn term_id(&mut self, term: &Term) -> TermId {
//         match self.map.get(term) {
//             Some(usize) => { return TermId(usize); },
//             None => {}
//         }
//         let term_id = self.map.len();
//         self.map.insert(term, term_id);
//         TermId(term_id)
//
//     }
// }

struct IndexWriter {
    max_doc: usize,

}

impl IndexWriter {

    fn suscribe(&mut self, term: &Term, doc_id: usize) {

    }

    pub fn add(&mut self, doc: Document) {
        let doc_id = self.max_doc;
        for field_value in doc {
            for token in tokenize(&field_value.text) {
                let term = Term {
                    field: &field_value.field,
                    text: &token
                };
                self.suscribe(&term, doc_id);
            }
        }
        self.max_doc += 1;
    }

    pub fn sync(&mut self,) -> Result<(), io::Error> {
        Ok(())
    }

}
