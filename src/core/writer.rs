
use std::io;
use core::schema::Document;
use core::schema::Term;
use core::schema::Field ;
use core::analyzer::tokenize;
use std::collections::{HashMap, BTreeMap};
use core::DocId;

pub struct PostingsWriter {
	doc_ids: Vec<DocId>,
}

impl PostingsWriter {
    pub fn new()->PostingsWriter {
        PostingsWriter {
            doc_ids: Vec::new(),
        }
    }

	pub fn suscribe(&mut self, doc_id: DocId) {
		self.doc_ids.push(doc_id);
	}
}

struct FieldWriter {
    postings: Vec<PostingsWriter>,
    term_index: BTreeMap<String, usize>,
}

impl FieldWriter {
    pub fn new() -> FieldWriter {
        FieldWriter {
            term_index: BTreeMap::new(),
            postings: Vec::new()
        }
    }

    pub fn get_postings_writer(&mut self, term_text: &str) -> &mut PostingsWriter {
        match self.term_index.get(term_text) {
            Some(unord_id) => {
                return &mut self.postings[*unord_id];
            },
            None => {}
        }
        let unord_id = self.term_index.len();
        self.postings.push(PostingsWriter::new());
        self.term_index.insert(String::from(term_text), unord_id.clone());
        &mut self.postings[unord_id]
    }

    pub fn suscribe(&mut self, doc: DocId, term_text: &str) {
        self.get_postings_writer(term_text).suscribe(doc);
    }
}

pub struct IndexWriter {
    max_doc: usize,
    term_writers: HashMap<Field, FieldWriter>,
}

impl IndexWriter {

    pub fn new() -> IndexWriter {
        IndexWriter {
            max_doc: 0,
            term_writers: HashMap::new(),
        }
    }

    fn get_field_writer(&mut self, field: &Field) -> &mut FieldWriter {
        if !self.term_writers.contains_key(field) {
            self.term_writers.insert((*field).clone(), FieldWriter::new());
        }
        self.term_writers.get_mut(field).unwrap()
    }

    pub fn add(&mut self, doc: Document) {
        let doc_id = self.max_doc;
        for field_value in doc {
            let field = field_value.field;
            let field_writer = self.get_field_writer(&field);
            for token in tokenize(&field_value.text) {
                field_writer.suscribe(doc_id, token);
            }
        }
        self.max_doc += 1;
    }

    pub fn sync(&mut self,) -> Result<(), io::Error> {
        Ok(())
    }

}
