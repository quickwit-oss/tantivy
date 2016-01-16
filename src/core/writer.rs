
use std::io;
use std::slice;
use core::global::*;
use core::schema::*;
use core::directory::Directory;
use core::analyzer::tokenize;
use std::collections::{HashMap, BTreeMap};
use std::collections::{hash_map, btree_map};
use core::postings::PostingsWriter;
use std::io::{BufWriter, Write};
use std::mem;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use std::iter::Peekable;
use core::serial::*;

pub struct SimplePostingsWriter {
	doc_ids: Vec<DocId>,
}

impl SimplePostingsWriter {
    pub fn new() -> SimplePostingsWriter {
        SimplePostingsWriter {
            doc_ids: Vec::new(),
        }
    }
}

impl PostingsWriter for SimplePostingsWriter {
	fn suscribe(&mut self, doc_id: DocId) {
		if self.doc_ids.len() == 0 || self.doc_ids[self.doc_ids.len() - 1] < doc_id {
			self.doc_ids.push(doc_id);
		}
	}
}

struct FieldWriter {
    postings: Vec<SimplePostingsWriter>,
    term_index: BTreeMap<String, usize>,
}

impl FieldWriter {
    pub fn new() -> FieldWriter {
        FieldWriter {
            term_index: BTreeMap::new(),
            postings: Vec::new()
        }
    }

    pub fn get_postings_writer(&mut self, term_text: &str) -> &mut SimplePostingsWriter {
        match self.term_index.get(term_text) {
            Some(unord_id) => {
                return &mut self.postings[*unord_id];
            },
            None => {}
        }
        let unord_id = self.term_index.len();
        self.postings.push(SimplePostingsWriter::new());
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
	directory: Directory,
}

impl IndexWriter {

    pub fn open(directory: &Directory) -> IndexWriter {
		IndexWriter {
            max_doc: 0,
            term_writers: HashMap::new(),
			directory: (*directory).clone(),
        }
    }

    fn get_field_writer<'a>(&'a mut self, field: &Field) -> &'a mut FieldWriter {
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

	pub fn close(self) -> ClosedIndexWriter {
		ClosedIndexWriter {
			index_writer: self
		}
	}

    pub fn sync(&mut self,) -> Result<(), io::Error> {
		self.directory.new_segment();
        Ok(())
    }

}

pub struct ClosedIndexWriter {
	index_writer: IndexWriter,
}




//////////////////////////////////
// CIWFormCursor
//
struct CIWFormCursor<'a> {
	term_it: btree_map::Iter<'a, String, usize>, // term -> postings_idx
	postings_map: &'a Vec<SimplePostingsWriter>, 	 // postings_idx -> postings
}

struct FormPostings<'a> {
	form: &'a str,
	postings: &'a SimplePostingsWriter,
}

impl<'a> Iterator for CIWFormCursor<'a> {
	type Item = FormPostings<'a>;

	fn next(&mut self,) -> Option<FormPostings<'a>> {
		self.term_it.next()
			   .map(|(form, postings_idx)| {
			FormPostings {
				form: form,
				postings: unsafe { self.postings_map.get_unchecked(*postings_idx) }
			}
		})
	}
}

//////////////////////////////////
// CIWDocCursor
//

pub struct CIWTermCursor<'a> {
	field_it: hash_map::Iter<'a, Field, FieldWriter>,
	form_it: CIWFormCursor<'a>,
	current_form_postings: Option<FormPostings<'a>>,
	field: &'a Field,
}

impl<'a> CIWTermCursor<'a> {


	fn next_form(&mut self,) -> bool {
		match self.form_it.next() {
			Some(form_postings) => {
				self.current_form_postings = Some(form_postings);
				return true;
			},
			None => { false }
		}
	}

	// Advance to the next field
	// sets up form_it to iterate on forms
	// returns true iff there was a next field
	fn next_field(&mut self,) -> bool {
		match self.field_it.next() {
			Some((field, field_writer)) => {
				self.form_it = CIWFormCursor {
					term_it: field_writer.term_index.iter(),
					postings_map: &field_writer.postings,
				};
				self.field = field;
				true
			},
			None => false,
		}
	}
}

impl<'a> TermCursor<'a> for CIWTermCursor<'a> {

	type DocCur = CIWDocCursor<'a>;

	fn get_term(&self) -> Term<'a> {
		Term {
			field: self.field.clone(),
			text: self.current_form_postings.as_ref().unwrap().form,
		}
	}

	fn doc_cursor(&self,) -> CIWDocCursor<'a> {
		CIWDocCursor {
			docs_it: self.current_form_postings
				.as_ref()
				.unwrap()
				.postings
				.doc_ids
				.iter(),
			current: None
		}
	}

	fn advance(&mut self,) -> bool {
		let next_form = self.next_form();
		if next_form {
			true
		}
		else {
			if self.next_field() {
				self.advance()
			}
			else {
				false
			}
		}
	}
}

//
// TODO use a Term type
//

impl<'a> SerializableSegment<'a> for ClosedIndexWriter {

	type TermCur = CIWTermCursor<'a>;

	fn term_cursor(&'a mut self) -> CIWTermCursor<'a> {
		let mut field_it: hash_map::Iter<'a, Field, FieldWriter> = self.index_writer.term_writers.iter();
		let (field, field_writer) = field_it.next().unwrap(); // TODO handle no field
		let mut term_cursor = CIWTermCursor {
			field_it: field_it,
			form_it: CIWFormCursor {
				term_it: field_writer.term_index.iter(),
				postings_map: &field_writer.postings,
			},
			field: field,
			current_form_postings: None,
		};
		// TODO handle having no fields at all
		term_cursor
	}
}

// TODO add positions

pub struct CIWDocCursor<'a> {
	docs_it: slice::Iter<'a, DocId>,
	current: Option<DocId>,
}

impl<'a> Iterator for CIWDocCursor<'a> {
	type Item=DocId;

	fn next(&mut self) -> Option<DocId> {
		self.current = self.docs_it.next().map(|x| *x);
		self.current
	}
}

impl<'a> DocCursor for CIWDocCursor<'a> {
	fn doc(&self,) -> DocId {
		self.current.unwrap()
	}
}
