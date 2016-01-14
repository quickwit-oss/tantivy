
use std::io;
use core::schema::Document;
use core::schema::Field;
use core::directory::Directory;
use core::analyzer::tokenize;
use std::collections::{HashMap, BTreeMap};
use std::collections::{hash_map, btree_map};
use core::DocId;
use core::postings::PostingsWriter;
use core::global::Flushable;
use std::io::{BufWriter, Write};
use std::mem;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use std::iter::Peekable;
use core::serial::{FieldCursor, TermCursor, DocCursor, SerializableSegment};

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
		self.doc_ids.push(doc_id);
	}
}

struct FieldWriter {
    postings: Vec<SimplePostingsWriter>,
    term_index: BTreeMap<String, usize>,
}

impl Flushable for SimplePostingsWriter {
	fn flush<W: Write>(&self, writer: &mut W) -> Result<usize, io::Error> {
		let mut num_bytes_written = 0;
		let num_docs = self.doc_ids.len() as u64;
		writer.write_u64::<NativeEndian>(num_docs);
		num_bytes_written += 8;
		for &doc_id in self.doc_ids.iter() {
			writer.write_u64::<NativeEndian>(doc_id as u64);
			num_bytes_written += 8;
		}
		Ok(num_bytes_written)
	}
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



//-----------------------------------------
// Implementation of SerializableSegment
//

pub struct CIWFieldCursor<'a> {
	field_it: hash_map::Iter<'a, Field, FieldWriter>,
	current: Option<(&'a Field, &'a FieldWriter)>
}

impl<'a> CIWFieldCursor<'a> {
	fn get_field_writer(&self) -> &'a FieldWriter {
		self.current.map(|(_, second)| second).unwrap()
	}
}

impl<'a> Iterator for CIWFieldCursor<'a> {
	type Item=&'a Field;

	fn next(&mut self) -> Option<&'a Field> {
		self.current = self.field_it.next();
		self.get_field()
	}
}

impl<'a> FieldCursor<'a> for CIWFieldCursor<'a> {

	type TTermCur = CIWTermCursor<'a>;

	fn get_field(&self) -> Option<&'a Field> {
		self.current.map(|(first, _)| first)
	}

	fn term_cursor<'b>(&'b self) -> CIWTermCursor<'b>  {
		let field_writer = self.get_field_writer();
		CIWTermCursor {
			postings: &field_writer.postings,
			term_it: field_writer.term_index.iter(),
			current: None
		}
	}
}

// TODO use a Term type

impl<'a> SerializableSegment<'a> for ClosedIndexWriter {

	type TFieldCur = CIWFieldCursor<'a>;

	fn field_cursor(&'a self) -> CIWFieldCursor<'a> {
		let mut field_it: hash_map::Iter<'a, Field, FieldWriter> = self.index_writer.term_writers.iter();
		let current: Option<(&'a Field, &'a FieldWriter)> = None;
		CIWFieldCursor {
				current: current,
				field_it: field_it
		}
	}
}

//////////////////////////////////
// CIWTermCursor
//
pub struct CIWTermCursor<'a> {
	postings: &'a Vec<SimplePostingsWriter>,
	term_it: btree_map::Iter<'a, String, usize>,
	current: Option<(&'a String, &'a usize)>
}

impl<'a> CIWTermCursor<'a> {
    fn get_term_option(&self) -> Option<&'a String> {
		self.current
			.map(|(first, _)| first)
	}
}

impl<'a> Iterator for CIWTermCursor<'a> {
	type Item=&'a String;

	fn next(&mut self) -> Option<&'a String> {
		self.current = self.term_it.next();
		self.get_term_option()
	}
}

impl<'a> TermCursor<'a> for CIWTermCursor<'a> {
	type TDocCur = CIWDocCursor<'a>;

	fn doc_cursor(&self) -> CIWDocCursor<'a> {
		let (_, &postings_id) = self.current.unwrap();
		unsafe {
			let postings_writer = self.postings.get_unchecked(postings_id);
			let docs_it = postings_writer.doc_ids.iter();
			CIWDocCursor {
				docs_it: Box::new(docs_it),
				current: None,
			}
		}
	}

    fn get_term(&self) -> &'a String {
		self.get_term_option()
			.unwrap()
	}
}

//////////////////////////////////
// CIWDocCursor
//

// TODO add positions

pub struct CIWDocCursor<'a> {
	docs_it: Box<Iterator<Item=&'a DocId> + 'a>,
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
