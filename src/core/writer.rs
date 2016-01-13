
use std::io;
use core::schema::Document;
use core::schema::Field;
use core::directory::Directory;
use core::analyzer::tokenize;
use std::collections::{HashMap, BTreeMap};
use core::DocId;
use core::postings::PostingsWriter;
use core::global::Flushable;
use std::io::{BufWriter, Write};
use std::mem;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};


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
		self.directory.new_segment();
        Ok(())
    }

}
