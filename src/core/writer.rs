
use std::io;
use std::slice;
use core::global::*;
use core::schema::*;
use core::codec::*;
use core::directory::Directory;
use core::analyzer::tokenize;
use std::collections::{HashMap, BTreeMap};
use std::collections::{hash_map, btree_map};
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::mem;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use std::iter::Peekable;
use core::serial::*;
use core::error::*;
use std::cell::RefCell;
use std::borrow::BorrowMut;
use core::directory::Segment;

pub struct PostingsWriter {
	doc_ids: Vec<DocId>,
}

impl PostingsWriter {

    pub fn new() -> PostingsWriter {
        PostingsWriter {
            doc_ids: Vec::new(),
        }
    }

	fn suscribe(&mut self, doc_id: DocId) {
		if self.doc_ids.len() == 0 || self.doc_ids[self.doc_ids.len() - 1] < doc_id {
			self.doc_ids.push(doc_id);
		}
	}
}

pub struct IndexWriter {
	segment_writer: SegmentWriter,
	directory: Directory,
	schema: Schema,
}

impl IndexWriter {

    pub fn open(directory: &Directory) -> IndexWriter {
		let schema = directory.schema();
		IndexWriter {
			segment_writer: SegmentWriter::new(),
			directory: directory.clone(),
			schema: schema,
		}
    }

    pub fn add(&mut self, doc: Document) {
        self.segment_writer.add(doc, &self.schema);
    }

	// TODO remove that some day
	pub fn current_segment_writer(&self,) -> &SegmentWriter {
		&self.segment_writer
	}

    pub fn commit(&mut self,) -> Result<Segment> {
		let segment = self.directory.new_segment();
		try!(SimpleCodec::write(&self.segment_writer, &segment).map(|sz| (segment.clone(), sz)));
		// At this point, the segment is written
		// We still need to sync all of the file, as well as the parent directory.
		try!(self.directory.sync(segment.clone()));
		self.directory.publish_segment(segment.clone());
		self.segment_writer = SegmentWriter::new();
		Ok(segment)
	}

}


pub struct SegmentWriter {
    max_doc: DocId,
    postings: Vec<PostingsWriter>,
	term_index: BTreeMap<Term, usize>,
}


impl SegmentWriter {

	fn new() -> SegmentWriter {
		SegmentWriter {
			max_doc: 0,
			postings: Vec::new(),
			term_index: BTreeMap::new(),
		}
	}

    pub fn add(&mut self, doc: Document, schema: &Schema) {
        let doc_id = self.max_doc;
        for field_value in doc.fields() {
			let field_options = schema.get_field(field_value.field.clone());
			if field_options.is_tokenized_indexed() {
				for token in tokenize(&field_value.text) {
					let term = Term::from_field_text(&field_value.field, token);
	                self.suscribe(doc_id, term);
	            }
			}
		}
        self.max_doc += 1;
    }

	pub fn get_postings_writer(&mut self, term: Term) -> &mut PostingsWriter {
        match self.term_index.get(&term) {
            Some(unord_id) => {
                return &mut self.postings[*unord_id];
            },
            None => {}
        }
        let unord_id = self.term_index.len();
        self.postings.push(PostingsWriter::new());
        self.term_index.insert(term, unord_id.clone());
        &mut self.postings[unord_id]
    }

	pub fn suscribe(&mut self, doc: DocId, term: Term) {
        self.get_postings_writer(term).suscribe(doc);

    }

}

impl SerializableSegment for SegmentWriter {
	fn write<Output, SegSer: SegmentSerializer<Output>>(&self, mut serializer: SegSer) -> Result<Output> {
    	for (term, postings_id) in self.term_index.iter() {
			let doc_ids = &self.postings[postings_id.clone()].doc_ids;
			let term_docfreq = doc_ids.len() as u32;
			serializer.new_term(&term, term_docfreq);
			for doc_id in doc_ids {
				serializer.add_doc(doc_id.clone());
			}
		}
		serializer.close()
	}
}
