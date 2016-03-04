use core::schema::*;
use core::codec::*;
use std::io;
use std::rc::Rc;
use core::index::Index;
use core::analyzer::SimpleTokenizer;
use std::collections::BTreeMap;
use core::serial::{SegmentSerializer, SerializableSegment};
use core::analyzer::StreamingIterator;
use std::io::Error as IOError;
use core::index::Segment;


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
	segment_writer: Rc<SegmentWriter>,
	directory: Index,
	schema: Schema,
}

fn new_segment_writer(directory: &Index, ) -> SegmentWriter {
	let segment = directory.new_segment();
	SegmentWriter::for_segment(segment)
}

impl IndexWriter {

    pub fn open(directory: &Index) -> IndexWriter {
		let schema = directory.schema();
		IndexWriter {
			segment_writer: Rc::new(new_segment_writer(&directory)),
			directory: directory.clone(),
			schema: schema,
		}
    }

    pub fn add(&mut self, doc: Document) {
        Rc::get_mut(&mut self.segment_writer).unwrap().add(doc, &self.schema);
    }

	// TODO remove that some day
	pub fn current_segment_writer(&self,) -> &SegmentWriter {
		&self.segment_writer
	}

    pub fn commit(&mut self,) -> Result<Segment, IOError> {
		// TODO error handling
		let segment_writer_rc = self.segment_writer.clone();
		self.segment_writer = Rc::new(new_segment_writer(&self.directory));
		let segment_writer_res = Rc::try_unwrap(segment_writer_rc);
		match segment_writer_res {
			Ok(segment_writer) => {
				let segment = segment_writer.segment();
				segment_writer.write_pending();
				// write(self.segment_serializer);
				// try!(SimpleCodec::write(&self.segment_writer, &segment).map(|sz| (segment.clone(), sz)));
				// At this point, the segment is written
				// We still need to sync all of the file, as well as the parent directory.
				try!(self.directory.sync(segment.clone()));
				self.directory.publish_segment(segment.clone());
				Ok(segment)
			},
			Err(_) => {
				panic!("error while acquiring segment writer.");
			}
		}

	}

}


pub struct SegmentWriter {
	num_tokens: usize,
    max_doc: DocId,
    postings: Vec<PostingsWriter>,
	term_index: BTreeMap<Term, usize>,
	tokenizer: SimpleTokenizer,
	segment_serializer: SimpleSegmentSerializer,
}

impl SegmentWriter {

	// write on disk all of the stuff that
	// are still on RAM.
	// for this version, that's the term dictionary
	// and the postings
	fn write_pending(mut self,) -> Result<(), IOError> {
		{
		for (term, postings_id) in self.term_index.iter() {
			let doc_ids = &self.postings[postings_id.clone()].doc_ids;
			let term_docfreq = doc_ids.len() as u32;
			self.segment_serializer.new_term(&term, term_docfreq);
			self.segment_serializer.write_docs(&doc_ids);
		}
		}
		self.segment_serializer.close()
	}

	pub fn segment(&self,) -> Segment {
		self.segment_serializer.segment()
	}

	fn for_segment(segment: Segment) -> SegmentWriter {
		// TODO handle error
		let segment_serializer = SimpleCodec::serializer(&segment).unwrap();
		SegmentWriter {
			num_tokens: 0,
			max_doc: 0,
			postings: Vec::new(),
			term_index: BTreeMap::new(),
			tokenizer: SimpleTokenizer::new(),
			segment_serializer: segment_serializer,
		}
	}

    pub fn add(&mut self, doc: Document, schema: &Schema) {
        let doc_id = self.max_doc;
        for field_value in doc.fields() {
			let field_options = schema.field_options(&field_value.field);
			if field_options.is_tokenized_indexed() {
				let mut tokens = self.tokenizer.tokenize(&field_value.text);
				loop {
					match tokens.next() {
						Some(token) => {
							let term = Term::from_field_text(&field_value.field, token);
							self.suscribe(doc_id, term);
							self.num_tokens += 1;
						},
						None => { break; }
					}
				}
			}
		}
		let mut stored_fieldvalues_it = doc.fields().filter(|field_value| {
			schema.field_options(&field_value.field).is_stored()
		});
		self.segment_serializer.store_doc(&mut stored_fieldvalues_it);
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
	fn write<Output, SegSer: SegmentSerializer<Output>>(&self, mut serializer: SegSer) -> io::Result<Output> {
    	for (term, postings_id) in self.term_index.iter() {
			let doc_ids = &self.postings[postings_id.clone()].doc_ids;
			let term_docfreq = doc_ids.len() as u32;
			serializer.new_term(&term, term_docfreq);
			serializer.write_docs(&doc_ids);
		}
		serializer.close()
	}
}
