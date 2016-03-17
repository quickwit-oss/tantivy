use core::schema::*;
use core::codec::*;
use std::io;
use std::rc::Rc;
use core::index::Index;
use core::analyzer::SimpleTokenizer;
use core::serial::SerializableSegment;
use core::analyzer::StreamingIterator;
use core::index::Segment;
use core::index::SegmentInfo;
use core::postings::PostingsWriter;
use core::fastfield::FastFieldWriters;


pub struct IndexWriter {
	segment_writer: Rc<SegmentWriter>,
	directory: Index,
	schema: Schema,
}


impl IndexWriter {

    pub fn open(directory: &Index) -> io::Result<IndexWriter> {
		let segment = directory.new_segment();
		let schema = directory.schema();
		let segment_writer = try!(SegmentWriter::for_segment(segment, &schema));
		Ok(IndexWriter {
			segment_writer: Rc::new(segment_writer),
			directory: directory.clone(),
			schema: schema,
		})
    }

    pub fn add(&mut self, doc: Document) -> io::Result<()> {
        Rc::get_mut(&mut self.segment_writer).unwrap().add(doc, &self.schema)
    }

    pub fn commit(&mut self,) -> io::Result<Segment> {
		let segment_writer_rc = self.segment_writer.clone();
		let segment = self.directory.new_segment();
		self.segment_writer = Rc::new(try!(SegmentWriter::for_segment(segment, &self.schema)));
		match Rc::try_unwrap(segment_writer_rc) {
			Ok(segment_writer) => {
				let segment = segment_writer.segment();
				try!(segment_writer.finalize());
				try!(self.directory.sync(segment.clone()));
				try!(self.directory.publish_segment(segment.clone()));
				Ok(segment)
			},
			Err(_) => {
				panic!("error while acquiring segment writer.");
			}
		}
	}

}


pub struct SegmentWriter {
    max_doc: DocId,
	tokenizer: SimpleTokenizer,
	postings_writer: PostingsWriter,
	segment_serializer: SegmentSerializer,
}

impl SegmentWriter {

	// Write on disk all of the stuff that
	// is still on RAM :
	// - the dictionary in an fst
	// - the postings
	// - the segment info
	fn finalize(mut self,) -> io::Result<()> {
		try!(self.postings_writer.serialize(&mut self.segment_serializer));
		{
			let segment_info = SegmentInfo {
				max_doc: self.max_doc
			};
			try!(self.segment_serializer.write_segment_info(&segment_info));
		}
		self.segment_serializer.close()
	}

	pub fn max_doc(&self,) -> DocId {
		self.max_doc
	}

	pub fn segment(&self,) -> Segment {
		self.segment_serializer.segment()
	}

	fn for_segment(segment: Segment, schema: &Schema) -> io::Result<SegmentWriter> {
		let segment_serializer = try!(SegmentSerializer::for_segment(&segment));
		Ok(SegmentWriter {
			max_doc: 0,
			postings_writer: PostingsWriter::new(),
			segment_serializer: segment_serializer,
			tokenizer: SimpleTokenizer::new(),
		})
	}

    pub fn add(&mut self, doc: Document, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        for field_value in doc.text_fields() {
			let field_options = schema.text_field_options(&field_value.field);
			if field_options.is_tokenized_indexed() {
				let mut tokens = self.tokenizer.tokenize(&field_value.text);
				loop {
					match tokens.next() {
						Some(token) => {
							let term = Term::from_field_text(&field_value.field, token);
							self.postings_writer.suscribe(doc_id, term);
						},
						None => { break; }
					}
				}
			}
		}
		for field_value in doc.u32_fields() {
            let field_options = schema.u32_field_options(&field_value.field);
            if field_options.is_indexed() {
                let term = Term::from_field_u32(&field_value.field, field_value.value);
                self.postings_writer.suscribe(doc_id, term);
            }
		}
		let mut stored_fieldvalues_it = doc.text_fields().filter(|text_field_value| {
			schema.text_field_options(&text_field_value.field).is_stored()
		});
		// try!(self.fastfield_writers.add_doc(&doc));
		try!(self.segment_serializer.store_doc(&mut stored_fieldvalues_it));
        self.max_doc += 1;
		Ok(())
    }

}

impl SerializableSegment for SegmentWriter {
	fn write(&self, mut serializer: SegmentSerializer) -> io::Result<()> {
		try!(self.postings_writer.serialize(&mut serializer));
		serializer.close()
	}
}
