use DocId;
use schema::Schema;
use schema::Document;
use schema::Term;
use schema::FieldEntry;
use core::codec::SegmentSerializer;
use core::index::SegmentInfo;
use core::index::Segment;
use analyzer::SimpleTokenizer;
use core::index::SerializableSegment;
use analyzer::StreamingIterator;
use postings::PostingsWriter;
use fastfield::U32FastFieldsWriter;
use std::clone::Clone;
use std::io;
use schema::FieldValue;

pub struct SegmentWriter {
    max_doc: DocId,
	tokenizer: SimpleTokenizer,
	postings_writer: PostingsWriter,
	segment_serializer: SegmentSerializer,
	fast_field_writers: U32FastFieldsWriter,
}

impl SegmentWriter {

	pub fn for_segment(segment: Segment, schema: &Schema) -> io::Result<SegmentWriter> {
		let segment_serializer = try!(SegmentSerializer::for_segment(&segment));
		Ok(SegmentWriter {
			max_doc: 0,
			postings_writer: PostingsWriter::new(),
			segment_serializer: segment_serializer,
			tokenizer: SimpleTokenizer::new(),
			fast_field_writers: U32FastFieldsWriter::from_schema(schema),
		})
	}

	// Write on disk all of the stuff that
	// is still on RAM :
	// - the dictionary in an fst
	// - the postings
	// - the segment info
	// The segment writer cannot be used after this.
	pub fn finalize(mut self,) -> io::Result<()> {
		let segment_info = self.segment_info();
		self.postings_writer.close();
		write(&self.postings_writer,
			  &self.fast_field_writers,
			  segment_info,
			  self.segment_serializer)
	}

    pub fn add_document(&mut self, doc: &Document, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        for field_value in doc.get_fields() {
			let field_options = schema.get_field_entry(field_value.field());
			match *field_options {
				FieldEntry::Text(_, ref text_options) => {
					if text_options.get_indexing_options().is_tokenized() {
						let mut tokens = self.tokenizer.tokenize(field_value.text());
						let mut pos = 0u32;
						let field = field_value.field();
						loop {
							match tokens.next() {
								Some(token) => {
									let term = Term::from_field_text(field, token);
									self.postings_writer.suscribe(doc_id, pos, term);
									pos += 1;
								},
								None => { break; }
							}
						}
					}
					// TODO untokenized yet indexed
				}
				FieldEntry::U32(_, ref u32_options) => {
					if u32_options.is_indexed() {
						let term = Term::from_field_u32(field_value.field(), field_value.u32_value());
						self.postings_writer.suscribe(doc_id, 0.clone(), term);
					}
				}
			}
		}
		self.fast_field_writers.add_document(&doc);
		let stored_fieldvalues: Vec<&FieldValue> = doc
			.get_fields()
			.iter()
			.filter(|field_value| schema.get_field_entry(field_value.field()).is_stored())
			.collect();
		let doc_writer = self.segment_serializer.get_store_writer();
		try!(doc_writer.store(&stored_fieldvalues));
        self.max_doc += 1;
		Ok(())
    }

	fn segment_info(&self,) -> SegmentInfo {
		SegmentInfo {
			max_doc: self.max_doc
		}
	}
}

fn write(postings_writer: &PostingsWriter,
		 fast_field_writers: &U32FastFieldsWriter,
		 segment_info: SegmentInfo,
	  	mut serializer: SegmentSerializer) -> io::Result<()> {
		try!(postings_writer.serialize(serializer.get_postings_serializer()));
		try!(fast_field_writers.serialize(serializer.get_fast_field_serializer()));
		try!(serializer.write_segment_info(&segment_info));
		try!(serializer.close());
		Ok(())
}

impl SerializableSegment for SegmentWriter {
	fn write(&self, serializer: SegmentSerializer) -> io::Result<()> {
		write(&self.postings_writer,
		      &self.fast_field_writers,
			  self.segment_info(),
		      serializer)
	}
}
