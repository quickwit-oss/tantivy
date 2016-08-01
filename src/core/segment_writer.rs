use DocId;
use schema::Schema;
use schema::Document;
use schema::Term;
use schema::FieldEntry;
use core::segment_serializer::SegmentSerializer;
use core::index::SegmentInfo;
use core::index::Segment;
use analyzer::SimpleTokenizer;
use core::index::SerializableSegment;
use analyzer::StreamingIterator;
use postings::PostingsWriter;
use fastfield::U32FastFieldsWriter;
use std::clone::Clone;
use std::io;
use schema::Field;
use schema::FieldValue;

pub struct SegmentWriter {
    max_doc: DocId,
	tokenizer: SimpleTokenizer,
	postings_writer: PostingsWriter,
	segment_serializer: SegmentSerializer,
	fast_field_writers: U32FastFieldsWriter,
	fieldnorms_writer: U32FastFieldsWriter,
}

fn create_fieldnorms_writer(schema: &Schema) -> U32FastFieldsWriter {
	let u32_fields: Vec<Field> = schema.fields()
		.iter()
		.enumerate()
		.filter(|&(_, field_entry)| field_entry.is_indexed()) 
		.map(|(field_id, _)| Field(field_id as u8))
		.collect();
	U32FastFieldsWriter::new(u32_fields)
}

impl SegmentWriter {
	

	
	pub fn for_segment(segment: Segment, schema: &Schema) -> io::Result<SegmentWriter> {
		let segment_serializer = try!(SegmentSerializer::for_segment(&segment));
		Ok(SegmentWriter {
			max_doc: 0,
			postings_writer: PostingsWriter::new(),
			fieldnorms_writer: create_fieldnorms_writer(schema),
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
	// The segment writer cannot be used after this, which is
	// enforced by the fact that "self" is moved.
	pub fn finalize(mut self,) -> io::Result<()> {
		let segment_info = self.segment_info();
		self.postings_writer.close();
		write(&self.postings_writer,
			  &self.fast_field_writers,
			  &self.fieldnorms_writer,
			  segment_info,
			  self.segment_serializer)
	}

    pub fn add_document(&mut self, doc: &Document, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        for (field, field_values) in doc.get_sorted_fields() {
			let mut num_tokens: usize = 0;
			for field_value in field_values {
				let field_options = schema.get_field_entry(field);
				match *field_options {
					FieldEntry::Text(_, ref text_options) => {
						if text_options.get_indexing_options().is_tokenized() {
							let mut tokens = self.tokenizer.tokenize(field_value.text());
							// right now num_tokens and pos are redundant, but it should
							// change when we get proper analyzers
							
							let mut pos = 0u32;
							let field = field_value.field();
							loop {
								match tokens.next() {
									Some(token) => {
										let term = Term::from_field_text(field, token);
										self.postings_writer.suscribe(doc_id, pos, term);
										pos += 1;
										num_tokens += 1;
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
			self.fieldnorms_writer
				.get_field_writer(field)
				.map(|field_norms_writer| {
					field_norms_writer.add_val(num_tokens as u32)
				});
		}
		
		self.fieldnorms_writer.fill_val_up_to(doc_id);
		
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
		 fieldnorms_writer: &U32FastFieldsWriter,
		 segment_info: SegmentInfo,
	  	mut serializer: SegmentSerializer) -> io::Result<()> {
		try!(postings_writer.serialize(serializer.get_postings_serializer()));
		try!(fast_field_writers.serialize(serializer.get_fast_field_serializer()));
		try!(fieldnorms_writer.serialize(serializer.get_fieldnorms_serializer()));
		try!(serializer.write_segment_info(&segment_info));
		try!(serializer.close());
		Ok(())
}

impl SerializableSegment for SegmentWriter {
	fn write(&self, serializer: SegmentSerializer) -> io::Result<()> {
		write(&self.postings_writer,
		      &self.fast_field_writers,
			  &self.fieldnorms_writer,
			  self.segment_info(),
		      serializer)
	}
}
