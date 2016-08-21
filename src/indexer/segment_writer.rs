use Result;
use DocId;
use std::io;
use schema::Schema;
use schema::Document;
use schema::Term;
use core::index::SegmentInfo;
use core::index::Segment;
use analyzer::SimpleTokenizer;
use core::index::SerializableSegment;
use analyzer::StreamingIterator;
use postings::PostingsWriter;
use fastfield::U32FastFieldsWriter;
use schema::Field;
use schema::FieldEntry;
use schema::FieldValue;
use schema::FieldType;
use schema::TextIndexingOptions;
use postings::SpecializedPostingsWriter;
use postings::{NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
use indexer::segment_serializer::SegmentSerializer;

pub struct SegmentWriter {
    max_doc: DocId,
	tokenizer: SimpleTokenizer,
	per_field_postings_writers: Vec<Box<PostingsWriter>>,
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

fn posting_from_field_entry(field_entry: &FieldEntry) -> Box<PostingsWriter> {
	match field_entry.field_type() {
		&FieldType::Str(ref text_options) => {
			match text_options.get_indexing_options() {
				TextIndexingOptions::TokenizedWithFreq => {
					SpecializedPostingsWriter::<TermFrequencyRecorder>::new_boxed()
				}
				TextIndexingOptions::TokenizedWithFreqAndPosition => {
					SpecializedPostingsWriter::<TFAndPositionRecorder>::new_boxed()
				}
				_ => {
					SpecializedPostingsWriter::<NothingRecorder>::new_boxed()
				}
			}
		} 
		&FieldType::U32(_) => {
			SpecializedPostingsWriter::<NothingRecorder>::new_boxed()
		}
	}
}


impl SegmentWriter {

	pub fn for_segment(segment: Segment, schema: &Schema) -> Result<SegmentWriter> {
		let segment_serializer = try!(SegmentSerializer::for_segment(&segment));
		let per_field_postings_writers = schema.fields()
			  .iter()
			  .map(|field_entry| {
				  posting_from_field_entry(field_entry)
			  })
			  .collect();
		Ok(SegmentWriter {
			max_doc: 0,
			per_field_postings_writers: per_field_postings_writers,
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
	pub fn finalize(mut self,) -> Result<()> {
		let segment_info = self.segment_info();
		for per_field_postings_writer in self.per_field_postings_writers.iter_mut() {
			per_field_postings_writer.close();
		}
		write(&self.per_field_postings_writers,
			  &self.fast_field_writers,
			  &self.fieldnorms_writer,
			  segment_info,
			  self.segment_serializer)
	}

    pub fn add_document(&mut self, doc: &Document, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        for (field, field_values) in doc.get_sorted_fields() {
			// TODO pos collision if the field is redundant				
			let field_posting_writers: &mut Box<PostingsWriter> = &mut self.per_field_postings_writers[field.0 as usize];
			let field_options = schema.get_field_entry(field);
			match *field_options.field_type() {
				FieldType::Str(ref text_options) => {
					let mut pos = 0u32;
					let mut num_tokens: usize = 0;
					for field_value in field_values {
						if text_options.get_indexing_options().is_tokenized() {
							let mut tokens = self.tokenizer.tokenize(field_value.value().text());
							// right now num_tokens and pos are redundant, but it should
							// change when we get proper analyzers
							let field = field_value.field();
							loop {
								match tokens.next() {
									Some(token) => {
										let term = Term::from_field_text(field, token);
										field_posting_writers.suscribe(doc_id, pos, term);
										pos += 1;
										num_tokens += 1;
									},
									None => { break; }
								}
							}
						}
						else {
							let term = Term::from_field_text(field, field_value.value().text());
							field_posting_writers.suscribe(doc_id, 0, term);
						}
						pos += 1;
						// THIS is to avoid phrase query accross field repetition.
						// span queries might still match though :|
					}
					self.fieldnorms_writer
						.get_field_writer(field)
						.map(|field_norms_writer| {
							field_norms_writer.add_val(num_tokens as u32)
						});
				}
				FieldType::U32(ref u32_options) => {
					if u32_options.is_indexed() {
						for field_value in field_values {
							let term = Term::from_field_u32(field_value.field(), field_value.value().u32_value());
							field_posting_writers.suscribe(doc_id, 0, term);
						}
					}
				}
			}

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

	pub fn max_doc(&self,) -> u32 {
		self.max_doc
	}

}

fn write(per_field_postings_writers: &Vec<Box<PostingsWriter>>,
		 fast_field_writers: &U32FastFieldsWriter,
		 fieldnorms_writer: &U32FastFieldsWriter,
		 segment_info: SegmentInfo,
	  	mut serializer: SegmentSerializer) -> Result<()> {
		for per_field_postings_writer in per_field_postings_writers.iter() {
			try!(per_field_postings_writer.serialize(serializer.get_postings_serializer()));
		}
		try!(fast_field_writers.serialize(serializer.get_fast_field_serializer()));
		try!(fieldnorms_writer.serialize(serializer.get_fieldnorms_serializer()));
		try!(serializer.write_segment_info(&segment_info));
		try!(serializer.close());
		Ok(())
}

impl SerializableSegment for SegmentWriter {
	fn write(&self, serializer: SegmentSerializer) -> Result<()> {
		write(&self.per_field_postings_writers,
		      &self.fast_field_writers,
			  &self.fieldnorms_writer,
			  self.segment_info(),
		      serializer)
	}
}
