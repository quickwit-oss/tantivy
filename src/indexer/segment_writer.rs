use Result;
use DocId;
use std::io;
use schema::Schema;	
use schema::Term;
use core::Segment;
use core::SerializableSegment;
use postings::PostingsWriter;
use fastfield::FastFieldsWriter;
use schema::Field;
use schema::FieldEntry;
use schema::FieldValue;
use schema::FieldType;
use schema::TextIndexingOptions;
use postings::SpecializedPostingsWriter;
use postings::{NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
use indexer::segment_serializer::SegmentSerializer;
use datastruct::stacker::Heap;
use indexer::index_writer::MARGIN_IN_BYTES;
use super::operation::AddOperation;


/// A `SegmentWriter` is in charge of creating segment index from a
/// documents.
///  
/// They creates the postings list in anonymous memory.
/// The segment is layed on disk when the segment gets `finalized`.
pub struct SegmentWriter<'a> {
	heap: &'a Heap,
    max_doc: DocId,
	per_field_postings_writers: Vec<Box<PostingsWriter + 'a>>,
	segment_serializer: SegmentSerializer,
	fast_field_writers: FastFieldsWriter,
	fieldnorms_writer: FastFieldsWriter,
	doc_opstamps: Vec<u64>,
}


fn create_fieldnorms_writer(schema: &Schema) -> FastFieldsWriter {
	let u64_fields: Vec<Field> = schema.fields()
		.iter()
		.enumerate()
		.filter(|&(_, field_entry)| field_entry.is_indexed()) 
		.map(|(field_id, _)| Field(field_id as u32))
		.collect();
	FastFieldsWriter::new(u64_fields)
}


fn posting_from_field_entry<'a>(field_entry: &FieldEntry, heap: &'a Heap) -> Box<PostingsWriter + 'a> {
	match *field_entry.field_type() {
		FieldType::Str(ref text_options) => {
			match text_options.get_indexing_options() {
				TextIndexingOptions::TokenizedWithFreq => {
					SpecializedPostingsWriter::<TermFrequencyRecorder>::new_boxed(heap)
				}
				TextIndexingOptions::TokenizedWithFreqAndPosition => {
					SpecializedPostingsWriter::<TFAndPositionRecorder>::new_boxed(heap)
				}
				_ => {
					SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap)
				}
			}
		} 
		FieldType::U64(_) => {
			SpecializedPostingsWriter::<NothingRecorder>::new_boxed(heap)
		}
		FieldType::I64(_) => {
			// TODO Implement
			panic!("Notimplemented yet");
		}
	}
}


impl<'a> SegmentWriter<'a> {
	
	/// Creates a new `SegmentWriter`
	///
	/// The arguments are defined as follows
	///
	/// - heap: most of the segment writer data (terms, and postings lists recorders)
	/// is stored in a user-defined heap object. This makes it possible for the user to define
	/// the flushing behavior as a buffer limit
	/// - segment: The segment being written  
	/// - schema
	pub fn for_segment(heap: &'a Heap,
					   mut segment: Segment,
					   schema: &Schema) -> Result<SegmentWriter<'a>> {
		let segment_serializer = try!(SegmentSerializer::for_segment(&mut segment));
		let mut per_field_postings_writers: Vec<Box<PostingsWriter + 'a>> = Vec::new();
		for field_entry in schema.fields() {
			let postings_writer: Box<PostingsWriter + 'a> = posting_from_field_entry(field_entry, heap);
			per_field_postings_writers.push(postings_writer);
		}
		Ok(SegmentWriter {
			heap: heap,
			max_doc: 0,
			per_field_postings_writers: per_field_postings_writers,
			fieldnorms_writer: create_fieldnorms_writer(schema),
			segment_serializer: segment_serializer,
			fast_field_writers: FastFieldsWriter::from_schema(schema),
			doc_opstamps: Vec::with_capacity(1_000),
		})
	}
	
	/// Lay on disk the current content of the `SegmentWriter`
	/// 
	/// Finalize consumes the `SegmentWriter`, so that it cannot 
	/// be used afterwards.
	pub fn finalize(mut self) -> Result<Vec<u64>> {
		for per_field_postings_writer in &mut self.per_field_postings_writers {
			per_field_postings_writer.close(self.heap);
		}
		write(&self.per_field_postings_writers,
			  &self.fast_field_writers,
			  &self.fieldnorms_writer,
			  self.segment_serializer,
			  self.heap)?;
		Ok(self.doc_opstamps)
	}
	
	/// Returns true iff the segment writer's buffer has reached capacity.
	///
	/// The limit is defined as `the user defined heap size - an arbitrary margin of 10MB`
	/// The `Segment` is `finalize`d when the buffer gets full.
	///
	/// Because, we cannot cut through a document, the margin is there to ensure that we rarely
	/// exceeds the heap size.  
	pub fn is_buffer_full(&self,) -> bool {
		self.heap.num_free_bytes() <= MARGIN_IN_BYTES
	}
	
	/// Indexes a new document
	///
	/// As a user, you should rather use `IndexWriter`'s add_document.
    pub fn add_document(&mut self, add_operation: &AddOperation, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
		let doc = &add_operation.document;
		self.doc_opstamps.push(add_operation.opstamp);
        for (field, field_values) in doc.get_sorted_field_values() {
			let field_posting_writer: &mut Box<PostingsWriter> = &mut self.per_field_postings_writers[field.0 as usize];
			let field_options = schema.get_field_entry(field);
			match *field_options.field_type() {
				FieldType::Str(ref text_options) => {
					let num_tokens: u32 =
						if text_options.get_indexing_options().is_tokenized() {
							field_posting_writer.index_text(doc_id, field, &field_values, self.heap)
						}
						else {
							let num_field_values = field_values.len() as u32;
							for field_value in field_values {
								let term = Term::from_field_text(field, field_value.value().text());
								field_posting_writer.suscribe(doc_id, 0, &term, self.heap);
							}
							num_field_values
						};
					self.fieldnorms_writer
						.get_field_writer(field)
						.map(|field_norms_writer| {
							field_norms_writer.add_val(num_tokens as u64)
						});
				}
				FieldType::U64(ref int_option) => {
					if int_option.is_indexed() {
						for field_value in field_values {
							let term = Term::from_field_u64(field_value.field(), field_value.value().u64_value());
							field_posting_writer.suscribe(doc_id, 0, &term, self.heap);
						}
					}
				}
				FieldType::I64(ref int_option) => {
					if int_option.is_indexed() {
						for field_value in field_values {
							let term = Term::from_field_i64(field_value.field(), field_value.value().i64_value());
							field_posting_writer.suscribe(doc_id, 0, &term, self.heap);
						}
					}
				}
			}
		}
		self.fieldnorms_writer.fill_val_up_to(doc_id);
		self.fast_field_writers.add_document(&doc);
		let stored_fieldvalues: Vec<&FieldValue> = doc
			.field_values()
			.iter()
			.filter(|field_value| schema.get_field_entry(field_value.field()).is_stored())
			.collect();
		let doc_writer = self.segment_serializer.get_store_writer();
		try!(doc_writer.store(&stored_fieldvalues));
        self.max_doc += 1;
		Ok(())
    }
	
	
	/// Max doc is 
	/// - the number of documents in the segment assuming there is no deletes
	/// - the maximum document id (including deleted documents) + 1
	///
	/// Currently, **tantivy** does not handle deletes anyway,
	/// so `max_doc == num_docs`  
	pub fn max_doc(&self,) -> u32 {
		self.max_doc
	}
	
	/// Number of documents in the index.
	/// Deleted documents are not counted.
	///
	/// Currently, **tantivy** does not handle deletes anyway,
	/// so `max_doc == num_docs`
	#[allow(dead_code)]
	pub fn num_docs(&self,) -> u32 {
		self.max_doc
	}

}

// This method is used as a trick to workaround the borrow checker
fn write<'a>(per_field_postings_writers: &[Box<PostingsWriter + 'a>],
		 fast_field_writers: &FastFieldsWriter,
		 fieldnorms_writer: &FastFieldsWriter,
	  	 mut serializer: SegmentSerializer,
		 heap: &'a Heap,) -> Result<()> {
		for per_field_postings_writer in per_field_postings_writers {
			try!(per_field_postings_writer.serialize(serializer.get_postings_serializer(), heap));
		}
		try!(fast_field_writers.serialize(serializer.get_fast_field_serializer()));
		try!(fieldnorms_writer.serialize(serializer.get_fieldnorms_serializer()));
		try!(serializer.close());
		Ok(())
}

impl<'a> SerializableSegment for SegmentWriter<'a> {
	fn write(&self, serializer: SegmentSerializer) -> Result<u32> {
		let max_doc = self.max_doc;
		write(&self.per_field_postings_writers,
		      &self.fast_field_writers,
			  &self.fieldnorms_writer,
		      serializer,
			  self.heap)?;
		Ok(max_doc)
	}
}
