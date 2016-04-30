use DocId;
use core::schema::*;
use core::codec::*;
use core::index::Index;
use core::analyzer::SimpleTokenizer;
use core::index::SerializableSegment;
use core::analyzer::StreamingIterator;
use core::index::Segment;
use core::index::SegmentInfo;
use postings::PostingsWriter;
use core::fastfield::U32FastFieldsWriter;
use std::clone::Clone;
use std::sync::mpsc;
use std::thread;
use std::io::ErrorKind;
use std::io;
use std::sync::Mutex;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;
use std::sync::Arc;
use core::merger::IndexMerger;

pub struct IndexWriter {
	// segment_writers: Vec<SegmentWriter>,
	threads: Vec<JoinHandle<()>>,
	index: Index,
	schema: Schema,
	queue_input: SyncSender<ArcDoc>,
}

type ArcDoc = Arc<Document>;

impl IndexWriter {

	pub fn open(index: &Index, num_threads: usize) -> io::Result<IndexWriter> {
		let schema = index.schema();
		let (queue_input, queue_output): (SyncSender<ArcDoc>, Receiver<ArcDoc>) = mpsc::sync_channel(10_000);
		let queue_output_sendable = Arc::new(Mutex::new(queue_output));
		let threads = (0..num_threads).map(|_|  {
			let queue_output_clone = queue_output_sendable.clone();
			let mut index_clone = index.clone();
			let schema_clone = schema.clone();

			thread::spawn(move || {

				// TODO think about how to handle error within the thread

				let mut docs_remaining = true;
				while docs_remaining {
					let segment = index_clone.new_segment();
					let mut doc;
					{
						match queue_output_clone.lock().unwrap().recv() {
							Ok(doc_) => { doc = doc_ }
							Err(_) => { return; }
						}
					}

					let mut segment_writer = SegmentWriter::for_segment(segment.clone(), &schema_clone).unwrap();
					segment_writer.add_document(&*doc, &schema_clone).unwrap();
					for _ in 0..(225_000 - 1) {
						{
							let queue = queue_output_clone.lock().unwrap();
							match queue.recv() {
								Ok(doc_) => {
									doc = doc_;
								}
								Err(_) => {
									docs_remaining = false;
									break;
								}
							}
						}
						segment_writer.add_document(&*doc, &schema_clone).unwrap();
					}
					segment_writer.finalize().unwrap();
					index_clone.sync(&segment).unwrap();
					index_clone.publish_segment(&segment).unwrap();
				}
			})
		}).collect();
		Ok(IndexWriter {
			threads: threads,
			index: index.clone(),
			schema: schema,
			queue_input: queue_input,
		})
	}

	pub fn merge(&mut self, segments: &Vec<Segment>) -> io::Result<()> {
		let schema = self.schema.clone();
		let merger = try!(IndexMerger::open(schema, segments));
		let merged_segment = self.index.new_segment();
		let segment_serializer = try!(SegmentSerializer::for_segment(&merged_segment));
		try!(merger.write(segment_serializer));
		self.index.sync(&merged_segment).unwrap();
		self.index.publish_merge_segment(segments, &merged_segment)
	}

	pub fn wait(self,) -> thread::Result<()> {
		drop(self.queue_input);
		for thread in self.threads {
			try!(thread.join());
		}
		Ok(())
	}

    pub fn add_document(&mut self, doc: Document) -> io::Result<()> {
        let arc_doc = ArcDoc::new(doc);
		try!(
			self.queue_input.send(arc_doc)
				.map_err(|e| io::Error::new(ErrorKind::Other, e))
		);
		Ok(())
    }


}


pub struct SegmentWriter {
    max_doc: DocId,
	tokenizer: SimpleTokenizer,
	postings_writer: PostingsWriter,
	segment_serializer: SegmentSerializer,
	fast_field_writers: U32FastFieldsWriter,
}

impl SegmentWriter {

	fn for_segment(segment: Segment, schema: &Schema) -> io::Result<SegmentWriter> {
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
	// The segment cannot be used after this.
	fn finalize(	self,) -> io::Result<()> {
		let segment_info = self.segment_info();
		write(&self.postings_writer,
			  &self.fast_field_writers,
			  segment_info,
			  self.segment_serializer)
	}

    pub fn add_document(&mut self, doc: &Document, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        for field_value in doc.text_fields() {
			let field_options = schema.text_field_options(&field_value.field);
			if field_options.is_tokenized_indexed() {
				let mut tokens = self.tokenizer.tokenize(&field_value.text);
				let mut pos = 0u32;
				loop {
					match tokens.next() {
						Some(token) => {
							let term = Term::from_field_text(&field_value.field, token);
							self.postings_writer.suscribe(doc_id, pos.clone(), term);
							pos += 1;
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
                self.postings_writer.suscribe(doc_id, 0.clone(), term);
            }
		}
		self.fast_field_writers.add_document(&doc);
		let stored_fieldvalues: Vec<&TextFieldValue> = doc
			.text_fields()
			.filter(|text_field_value| schema.text_field_options(&text_field_value.field).is_stored())
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
