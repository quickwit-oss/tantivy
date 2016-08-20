use schema::Schema;
use schema::Document;
use indexer::segment_serializer::SegmentSerializer;
use core::index::Index;
use core::index::SerializableSegment;
use core::index::Segment;
use std::clone::Clone;
use std::thread;
use std::io;
use std::thread::JoinHandle;
use indexer::merger::IndexMerger;
use indexer::segment_writer::SegmentWriter;
use chan;

use Result;
use Error;

// struct IndexWorker {
// 	schema: Schema,
// 	document_receiver: chan::Receiver<Document>,
// }

pub struct IndexWriter {
	index_workers: Vec<JoinHandle<()>>,
	index: Index,
	schema: Schema,
	document_sender: chan::Sender<Document>,
}

const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

impl IndexWriter {

	pub fn open(index: &Index, num_threads: usize) -> Result<IndexWriter> {
		let schema = index.schema();
		let (document_sender, document_receiver): (chan::Sender<Document>, chan::Receiver<Document>) = chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
		// let workers = (0 .. num_threads).map(_ {
			
		// });	
		// let index_worker = IndexWorker {
		// 	schema: schema.clone(),
		// 	document_receiver: document_receiver,
		// }
		
				
		let threads = (0..num_threads).map(|_|  {
			
			let document_receiver_clone = document_receiver.clone();
			let mut index_clone = index.clone();
			let schema_clone = schema.clone();
			
			thread::spawn(move || {

				// TODO think about how to handle error within the thread

				let mut docs_remaining = true;
				while docs_remaining {
					
					// the first document is handled separately in order to
					// avoid creating a new segment if there are no documents. 
					
					let mut doc: Document;
					{
						match document_receiver_clone.recv() {
							Some(doc_) => { 
								doc = doc_;
							}
							None => {
								return;
							}
						}
					}
					
					let segment = index_clone.new_segment();
					let mut segment_writer = SegmentWriter::for_segment(segment.clone(), &schema_clone).unwrap();
					segment_writer.add_document(&doc, &schema_clone).unwrap();
					
					for _ in 0..100_000 {
						{
							match document_receiver_clone.recv() {
								Some(doc_) => {
									doc = doc_
								}
								None => {
									docs_remaining = false;
									break;
								}
							}
						}
						segment_writer.add_document(&doc, &schema_clone).unwrap();
					}
					segment_writer.finalize().unwrap();
					index_clone.publish_segment(&segment).unwrap();
				}
			})
		}).collect();
		// TODO err in thread?
		Ok(IndexWriter {
			index_workers: threads,
			index: index.clone(),
			schema: schema,
			document_sender: document_sender,
		})
	}

	pub fn merge(&mut self, segments: &Vec<Segment>) -> Result<()> {
		let schema = self.schema.clone();
		let merger = try!(IndexMerger::open(schema, segments));
		let merged_segment = self.index.new_segment();
		let segment_serializer = try!(SegmentSerializer::for_segment(&merged_segment));
		try!(merger.write(segment_serializer));
		try!(self.index.publish_merge_segment(segments, &merged_segment));
		Ok(())
	}

	pub fn commit(self,) -> Result<()> {
		drop(self.document_sender);
		for worker in self.index_workers {
			try!(worker
				.join()
				.map_err(|e| Error::ErrorInThread(format!("{:?}", e)))
			);
		}
		Ok(())
	}
  
    pub fn add_document(&mut self, doc: Document) -> io::Result<()> {
		self.document_sender.send(doc);
		Ok(())
	}
	

}

