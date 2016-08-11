use schema::Schema;
use schema::Document;
use core::segment_serializer::SegmentSerializer;
use core::index::Index;
use core::index::SerializableSegment;
use core::index::Segment;
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
use core::segment_writer::SegmentWriter;
use Result;
use Error;

pub struct IndexWriter {
	threads: Vec<JoinHandle<()>>,
	index: Index,
	schema: Schema,
	queue_input: SyncSender<ArcDoc>,
}

type ArcDoc = Arc<Document>;

impl IndexWriter {

	pub fn open(index: &Index, num_threads: usize) -> Result<IndexWriter> {
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
					for _ in 0..200_000 {
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
		// TODO err in thread?
		Ok(IndexWriter {
			threads: threads,
			index: index.clone(),
			schema: schema,
			queue_input: queue_input,
		})
	}

	pub fn merge(&mut self, segments: &Vec<Segment>) -> Result<()> {
		let schema = self.schema.clone();
		let merger = try!(IndexMerger::open(schema, segments));
		let merged_segment = self.index.new_segment();
		let segment_serializer = try!(SegmentSerializer::for_segment(&merged_segment));
		try!(merger.write(segment_serializer));
		try!(self.index.sync(&merged_segment));
		try!(self.index.publish_merge_segment(segments, &merged_segment));
		Ok(())
	}

	pub fn wait(self,) -> Result<()> {
		drop(self.queue_input);
		for thread in self.threads {
			try!(thread.join()
					   .map_err(|e| Error::ErrorInThread(format!("{:?}", e)))
			);
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

