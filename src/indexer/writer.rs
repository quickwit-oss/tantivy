use schema::Schema;
use schema::Document;
use indexer::segment_serializer::SegmentSerializer;
use core::index::Index;
use core::index::SerializableSegment;
use core::index::Segment;
use std::clone::Clone;
use std::io;
use indexer::merger::IndexMerger;
use indexer::IndexWorker;
use chan;

use Result;
use Error;


pub struct IndexWriter {
	index_workers: Vec<IndexWorker>,
	index: Index,
	schema: Schema,
	document_sender: chan::Sender<Document>,
}

const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

impl IndexWriter {

	pub fn open(index: &Index, num_threads: usize) -> Result<IndexWriter> {
		let schema = index.schema();
		let (document_sender, document_receiver): (chan::Sender<Document>, chan::Receiver<Document>) = chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
		let mut index_workers = Vec::new();
		for _ in 0 .. num_threads {
			index_workers.push(IndexWorker::spawn(index.clone(), document_receiver.clone()));
		}
		// TODO err in thread?
		Ok(IndexWriter {
			index_workers: index_workers,
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
				.wait()
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

