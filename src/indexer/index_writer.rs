use schema::Schema;
use schema::Document;
use indexer::SegmentSerializer;
use core::Index;
use core::SerializableSegment;
use core::Segment;
use std::thread::JoinHandle;
use indexer::SegmentWriter;
use std::clone::Clone;
use std::io;
use std::path::Path;
use std::thread;
use std::collections::HashSet;
use indexer::merger::IndexMerger;
use core::SegmentId;
use datastruct::stacker::Heap;
use std::mem::swap;
use chan;
use directory::WritePtr;

use Result;
use Error;

// Size of the margin for the heap. A segment is closed when the remaining memory
// in the heap goes below MARGIN_IN_BYTES.
pub const MARGIN_IN_BYTES: u32 = 10_000_000u32;

// We impose the memory per thread to be at least 30 MB.
pub const HEAP_SIZE_LIMIT: u32 = MARGIN_IN_BYTES * 3u32;

// Add document will block if the number of docs waiting in the queue to be indexed reaches PIPELINE_MAX_SIZE_IN_DOCS
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

pub const LOCKFILE_NAME: &'static str = ".tantivy-indexer.lock";


type DocumentSender = chan::Sender<Document>;
type DocumentReceiver = chan::Receiver<Document>;

type NewSegmentSender = chan::Sender<Result<(SegmentId, usize)>>;
type NewSegmentReceiver = chan::Receiver<Result<(SegmentId, usize)>>;

/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independant `Segment`, via
/// a `SegmentWriter` object.
pub struct IndexWriter {
	index: Index,
	heap_size_in_bytes_per_thread: usize,
	workers_join_handle: Vec<JoinHandle<()>>,
	segment_ready_sender: NewSegmentSender,
	segment_ready_receiver: NewSegmentReceiver,
	document_receiver: DocumentReceiver,
	document_sender: DocumentSender,
	num_threads: usize,
	  docstamp: u64,
}


fn index_documents(heap: &mut Heap,
				   segment: Segment,
				   schema: &Schema,
				   document_iterator: &mut Iterator<Item=Document>) -> Result<usize> {
	heap.clear();
	let mut segment_writer = try!(SegmentWriter::for_segment(heap, segment, &schema));
	for doc in document_iterator {
		try!(segment_writer.add_document(&doc, &schema));
		if segment_writer.is_buffer_full() {
			info!("Buffer limit reached, flushing segment with maxdoc={}.", segment_writer.max_doc());
			break;
		}
	}
	let num_docs = segment_writer.max_doc() as usize;
	try!(segment_writer.finalize());
	Ok(num_docs)
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        let lockfile_path = Path::new(LOCKFILE_NAME);
        match self.index.directory_mut().delete(lockfile_path) {
            Ok(_) => (),
            Err(_) => ()
        }
    }
}
impl IndexWriter {

	/// Spawns a new worker thread for indexing.
	/// The thread consumes documents from the pipeline.
	///
	fn add_indexing_worker(&mut self,) -> Result<()> {
		let index = self.index.clone();
		let schema = self.index.schema();
		let segment_ready_sender_clone = self.segment_ready_sender.clone();
		let document_receiver_clone = self.document_receiver.clone();
		
		let mut heap = Heap::with_capacity(self.heap_size_in_bytes_per_thread); 
		let join_handle: JoinHandle<()> = thread::spawn(move || {
			loop {
				let segment = index.new_segment();
				let segment_id = segment.id();
				let mut document_iterator = document_receiver_clone
					.clone()
					.into_iter()
					.peekable();
				// the peeking here is to avoid
				// creating a new segment's files 
				// if no document are available.
				if document_iterator.peek().is_some() {
					let index_result = index_documents(&mut heap, segment, &schema, &mut document_iterator)
						.map(|num_docs| (segment_id, num_docs));
					segment_ready_sender_clone.send(index_result);
				}
				else {
					return;
				}
			}
		});
		self.workers_join_handle.push(join_handle);
		Ok(())
	}
	
	/// Open a new index writer
	/// 
	/// num_threads tells the number of indexing worker that 
	/// should work at the same time.
	pub fn open(index: &Index,
				num_threads: usize,
				heap_size_in_bytes_per_thread: usize) -> Result<IndexWriter> {
		if heap_size_in_bytes_per_thread <= HEAP_SIZE_LIMIT as usize {
			panic!(format!("The heap size per thread needs to be at least {}.", HEAP_SIZE_LIMIT));
		}

      let mut cloned_index = index.clone();
      let lockfile_path = Path::new(LOCKFILE_NAME);

      try!(cloned_index.directory_mut().open_write(lockfile_path));
		let (document_sender, document_receiver): (DocumentSender, DocumentReceiver) = chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
		let (segment_ready_sender, segment_ready_receiver): (NewSegmentSender, NewSegmentReceiver) = chan::async();
		let mut index_writer = IndexWriter {
			heap_size_in_bytes_per_thread: heap_size_in_bytes_per_thread,
			index: cloned_index,
			segment_ready_receiver: segment_ready_receiver,
			segment_ready_sender: segment_ready_sender,
			document_receiver: document_receiver,
			document_sender: document_sender,
			workers_join_handle: Vec::new(),
			num_threads: num_threads,
			docstamp: try!(index.docstamp()),
		};
		try!(index_writer.start_workers());
		Ok(index_writer)
	}

	fn start_workers(&mut self,) -> Result<()> {
		for _ in 0 .. self.num_threads {
			try!(self.add_indexing_worker());
		}
		Ok(())
	}
	
	/// Merges a given list of segments
	pub fn merge(&mut self, segments: &[Segment]) -> Result<()> {
		let schema = self.index.schema();
		let merger = try!(IndexMerger::open(schema, segments));
		let mut merged_segment = self.index.new_segment();
		let segment_serializer = try!(SegmentSerializer::for_segment(&mut merged_segment));
		try!(merger.write(segment_serializer));
		let merged_segment_ids: HashSet<SegmentId> = segments.iter().map(|segment| segment.id()).collect();
		try!(self.index.publish_merge_segment(merged_segment_ids, merged_segment.id()));
		Ok(())
	}

	/// Closes the current document channel send.
	/// and replace all the channels by new ones.
	///
	/// The current workers will keep on indexing
	/// the pending document and stop 
	/// when no documents are remaining.
	///
	/// Returns the former segment_ready channel.  
	fn recreate_channels(&mut self,) -> (DocumentReceiver, chan::Receiver<Result<(SegmentId, usize)>>) {
		let (mut document_sender, mut document_receiver): (DocumentSender, DocumentReceiver) = chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
		let (mut segment_ready_sender, mut segment_ready_receiver): (NewSegmentSender, NewSegmentReceiver) = chan::async();
		swap(&mut self.document_sender, &mut document_sender);
		swap(&mut self.document_receiver, &mut document_receiver);
		swap(&mut self.segment_ready_sender, &mut segment_ready_sender);
		swap(&mut self.segment_ready_receiver, &mut segment_ready_receiver);
		(document_receiver, segment_ready_receiver)
	}


	/// Rollback to the last commit
	///
	/// This cancels all of the update that
	/// happened before after the last commit.
	/// After calling rollback, the index is in the same 
	/// state as it was after the last commit.
	///
	/// The docstamp at the last commit is returned. 
	pub fn rollback(&mut self,) -> Result<u64> {

		// we cannot drop segment ready receiver yet
		// as it would block the workers.
		let (document_receiver, mut _segment_ready_receiver) = self.recreate_channels();

		// consumes the document receiver pipeline
		// worker don't need to index the pending documents.
		for _ in document_receiver {};

		let mut former_workers_join_handle = Vec::new();
		swap(&mut former_workers_join_handle, &mut self.workers_join_handle);
		
		// wait for all the worker to finish their work
		// (it should be fast since we consumed all pending documents)
		for worker_handle in former_workers_join_handle {
			try!(worker_handle
				.join()
				.map_err(|e| Error::ErrorInThread(format!("{:?}", e)))
			);	
			// add a new worker for the next generation.
			try!(self.add_indexing_worker());
		}

		// reset the docstamp to what it was before
		self.docstamp = try!(self.index.docstamp());
		Ok(self.docstamp)
	}


	/// Commits all of the pending changes
	/// 
	/// A call to commit blocks. 
	/// After it returns, all of the document that
	/// were added since the last commit are published 
	/// and persisted.
	///
	/// In case of a crash or an hardware failure (as 
	/// long as the hard disk is spared), it will be possible
	/// to resume indexing from this point.
	///
	/// Commit returns the `docstamp` of the last document
	/// that made it in the commit.
	///
	pub fn commit(&mut self,) -> Result<u64> {
		
		let (document_receiver, segment_ready_receiver) = self.recreate_channels();
		drop(document_receiver);

		// Docstamp of the last document in this commit.
		let commit_docstamp = self.docstamp;

		let mut former_workers_join_handle = Vec::new();
		swap(&mut former_workers_join_handle, &mut self.workers_join_handle);
		
		for worker_handle in former_workers_join_handle {
			try!(worker_handle
				.join()
				.map_err(|e| Error::ErrorInThread(format!("{:?}", e)))
			);
			// add a new worker for the next generation.
			try!(self.add_indexing_worker());
		}
		
		let segment_ids_and_size: Vec<(SegmentId, usize)> = try!(
			segment_ready_receiver
				.into_iter()
				.collect()
		);

		let segment_ids: Vec<SegmentId> = segment_ids_and_size
			.iter()
			.map(|&(segment_id, _num_docs)| segment_id)
			.collect();
		
		try!(self.index.publish_segments(&segment_ids, commit_docstamp));

		Ok(commit_docstamp)
	}
	

	/// Adds a document.
	///
	/// If the indexing pipeline is full, this call may block.
	/// 
	/// The docstamp is an increasing `u64` that can
	/// be used by the client to align commits with its own
	/// document queue.
	/// 
	/// Currently it represents the number of documents that 
	/// have been added since the creation of the index. 
	pub fn add_document(&mut self, doc: Document) -> io::Result<u64> {
		self.document_sender.send(doc);
		self.docstamp += 1;
		Ok(self.docstamp)
	}
	

}



#[cfg(test)]
mod tests {

	use schema::{self, Document};
	use Index;
	use Term;

	#[test]
	fn test_commit_and_rollback() {
		let mut schema_builder = schema::SchemaBuilder::default();
		let text_field = schema_builder.add_text_field("text", schema::TEXT);
		let index = Index::create_in_ram(schema_builder.build());


		let num_docs_containing = |s: &str| {
			let searcher = index.searcher();
			let term_a = Term::from_field_text(text_field, s);
			searcher.doc_freq(&term_a)
		};
		
		{
			// writing the segment
			let mut index_writer = index.writer_with_num_threads(3, 40_000_000).unwrap();
			{
				let mut doc = Document::default();
				doc.add_text(text_field, "a");
				index_writer.add_document(doc).unwrap();
			}
			assert_eq!(index_writer.rollback().unwrap(), 0u64);
			assert_eq!(num_docs_containing("a"), 0);

			{
				let mut doc = Document::default();
				doc.add_text(text_field, "b");
				index_writer.add_document(doc).unwrap();
			}
			{
				let mut doc = Document::default();
				doc.add_text(text_field, "c");
				index_writer.add_document(doc).unwrap();
			}
			assert_eq!(index_writer.commit().unwrap(), 2u64);
			
			assert_eq!(num_docs_containing("a"), 0);
			assert_eq!(num_docs_containing("b"), 1);
			assert_eq!(num_docs_containing("c"), 1);
		}
		index.searcher();
	}

}
