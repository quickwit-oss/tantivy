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
use std::thread;
use indexer::merger::IndexMerger;
use core::SegmentId;
use datastruct::stacker::Heap;
use std::mem::swap;
use chan;
use core::SegmentMeta;
use indexer::SegmentAppender;
use super::super::core::index::get_segment_manager;
use Result;
use Error;

// Size of the margin for the heap. A segment is closed when the remaining memory
// in the heap goes below MARGIN_IN_BYTES.
pub const MARGIN_IN_BYTES: u32 = 10_000_000u32;

// We impose the memory per thread to be at least 30 MB.
pub const HEAP_SIZE_LIMIT: u32 = MARGIN_IN_BYTES * 3u32;

// Add document will block if the number of docs waiting in the queue to be indexed reaches PIPELINE_MAX_SIZE_IN_DOCS
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;


type DocumentSender = chan::Sender<Document>;
type DocumentReceiver = chan::Receiver<Document>;

type NewSegmentSender = chan::Sender<Result<SegmentMeta>>;
type NewSegmentReceiver = chan::Receiver<Result<SegmentMeta>>;

/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independant `Segment`, via
/// a `SegmentWriter` object.
pub struct IndexWriter {
	index: Index,
	heap_size_in_bytes_per_thread: usize,
	workers_join_handle: Vec<JoinHandle<Result<()>>>,
	segment_appender: SegmentAppender,
	document_receiver: DocumentReceiver,
	document_sender: DocumentSender,
	num_threads: usize,
	docstamp: u64,
}


fn index_documents(heap: &mut Heap,
				   segment: Segment,
				   schema: &Schema,
				   document_iterator: &mut Iterator<Item=Document>,
				   segment_appender: &mut SegmentAppender) -> Result<bool> {
	heap.clear();
	let segment_id = segment.id();
	let mut segment_writer = try!(SegmentWriter::for_segment(heap, segment, &schema));
	for doc in document_iterator {
		try!(segment_writer.add_document(&doc, &schema));
		if segment_writer.is_buffer_full() {
			info!("Buffer limit reached, flushing segment with maxdoc={}.", segment_writer.max_doc());
			break;
		}
	}
	let num_docs = segment_writer.max_doc() as usize;
	let segment_meta = SegmentMeta {
		segment_id: segment_id,
		num_docs: num_docs,
	};

	try!(segment_writer.finalize());
	segment_appender.add_segment(segment_meta)
}


impl IndexWriter {

	/// Spawns a new worker thread for indexing.
	/// The thread consumes documents from the pipeline.
	///
	fn add_indexing_worker(&mut self,) -> Result<()> {
		let index = self.index.clone();
		let schema = self.index.schema();
		let mut segment_appender_clone = self.segment_appender.clone();
		let document_receiver_clone = self.document_receiver.clone();
		
		let mut heap = Heap::with_capacity(self.heap_size_in_bytes_per_thread); 
		let join_handle: JoinHandle<Result<()>> = thread::spawn(move || {
			loop {
				let segment = index.new_segment();
				let mut document_iterator = document_receiver_clone
					.clone()
					.into_iter()
					.peekable();
				// the peeking here is to avoid
				// creating a new segment's files 
				// if no document are available.
				if document_iterator.peek().is_some() {
					if !try!(
						index_documents(&mut heap,
						     			segment,
										&schema,
										&mut document_iterator,
										&mut segment_appender_clone)
					) {
						return Ok(());
					}
				}
				else {
					// No more documents.
					// Happens when there is a commit, or if the `IndexWriter`
					// was dropped.
					return Ok(());
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
		let (document_sender, document_receiver): (DocumentSender, DocumentReceiver) = chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
		let segment_manager = get_segment_manager(index);
		let segment_appender = SegmentAppender::for_manager(segment_manager);

		let mut index_writer = IndexWriter {
			heap_size_in_bytes_per_thread: heap_size_in_bytes_per_thread,
			index: index.clone(),
			segment_appender: segment_appender,
			document_receiver: document_receiver,
			document_sender: document_sender,
			workers_join_handle: Vec::new(),
			num_threads: num_threads,
			docstamp: index.docstamp(),
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
		//  TODO fix commit or uncommited?
		let schema = self.index.schema();
		// An IndexMerger is like a "view" of our merged segments. 
		let merger = try!(IndexMerger::open(schema, segments));
		let mut merged_segment = self.index.new_segment();
		// ... we just serialize this index merger in our new segment
		// to merge the two segments.
		let segment_serializer = try!(SegmentSerializer::for_segment(&mut merged_segment));
		let num_docs = try!(merger.write(segment_serializer));
		let merged_segment_ids: Vec<SegmentId> = segments.iter().map(|segment| segment.id()).collect();
		let segment_meta = SegmentMeta {
			segment_id: merged_segment.id(),
			num_docs: num_docs,
		};
		let segment_manager = get_segment_manager(&self.index);
		try!(segment_manager.end_merge(&merged_segment_ids, &segment_meta));
		try!(self.index.load_searchers());
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
	fn recreate_channel(&mut self,) -> DocumentReceiver {
		let (mut document_sender, mut document_receiver): (DocumentSender, DocumentReceiver) = chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
		swap(&mut self.document_sender, &mut document_sender);
		swap(&mut self.document_receiver, &mut document_receiver);
		let segment_manager = get_segment_manager(&self.index);
		self.segment_appender = SegmentAppender::for_manager(segment_manager);
		document_receiver
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

		// the current generation is killed...
		// if some pending segment are still in the pipe,
		// they won't be added to our index.
		self.segment_appender.close();

		// we cannot drop segment ready receiver yet
		// as it would block the workers.
		let document_receiver = self.recreate_channel();
		
		// consumes the document receiver pipeline
		// worker don't need to index the pending documents.
		for _ in document_receiver {};
		
		let mut former_workers_join_handle = Vec::new();
		swap(&mut former_workers_join_handle, &mut self.workers_join_handle);
		
		// wait for all the worker to finish their work
		// (it should be fast since we consumed all pending documents)
		for worker_handle in former_workers_join_handle {
			try!(try!(
				worker_handle
					.join()
					.map_err(|e| Error::ErrorInThread(format!("{:?}", e)))
			));
			// add a new worker for the next generation.
			try!(self.add_indexing_worker());
		}

		// reset the docstamp to what it was before
		self.docstamp = self.index.docstamp();
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
		
		// this will drop the current channel
		self.recreate_channel();
		
		// Docstamp of the last document in this commit.
		let commit_docstamp = self.docstamp;

		let mut former_workers_join_handle = Vec::new();
		swap(&mut former_workers_join_handle, &mut self.workers_join_handle);
		
		for worker_handle in former_workers_join_handle {
			let indexing_worker_result = try!(worker_handle
				.join()
				.map_err(|e| Error::ErrorInThread(format!("{:?}", e)))
			);
			try!(indexing_worker_result);
			// add a new worker for the next generation.
			try!(self.add_indexing_worker());
		}

		try!(super::super::core::index::commit(&mut self.index, commit_docstamp));
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