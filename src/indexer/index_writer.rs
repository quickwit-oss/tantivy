use schema::Schema;
use schema::Document;
use indexer::SegmentSerializer;
use core::SerializableSegment;
use core::Index;
use core::Segment;
use schema::Term;
use std::thread::JoinHandle;
use indexer::{MergePolicy, DefaultMergePolicy};
use indexer::SegmentWriter;
use super::directory_lock::DirectoryLock;
use std::clone::Clone;
use std::io;
use std::thread;
use std::mem;
use indexer::merger::IndexMerger;
use core::SegmentId;
use datastruct::stacker::Heap;
use std::mem::swap;
use std::sync::{Arc, Mutex};
use chan;
use core::SegmentMeta;
use super::delete_queue::{DeleteQueue, DeleteQueueCursor};
use super::segment_updater::{SegmentUpdater, SegmentUpdate, SegmentUpdateSender};
use std::time::Duration;
use super::super::core::index::get_segment_manager;
use super::segment_manager::CommitState;
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



/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independant `Segment`, via
/// a `SegmentWriter` object.
pub struct IndexWriter {
    
    // the lock is just used to bind the 
    // lifetime of the lock with that of the IndexWriter.
    _directory_lock: DirectoryLock, 
    
    _merge_policy: Arc<Mutex<Box<MergePolicy>>>,
    
    index: Index,
    heap_size_in_bytes_per_thread: usize,

    workers_join_handle: Vec<JoinHandle<Result<()>>>,

    document_receiver: DocumentReceiver,
    document_sender: DocumentSender,

    segment_update_sender: SegmentUpdateSender,
    segment_update_thread: JoinHandle<()>,

    worker_id: usize,

    num_threads: usize,

    delete_queue: DeleteQueue,

    uncommitted_docstamp: u64,
    committed_docstamp: u64,
}

// IndexWriter cannot be sent to another thread.
impl !Send for IndexWriter {}
impl !Sync for IndexWriter {}


fn index_documents(heap: &mut Heap,
                   segment: Segment,
                   schema: &Schema,
                   document_iterator: &mut Iterator<Item = Document>,
                   segment_update_sender: &mut SegmentUpdateSender,
                   delete_cursor: DeleteQueueCursor)
                   -> Result<()> {
    heap.clear();
    let segment_id = segment.id();
    let mut segment_writer = try!(SegmentWriter::for_segment(heap, segment, &schema, delete_cursor));
    for doc in document_iterator {
        try!(segment_writer.add_document(&doc, &schema));
        if segment_writer.is_buffer_full() {
            info!("Buffer limit reached, flushing segment with maxdoc={}.",
                  segment_writer.max_doc());
            break;
        }
    }
    let num_docs = segment_writer.max_doc();
    let segment_meta = SegmentMeta {
        segment_id: segment_id,
        num_docs: num_docs,
    };

    try!(segment_writer.finalize());
    segment_update_sender.send(SegmentUpdate::AddSegment(segment_meta));
    Ok(())

}


impl IndexWriter {
    /// The index writer
    pub fn wait_merging_threads(mut self) -> Result<()> {

        self.segment_update_sender.send(SegmentUpdate::Terminate);
        
        // this will stop the indexing thread,
        // dropping the last reference to the segment_update_sender.
        drop(self.document_sender);
        
        let mut v = Vec::new();
        mem::swap(&mut v, &mut self.workers_join_handle);
        for join_handle in v {
            try!(join_handle.join()
                .expect("Indexing Worker thread panicked")
                .map_err(|e| {
                    Error::ErrorInThread(format!("Error in indexing worker thread. {:?}", e))
                }));
        }
        drop(self.workers_join_handle);
        self.segment_update_thread
            .join()
            .map_err(|err| {
                error!("Error in the merging thread {:?}", err);
                Error::ErrorInThread(format!("{:?}", err))
            })
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    ///
    fn add_indexing_worker(&mut self) -> Result<()> {
        let index = self.index.clone();
        let schema = self.index.schema();
        let document_receiver_clone = self.document_receiver.clone();
        let mut segment_update_sender = self.segment_update_sender.clone();
        let mut heap = Heap::with_capacity(self.heap_size_in_bytes_per_thread);
        
        // TODO fix this. the cursor might be too advanced
        // at this point.
        let delete_cursor = self.delete_queue.cursor();
        
        let join_handle: JoinHandle<Result<()>> = try!(thread::Builder::new()
            .name(format!("indexing_thread_{}", self.worker_id))
            .spawn(move || {
                loop {
                    let segment = index.new_segment();

                    let mut document_iterator = document_receiver_clone.clone()
                        .into_iter()
                        .peekable();
                    // the peeking here is to avoid
                    // creating a new segment's files
                    // if no document are available.
                    if document_iterator.peek().is_some() {
                        try!(index_documents(&mut heap,
                                             segment,
                                             &schema,
                                             &mut document_iterator,
                                             &mut segment_update_sender,
                                             delete_cursor.clone()));
                    } else {
                        // No more documents.
                        // Happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        return Ok(());
                    }
                }
            }));
        self.worker_id += 1;
        self.workers_join_handle.push(join_handle);
        Ok(())
    }


    /// Open a new index writer. Attempts to acquire a lockfile.
    ///
    /// The lockfile should be deleted on drop, but it is possible
    /// that due to a panic or other error, a stale lockfile will be
    /// left in the index directory. If you are sure that no other
    /// `IndexWriter` on the system is accessing the index directory,
    /// it is safe to manually delete the lockfile.
    ///
    /// num_threads specifies the number of indexing workers that
    /// should work at the same time.
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// # Panics
    /// If the heap size per thread is too small, panics.
    pub fn open(index: &Index,
                num_threads: usize,
                heap_size_in_bytes_per_thread: usize)
                -> Result<IndexWriter> {

        if heap_size_in_bytes_per_thread <= HEAP_SIZE_LIMIT as usize {
            panic!(format!("The heap size per thread needs to be at least {}.",
                           HEAP_SIZE_LIMIT));
        }
        
        let directory_lock = try!(DirectoryLock::lock(index.directory().box_clone()));
        
        let (document_sender, document_receiver): (DocumentSender, DocumentReceiver) =
            chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
        
        let merge_policy: Arc<Mutex<Box<MergePolicy>>> = Arc::new(Mutex::new(box DefaultMergePolicy::default()));
        
        let (segment_update_sender, segment_update_thread) = SegmentUpdater::start_updater(index.clone(), merge_policy.clone());
        
        let mut index_writer = IndexWriter {
            
            _directory_lock: directory_lock,
            
            _merge_policy: merge_policy,
            
            heap_size_in_bytes_per_thread: heap_size_in_bytes_per_thread,
            index: index.clone(),

            document_receiver: document_receiver,
            document_sender: document_sender,

            segment_update_sender: segment_update_sender,
            segment_update_thread: segment_update_thread,

            workers_join_handle: Vec::new(),
            num_threads: num_threads,

            delete_queue: DeleteQueue::default(),

            committed_docstamp: index.docstamp(),
            uncommitted_docstamp: index.docstamp(),
            worker_id: 0,
        };
        try!(index_writer.start_workers());
        Ok(index_writer)
    }
    
    
    /// Returns a clone of the index_writer merge policy.
    pub fn get_merge_policy(&self) -> Box<MergePolicy> {
        self._merge_policy.lock().unwrap().box_clone() 
    }
    
    /// Set the merge policy.
    pub fn set_merge_policy(&self, merge_policy: Box<MergePolicy>) {
        *self._merge_policy.lock().unwrap() = merge_policy;
    }
    
    fn start_workers(&mut self) -> Result<()> {
        for _ in 0..self.num_threads {
            try!(self.add_indexing_worker());
        }
        Ok(())
    }

    /// Merges a given list of segments
    pub fn merge(&mut self, segments: &[Segment]) -> Result<()> {

        if segments.len() < 2 {
            // no segments or one segment? nothing to do.
            return Ok(());
        }


        let segment_manager = get_segment_manager(&self.index);

        {
            // let's check that all these segments are in the same
            // committed/uncommited state.
            let first_commit_state = segment_manager.is_committed(segments[0].id());

            for segment in segments {
                let commit_state = segment_manager.is_committed(segment.id());
                if commit_state == CommitState::Missing {
                    return Err(Error::InvalidArgument(format!("Segment {:?} is not in the index",
                                                              segments[0].id())));
                }
                if commit_state != first_commit_state {
                    return Err(Error::InvalidArgument(String::from("You may not merge segments \
                                                                    that are heterogenously in \
                                                                    committed and uncommited.")));
                }
            }
        }

        let schema = self.index.schema();

        // An IndexMerger is like a "view" of our merged segments.
        let merger = try!(IndexMerger::open(schema, segments));
        let mut merged_segment = self.index.new_segment();

        // ... we just serialize this index merger in our new segment
        // to merge the two segments.
        let segment_serializer = try!(SegmentSerializer::for_segment(&mut merged_segment));
        let num_docs = try!(merger.write(segment_serializer));
        let merged_segment_ids: Vec<SegmentId> =
            segments.iter().map(|segment| segment.id()).collect();
        let segment_meta = SegmentMeta {
            segment_id: merged_segment.id(),
            num_docs: num_docs,
        };

        segment_manager.end_merge(&merged_segment_ids, &segment_meta);
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
    fn recreate_document_channel(&mut self) -> DocumentReceiver {
        let (mut document_sender, mut document_receiver): (DocumentSender, DocumentReceiver) =
            chan::sync(PIPELINE_MAX_SIZE_IN_DOCS);
        swap(&mut self.document_sender, &mut document_sender);
        swap(&mut self.document_receiver, &mut document_receiver);
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
    pub fn rollback(&mut self) -> Result<u64> {

        self.segment_update_sender.send(SegmentUpdate::CancelGeneration);

        // we cannot drop segment ready receiver yet
        // as it would block the workers.
        let document_receiver = self.recreate_document_channel();

        // Drains the document receiver pipeline :
        // Workers don't need to index the pending documents.
        for _ in document_receiver {}

        let mut former_workers_join_handle = Vec::new();
        swap(&mut former_workers_join_handle,
             &mut self.workers_join_handle);

        // wait for all the worker to finish their work
        // (it should be fast since we consumed all pending documents)
        for worker_handle in former_workers_join_handle {
            // we stop one worker at a time ...
            try!(try!(worker_handle.join()
                .map_err(|e| Error::ErrorInThread(format!("{:?}", e)))));
            // ... and recreate a new one right away
            // to work on the next generation.
            try!(self.add_indexing_worker());
        }

        // All of our indexing workers for the rollbacked generation have
        // been terminated.
        // Our document receiver pipe was drained.
        // No new document have been added in the meanwhile because `IndexWriter`
        // is not shared by different threads.
        //
        // We can now open a new generation and reaccept segments
        // from now on.
        self.segment_update_sender.send(SegmentUpdate::NewGeneration);

        let rollbacked_segments = get_segment_manager(&self.index).rollback();
        for segment_id in rollbacked_segments {

            // TODO all delete must happen after saving
            // meta.json
            self.index.delete_segment(segment_id);
        }
        
        // reset the docstamp
        self.uncommitted_docstamp = self.committed_docstamp;
        Ok(self.committed_docstamp)
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
    pub fn commit(&mut self) -> Result<u64> {

        // this will drop the current document channel
        // and recreate a new one channels.
        self.recreate_document_channel();

        // Docstamp of the last document in this commit.
        self.committed_docstamp = self.uncommitted_docstamp;

        let mut former_workers_join_handle = Vec::new();
        swap(&mut former_workers_join_handle,
             &mut self.workers_join_handle);

        for worker_handle in former_workers_join_handle {
            let indexing_worker_result = try!(worker_handle.join()
                .map_err(|e| Error::ErrorInThread(format!("{:?}", e))));
            try!(indexing_worker_result);
            // add a new worker for the next generation.
            try!(self.add_indexing_worker());
        }
        // here, because we join all of the worker threads,
        // all of the segment update for this commit have been
        // sent.
        //
        // No document belonging to the next generation have been
        // pushed too, because add_document can only happen
        // on this thread.

        // This will move uncommitted segments to the state of
        // committed segments.
        self.segment_update_sender.send(SegmentUpdate::Commit(self.committed_docstamp));

        // wait for the segment update thread to have processed the info
        let segment_manager = get_segment_manager(&self.index);
        while segment_manager.docstamp() != self.committed_docstamp {
            thread::sleep(Duration::from_millis(100));
        }

        Ok(self.committed_docstamp)
    }    

    
    pub fn delete_term(&mut self, term: Term) {
        let opstamp = self.stamp();
        self.delete_queue.push(opstamp, term);
    }

    fn stamp(&mut self) -> u64 {
        let opstamp = self.uncommitted_docstamp;
        self.uncommitted_docstamp += 1u64;
        opstamp
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
        let opstamp = self.stamp();
        self.document_sender.send(doc);
        Ok(opstamp)
    }
}

#[cfg(test)]
mod tests {


    use schema::{self, Document};
    use Index;
    use Term;
    use Error;
    use indexer::NoMergePolicy;

    #[test]
    fn test_lockfile_stops_duplicates() {
        let schema_builder = schema::SchemaBuilder::default();
        let index = Index::create_in_ram(schema_builder.build());
        let _index_writer = index.writer(40_000_000).unwrap();
        match index.writer(40_000_000) {
            Err(Error::FileAlreadyExists(_)) => {}
            _ => panic!("Expected FileAlreadyExists error"),
        }
    }
    
    #[test]
    fn test_set_merge_policy() {
        let schema_builder = schema::SchemaBuilder::default();
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer = index.writer(40_000_000).unwrap();
        assert_eq!(format!("{:?}", index_writer.get_merge_policy()), "LogMergePolicy { min_merge_size: 8, min_layer_size: 10000, level_log_size: 0.75 }");
        let merge_policy = box NoMergePolicy::default();
        index_writer.set_merge_policy(merge_policy);
        assert_eq!(format!("{:?}", index_writer.get_merge_policy()), "NoMergePolicy");
    }

    #[test]
    fn test_lockfile_released_on_drop() {
        let schema_builder = schema::SchemaBuilder::default();
        let index = Index::create_in_ram(schema_builder.build());
        {
            let _index_writer = index.writer(40_000_000).unwrap();
            // the lock should be released when the 
            // index_writer leaves the scope.
        }
        let _index_writer_two = index.writer(40_000_000).unwrap();
    }
    
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

    #[test]
    fn test_with_merges() {
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
            let mut index_writer = index.writer_with_num_threads(4, 4 * 30_000_000).unwrap();
            // create 10 segments with 100 tiny docs
            for _doc in 0..100 {
                let mut doc = Document::default();
                doc.add_text(text_field, "a");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().expect("commit failed");
            for _doc in 0..100 {
                let mut doc = Document::default();
                doc.add_text(text_field, "a");
                index_writer.add_document(doc).unwrap();
            }
            // this should create 8 segments and trigger a merge.
            index_writer.commit().expect("commit failed");
            index_writer.wait_merging_threads().expect("waiting merging thread failed");
            assert_eq!(num_docs_containing("a"), 200);
            assert_eq!(index.searchable_segments().len(), 1);
        }
    }


}
