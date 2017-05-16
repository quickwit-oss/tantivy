use bit_set::BitSet;
use chan;
use core::Index;
use core::Segment;
use core::SegmentComponent;
use core::SegmentId;
use core::SegmentMeta;
use core::SegmentReader;
use indexer::stamper::Stamper;
use datastruct::stacker::Heap;
use directory::FileProtection;
use Error;
use Directory;
use fastfield::write_delete_bitset;
use indexer::delete_queue::{DeleteCursor, DeleteQueue};
use futures::Canceled;
use futures::Future;
use indexer::doc_opstamp_mapping::DocToOpstampMapping;
use indexer::MergePolicy;
use indexer::operation::DeleteOperation;
use indexer::SegmentEntry;
use indexer::SegmentWriter;
use postings::DocSet;
use postings::SegmentPostingsOption;
use Result;
use schema::Document;
use schema::Schema;
use schema::Term;
use std::mem;
use std::mem::swap;
use std::thread::JoinHandle;
use super::directory_lock::DirectoryLock;
use super::operation::AddOperation;
use super::segment_updater::SegmentUpdater;
use std::thread;

// Size of the margin for the heap. A segment is closed when the remaining memory
// in the heap goes below `MARGIN_IN_BYTES`.
pub const MARGIN_IN_BYTES: u32 = 10_000_000u32;

// We impose the memory per thread to be at least 30 MB.
pub const HEAP_SIZE_LIMIT: u32 = MARGIN_IN_BYTES * 3u32;

// Add document will block if the number of docs waiting in the queue to be indexed
// reaches `PIPELINE_MAX_SIZE_IN_DOCS`
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

type DocumentSender = chan::Sender<AddOperation>;
type DocumentReceiver = chan::Receiver<AddOperation>;

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

    index: Index,

    heap_size_in_bytes_per_thread: usize,

    workers_join_handle: Vec<JoinHandle<Result<()>>>,

    document_receiver: DocumentReceiver,
    document_sender: DocumentSender,

    segment_updater: SegmentUpdater,

    worker_id: usize,

    num_threads: usize,

    generation: usize,

    delete_queue: DeleteQueue,

    stamper: Stamper,
    committed_opstamp: u64,
}

// IndexWriter cannot be sent to another thread.
impl !Send for IndexWriter {}
impl !Sync for IndexWriter {}



/// Open a new index writer. Attempts to acquire a lockfile.
///
/// The lockfile should be deleted on drop, but it is possible
/// that due to a panic or other error, a stale lockfile will be
/// left in the index directory. If you are sure that no other
/// `IndexWriter` on the system is accessing the index directory,
/// it is safe to manually delete the lockfile.
///
/// `num_threads` specifies the number of indexing workers that
/// should work at the same time.
/// # Errors
/// If the lockfile already exists, returns `Error::FileAlreadyExists`.
/// # Panics
/// If the heap size per thread is too small, panics.
pub fn open_index_writer(index: &Index,
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


    let delete_queue = DeleteQueue::new();

    let current_opstamp = index.opstamp();

    let stamper = Stamper::new(current_opstamp);

    let segment_updater =
        SegmentUpdater::new(index.clone(), stamper.clone(), delete_queue.cursor())?;

    let mut index_writer = IndexWriter {
        _directory_lock: directory_lock,

        heap_size_in_bytes_per_thread: heap_size_in_bytes_per_thread,
        index: index.clone(),

        document_receiver: document_receiver,
        document_sender: document_sender,

        segment_updater: segment_updater,

        workers_join_handle: vec![],
        num_threads: num_threads,

        delete_queue: delete_queue,

        committed_opstamp: current_opstamp,
        stamper: stamper,

        generation: 0,

        worker_id: 0,
    };
    try!(index_writer.start_workers());
    Ok(index_writer)
}



pub fn compute_deleted_bitset(delete_bitset: &mut BitSet,
                              segment_reader: &SegmentReader,
                              delete_cursor: &mut DeleteCursor,
                              doc_opstamps: DocToOpstampMapping,
                              target_opstamp: u64)
                              -> Result<bool> {

    let mut might_have_changed = false;

    loop {
        if let Some(delete_op) = delete_cursor.get() {
            if delete_op.opstamp > target_opstamp {
                break;
            } else {
                // A delete operation should only affect
                // document that were inserted after it.
                //
                // Limit doc helps identify the first document
                // that may be affected by the delete operation.
                let limit_doc = doc_opstamps.compute_doc_limit(delete_op.opstamp);
                if let Some(mut docset) =
                    segment_reader.read_postings(&delete_op.term, SegmentPostingsOption::NoFreq) {
                    while docset.advance() {
                        let deleted_doc = docset.doc();
                        if deleted_doc < limit_doc {
                            delete_bitset.insert(deleted_doc as usize);
                            might_have_changed = true;
                        }
                    }
                }
            }
        } else {
            break;
        }
        delete_cursor.advance();
    }
    Ok(might_have_changed)
}

/// Advance delete for the given segment up
/// to the target opstamp.
pub fn advance_deletes(mut segment: Segment,
                       segment_entry: &mut SegmentEntry,
                       target_opstamp: u64)
                       -> Result<Option<FileProtection>> {

    let mut file_protect: Option<FileProtection> = None;

    {
        if let Some(previous_opstamp) = segment_entry.meta().delete_opstamp() {
            // We are already up-to-date here.
            if target_opstamp == previous_opstamp {
                return Ok(file_protect);
            }
        }
        let segment_reader = SegmentReader::open(segment.clone())?;
        let max_doc = segment_reader.max_doc();

        let mut delete_bitset: BitSet = match segment_entry.delete_bitset() {
            Some(ref previous_delete_bitset) => (*previous_delete_bitset).clone(),
            None => BitSet::with_capacity(max_doc as usize),
        };

        let delete_cursor = segment_entry.delete_cursor();

        compute_deleted_bitset(&mut delete_bitset,
                               &segment_reader,
                               delete_cursor,
                               DocToOpstampMapping::None,
                               target_opstamp)?;

        for doc in 0u32..max_doc {
            if segment_reader.is_deleted(doc) {
                delete_bitset.insert(doc as usize);
            }
        }

        let num_deleted_docs = delete_bitset.len();
        if num_deleted_docs > 0 {
            segment.set_delete_meta(num_deleted_docs as u32, target_opstamp);
            file_protect = Some(segment.protect_from_delete(SegmentComponent::DELETE));
            let mut delete_file = segment.open_write(SegmentComponent::DELETE)?;
            write_delete_bitset(&delete_bitset, &mut delete_file)?;
        }
    }
    segment_entry.set_meta(segment.meta().clone());

    Ok(file_protect)
}

fn index_documents(heap: &mut Heap,
                   segment: Segment,
                   schema: &Schema,
                   generation: usize,
                   document_iterator: &mut Iterator<Item = AddOperation>,
                   segment_updater: &mut SegmentUpdater,
                   mut delete_cursor: DeleteCursor)
                   -> Result<bool> {
    heap.clear();
    let segment_id = segment.id();
    let mut segment_writer = SegmentWriter::for_segment(heap, segment.clone(), &schema)?;
    for doc in document_iterator {
        try!(segment_writer.add_document(&doc, &schema));
        if segment_writer.is_buffer_full() {
            info!("Buffer limit reached, flushing segment with maxdoc={}.",
                  segment_writer.max_doc());
            break;
        }
    }
    let num_docs = segment_writer.max_doc();

    // this is ensured by the call to peek before starting
    // the worker thread.
    assert!(num_docs > 0);

    let doc_opstamps: Vec<u64> = segment_writer.finalize()?;

    let mut segment_meta = SegmentMeta::new(segment_id);
    segment_meta.set_max_doc(num_docs);

    let last_docstamp: u64 = *(doc_opstamps.last().unwrap());

    let doc_to_opstamps = DocToOpstampMapping::from(doc_opstamps);
    let segment_reader = SegmentReader::open(segment)?;
    let mut deleted_bitset = BitSet::with_capacity(num_docs as usize);
    let may_have_deletes = compute_deleted_bitset(&mut deleted_bitset,
                                                  &segment_reader,
                                                  &mut delete_cursor,
                                                  doc_to_opstamps,
                                                  last_docstamp)?;

    let segment_entry = SegmentEntry::new(segment_meta, delete_cursor, {
        if may_have_deletes {
            Some(deleted_bitset)
        } else {
            None
        }
    });

    Ok(segment_updater.add_segment(generation, segment_entry))

}


impl IndexWriter {
    /// The index writer
    pub fn wait_merging_threads(mut self) -> Result<()> {

        // this will stop the indexing thread,
        // dropping the last reference to the segment_updater.
        drop(self.document_sender);


        let former_workers_handles = mem::replace(&mut self.workers_join_handle, vec![]);
        for join_handle in former_workers_handles {
            try!(join_handle
                     .join()
                     .expect("Indexing Worker thread panicked")
                     .map_err(|e| {
                Error::ErrorInThread(format!("Error in indexing worker thread. {:?}", e))
            }));
        }
        drop(self.workers_join_handle);

        let result =
            self.segment_updater
                .wait_merging_thread()
                .map_err(|_| Error::ErrorInThread("Failed to join merging thread.".to_string()));

        if let &Err(ref e) = &result {
            error!("Some merging thread failed {:?}", e);
        }

        result
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    ///
    fn add_indexing_worker(&mut self) -> Result<()> {
        let schema = self.index.schema();
        let document_receiver_clone = self.document_receiver.clone();
        let mut segment_updater = self.segment_updater.clone();
        let mut heap = Heap::with_capacity(self.heap_size_in_bytes_per_thread);

        let generation = self.generation;

        let mut delete_cursor = self.delete_queue.cursor();

        let join_handle: JoinHandle<Result<()>> = thread::Builder::new()
            .name(format!("indexing thread {} for gen {}", self.worker_id, generation))
            .spawn(move || {

                loop {

                    let mut document_iterator =
                        document_receiver_clone.clone().into_iter().peekable();

                    // the peeking here is to avoid
                    // creating a new segment's files
                    // if no document are available.
                    //
                    // this is a valid guarantee as the
                    // peeked document now belongs to
                    // our local iterator.
                    if let Some(operation) = document_iterator.peek() {
                        delete_cursor.skip_to(operation.opstamp);
                    } else {
                        // No more documents.
                        // Happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        return Ok(());
                    }
                    let segment = segment_updater.new_segment();
                    index_documents(&mut heap,
                                    segment,
                                    &schema,
                                    generation,
                                    &mut document_iterator,
                                    &mut segment_updater,
                                    delete_cursor.clone())?;

                }
            })?;
        self.worker_id += 1;
        self.workers_join_handle.push(join_handle);
        Ok(())
    }

    /// Accessor to the merge policy.
    pub fn get_merge_policy(&self) -> Box<MergePolicy> {
        self.segment_updater.get_merge_policy()
    }

    /// Set the merge policy.
    pub fn set_merge_policy(&self, merge_policy: Box<MergePolicy>) {
        self.segment_updater.set_merge_policy(merge_policy);
    }

    fn start_workers(&mut self) -> Result<()> {
        for _ in 0..self.num_threads {
            try!(self.add_indexing_worker());
        }
        Ok(())
    }

    /// Detects and removes the files that
    /// are not used by the index anymore.
    pub fn garbage_collect_files(&mut self) -> Result<()> {
        self.segment_updater.garbage_collect_files()
    }

    /// Merges a given list of segments
    pub fn merge(&mut self,
                 segment_ids: &[SegmentId])
                 -> impl Future<Item = SegmentMeta, Error = Canceled> {
        self.segment_updater.start_merge(segment_ids)
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
        let (mut document_sender, mut document_receiver): (DocumentSender,
                                                           DocumentReceiver) =
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
    /// The opstamp at the last commit is returned.
    pub fn rollback(mut self) -> Result<IndexWriter> {
        info!("Rolling back to opstamp {}", self.committed_opstamp);

        self.segment_updater.kill();

        // Drains the document receiver pipeline :
        // Workers don't need to index the pending documents.
        let receiver_clone = self.document_receiver.clone();
        let index = self.index.clone();
        let num_threads = self.num_threads;
        let heap_size_in_bytes_per_thread = self.heap_size_in_bytes_per_thread;
        drop(self);
        for _ in receiver_clone {}

        let index_writer = open_index_writer(&index, num_threads, heap_size_in_bytes_per_thread)?;

        Ok(index_writer)

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
    /// Commit returns the `opstamp` of the last document
    /// that made it in the commit.
    ///
    pub fn commit(&mut self) -> Result<u64> {

        // here, because we join all of the worker threads,
        // all of the segment update for this commit have been
        // sent.
        //
        // No document belonging to the next generation have been
        // pushed too, because add_document can only happen
        // on this thread.

        // This will move uncommitted segments to the state of
        // committed segments.
        self.committed_opstamp = self.stamper.stamp();
        info!("committing {}", self.committed_opstamp);

        // this will drop the current document channel
        // and recreate a new one channels.
        self.recreate_document_channel();

        let mut former_workers_join_handle = Vec::new();
        swap(&mut former_workers_join_handle,
             &mut self.workers_join_handle);

        for worker_handle in former_workers_join_handle {
            let indexing_worker_result =
                try!(worker_handle
                         .join()
                         .map_err(|e| Error::ErrorInThread(format!("{:?}", e))));
            try!(indexing_worker_result);
            // add a new worker for the next generation.
            try!(self.add_indexing_worker());
        }



        // wait for the segment update thread to have processed the info
        self.segment_updater.commit(self.committed_opstamp)?;

        Ok(self.committed_opstamp)
    }

    /// Delete all documents containing a given term.
    ///
    /// Delete operation only affects documents that
    /// were added in previous commits, and documents
    /// that were added previously in the same commit.
    ///
    /// Like adds, the deletion itself will be visible
    /// only after calling `commit()`.
    pub fn delete_term(&mut self, term: Term) -> u64 {
        let opstamp = self.stamper.stamp();
        let delete_operation = DeleteOperation {
            opstamp: opstamp,
            term: term,
        };
        self.delete_queue.push(delete_operation);
        opstamp
    }

    /// Returns the opstamp of the last successful commit.
    ///
    /// This is, for instance, the opstamp the index will
    /// rollback to if there is a failure like a power surge.
    ///
    /// This is also the opstamp of the commit that is currently
    /// available for searchers.
    pub fn commit_opstamp(&self) -> u64 {
        self.committed_opstamp
    }

    /// Adds a document.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// The opstamp is an increasing `u64` that can
    /// be used by the client to align commits with its own
    /// document queue.
    ///
    /// Currently it represents the number of documents that
    /// have been added since the creation of the index.
    pub fn add_document(&mut self, document: Document) -> u64 {
        let opstamp = self.stamper.stamp();
        let add_operation = AddOperation {
            opstamp: opstamp,
            document: document,
        };
        self.document_sender.send(add_operation);
        opstamp
    }
}




#[cfg(test)]
mod tests {

    use indexer::NoMergePolicy;
    use schema::{self, Document};
    use Index;
    use Term;
    use Error;
    use env_logger;

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
        assert_eq!(format!("{:?}", index_writer.get_merge_policy()),
                   "LogMergePolicy { min_merge_size: 8, min_layer_size: 10000, \
                    level_log_size: 0.75 }");
        let merge_policy = box NoMergePolicy::default();
        index_writer.set_merge_policy(merge_policy);
        assert_eq!(format!("{:?}", index_writer.get_merge_policy()),
                   "NoMergePolicy");
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
                index_writer.add_document(doc);
            }

            index_writer = index_writer.rollback().unwrap();

            assert_eq!(index_writer.commit_opstamp(), 0u64);
            assert_eq!(num_docs_containing("a"), 0);

            {
                let mut doc = Document::default();
                doc.add_text(text_field, "b");
                index_writer.add_document(doc);
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "c");
                index_writer.add_document(doc);
            }
            assert_eq!(index_writer.commit().unwrap(), 2u64);
            index.load_searchers().unwrap();
            assert_eq!(num_docs_containing("a"), 0);
            assert_eq!(num_docs_containing("b"), 1);
            assert_eq!(num_docs_containing("c"), 1);
        }
        index.load_searchers().unwrap();
        index.searcher();
    }


    #[test]
    fn test_with_merges() {
        let _ = env_logger::init();
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
            // create 8 segments with 100 tiny docs
            for _doc in 0..100 {
                let mut doc = Document::default();
                doc.add_text(text_field, "a");
                index_writer.add_document(doc);
            }
            index_writer.commit().expect("commit failed");
            for _doc in 0..100 {
                let mut doc = Document::default();
                doc.add_text(text_field, "a");
                index_writer.add_document(doc);
            }
            // this should create 8 segments and trigger a merge.
            index_writer.commit().expect("commit failed");
            index_writer
                .wait_merging_threads()
                .expect("waiting merging thread failed");
            index.load_searchers().unwrap();

            assert_eq!(num_docs_containing("a"), 200);
            assert!(index.searchable_segments().unwrap().len() < 8);

        }
    }


}
