use super::operation::{AddOperation, UserOperation};
use super::segment_updater::SegmentUpdater;
use super::PreparedCommit;
use crate::common::BitSet;
use crate::core::Index;
use crate::core::Segment;
use crate::core::SegmentComponent;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::core::SegmentReader;
use crate::directory::TerminatingWrite;
use crate::directory::{DirectoryLock, GarbageCollectionResult};
use crate::docset::{DocSet, TERMINATED};
use crate::error::TantivyError;
use crate::fastfield::write_delete_bitset;
use crate::indexer::delete_queue::{DeleteCursor, DeleteQueue};
use crate::indexer::doc_opstamp_mapping::DocToOpstampMapping;
use crate::indexer::operation::DeleteOperation;
use crate::indexer::stamper::Stamper;
use crate::indexer::MergePolicy;
use crate::indexer::SegmentEntry;
use crate::indexer::SegmentWriter;
use crate::schema::Document;
use crate::schema::IndexRecordOption;
use crate::schema::Term;
use crate::Opstamp;
use crossbeam::channel;
use futures::executor::block_on;
use futures::future::Future;
use smallvec::smallvec;
use smallvec::SmallVec;
use std::mem;
use std::ops::Range;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

// Size of the margin for the heap. A segment is closed when the remaining memory
// in the heap goes below MARGIN_IN_BYTES.
pub const MARGIN_IN_BYTES: usize = 1_000_000;

// We impose the memory per thread to be at least 3 MB.
pub const HEAP_SIZE_MIN: usize = ((MARGIN_IN_BYTES as u32) * 3u32) as usize;
pub const HEAP_SIZE_MAX: usize = u32::max_value() as usize - MARGIN_IN_BYTES;

// Add document will block if the number of docs waiting in the queue to be indexed
// reaches `PIPELINE_MAX_SIZE_IN_DOCS`
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

// Group of operations.
// Most of the time, users will send operation one-by-one, but it can be useful to
// send them as a small block to ensure that
// - all docs in the operation will happen on the same segment and continuous docids.
// - all operations in the group are committed at the same time, making the group
// atomic.
type OperationGroup = SmallVec<[AddOperation; 4]>;
type OperationSender = channel::Sender<OperationGroup>;
type OperationReceiver = channel::Receiver<OperationGroup>;

/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independent `Segment`, via
/// a `SegmentWriter` object.
pub struct IndexWriter {
    // the lock is just used to bind the
    // lifetime of the lock with that of the IndexWriter.
    _directory_lock: Option<DirectoryLock>,

    index: Index,

    heap_size_in_bytes_per_thread: usize,

    workers_join_handle: Vec<JoinHandle<crate::Result<()>>>,

    operation_receiver: OperationReceiver,
    operation_sender: OperationSender,

    segment_updater: SegmentUpdater,

    worker_id: usize,

    num_threads: usize,

    delete_queue: DeleteQueue,

    stamper: Stamper,
    committed_opstamp: Opstamp,
}

fn compute_deleted_bitset(
    delete_bitset: &mut BitSet,
    segment_reader: &SegmentReader,
    delete_cursor: &mut DeleteCursor,
    doc_opstamps: &DocToOpstampMapping,
    target_opstamp: Opstamp,
) -> crate::Result<bool> {
    let mut might_have_changed = false;
    while let Some(delete_op) = delete_cursor.get() {
        if delete_op.opstamp > target_opstamp {
            break;
        }

        // A delete operation should only affect
        // document that were inserted after it.
        //
        // Limit doc helps identify the first document
        // that may be affected by the delete operation.
        let limit_doc = doc_opstamps.compute_doc_limit(delete_op.opstamp);
        let inverted_index = segment_reader.inverted_index(delete_op.term.field())?;
        if let Some(mut docset) =
            inverted_index.read_postings(&delete_op.term, IndexRecordOption::Basic)?
        {
            let mut deleted_doc = docset.doc();
            while deleted_doc != TERMINATED {
                if deleted_doc < limit_doc {
                    delete_bitset.insert(deleted_doc);
                    might_have_changed = true;
                }
                deleted_doc = docset.advance();
            }
        }
        delete_cursor.advance();
    }
    Ok(might_have_changed)
}

/// Advance delete for the given segment up to the target opstamp.
///
/// Note that there are no guarantee that the resulting `segment_entry` delete_opstamp
/// is `==` target_opstamp.
/// For instance, there was no delete operation between the state of the `segment_entry` and
/// the `target_opstamp`, `segment_entry` is not updated.
pub(crate) fn advance_deletes(
    mut segment: Segment,
    segment_entry: &mut SegmentEntry,
    target_opstamp: Opstamp,
) -> crate::Result<()> {
    if segment_entry.meta().delete_opstamp() == Some(target_opstamp) {
        // We are already up-to-date here.
        return Ok(());
    }

    if segment_entry.delete_bitset().is_none() && segment_entry.delete_cursor().get().is_none() {
        // There has been no `DeleteOperation` between the segment status and `target_opstamp`.
        return Ok(());
    }

    let segment_reader = SegmentReader::open(&segment)?;

    let max_doc = segment_reader.max_doc();
    let mut delete_bitset: BitSet = match segment_entry.delete_bitset() {
        Some(previous_delete_bitset) => (*previous_delete_bitset).clone(),
        None => BitSet::with_max_value(max_doc),
    };

    let num_deleted_docs_before = segment.meta().num_deleted_docs();

    compute_deleted_bitset(
        &mut delete_bitset,
        &segment_reader,
        segment_entry.delete_cursor(),
        &DocToOpstampMapping::None,
        target_opstamp,
    )?;

    // TODO optimize
    // It should be possible to do something smarter by manipulation bitsets directly
    // to compute this union.
    if let Some(seg_delete_bitset) = segment_reader.delete_bitset() {
        for doc in 0u32..max_doc {
            if seg_delete_bitset.is_deleted(doc) {
                delete_bitset.insert(doc);
            }
        }
    }

    let num_deleted_docs: u32 = delete_bitset.len() as u32;
    if num_deleted_docs > num_deleted_docs_before {
        // There are new deletes. We need to write a new delete file.
        segment = segment.with_delete_meta(num_deleted_docs as u32, target_opstamp);
        let mut delete_file = segment.open_write(SegmentComponent::DELETE)?;
        write_delete_bitset(&delete_bitset, max_doc, &mut delete_file)?;
        delete_file.terminate()?;
    }

    segment_entry.set_meta(segment.meta().clone());
    Ok(())
}

fn index_documents(
    memory_budget: usize,
    segment: Segment,
    grouped_document_iterator: &mut dyn Iterator<Item = OperationGroup>,
    segment_updater: &mut SegmentUpdater,
    mut delete_cursor: DeleteCursor,
) -> crate::Result<bool> {
    let schema = segment.schema();

    let mut segment_writer = SegmentWriter::for_segment(memory_budget, segment.clone(), &schema)?;
    for document_group in grouped_document_iterator {
        for doc in document_group {
            segment_writer.add_document(doc, &schema)?;
        }
        let mem_usage = segment_writer.mem_usage();
        if mem_usage >= memory_budget - MARGIN_IN_BYTES {
            info!(
                "Buffer limit reached, flushing segment with maxdoc={}.",
                segment_writer.max_doc()
            );
            break;
        }
    }

    if !segment_updater.is_alive() {
        return Ok(false);
    }

    let max_doc = segment_writer.max_doc();

    // this is ensured by the call to peek before starting
    // the worker thread.
    assert!(max_doc > 0);

    let doc_opstamps: Vec<Opstamp> = segment_writer.finalize()?;

    let segment_with_max_doc = segment.with_max_doc(max_doc);

    let last_docstamp: Opstamp = *(doc_opstamps.last().unwrap());

    let delete_bitset_opt = apply_deletes(
        &segment_with_max_doc,
        &mut delete_cursor,
        &doc_opstamps,
        last_docstamp,
    )?;

    let segment_entry = SegmentEntry::new(
        segment_with_max_doc.meta().clone(),
        delete_cursor,
        delete_bitset_opt,
    );
    block_on(segment_updater.schedule_add_segment(segment_entry))?;
    Ok(true)
}

fn apply_deletes(
    segment: &Segment,
    mut delete_cursor: &mut DeleteCursor,
    doc_opstamps: &[Opstamp],
    last_docstamp: Opstamp,
) -> crate::Result<Option<BitSet>> {
    if delete_cursor.get().is_none() {
        // if there are no delete operation in the queue, no need
        // to even open the segment.
        return Ok(None);
    }
    let segment_reader = SegmentReader::open(segment)?;
    let doc_to_opstamps = DocToOpstampMapping::from(doc_opstamps);

    let max_doc = segment.meta().max_doc();
    let mut deleted_bitset = BitSet::with_max_value(max_doc);
    let may_have_deletes = compute_deleted_bitset(
        &mut deleted_bitset,
        &segment_reader,
        &mut delete_cursor,
        &doc_to_opstamps,
        last_docstamp,
    )?;
    Ok(if may_have_deletes {
        Some(deleted_bitset)
    } else {
        None
    })
}

impl IndexWriter {
    /// Create a new index writer. Attempts to acquire a lockfile.
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
    pub(crate) fn new(
        index: &Index,
        num_threads: usize,
        heap_size_in_bytes_per_thread: usize,
        directory_lock: DirectoryLock,
    ) -> crate::Result<IndexWriter> {
        if heap_size_in_bytes_per_thread < HEAP_SIZE_MIN {
            let err_msg = format!(
                "The heap size per thread needs to be at least {}.",
                HEAP_SIZE_MIN
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        if heap_size_in_bytes_per_thread >= HEAP_SIZE_MAX {
            let err_msg = format!("The heap size per thread cannot exceed {}", HEAP_SIZE_MAX);
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        let (document_sender, document_receiver): (OperationSender, OperationReceiver) =
            channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);

        let delete_queue = DeleteQueue::new();

        let current_opstamp = index.load_metas()?.opstamp;

        let stamper = Stamper::new(current_opstamp);

        let segment_updater =
            SegmentUpdater::create(index.clone(), stamper.clone(), &delete_queue.cursor())?;

        let mut index_writer = IndexWriter {
            _directory_lock: Some(directory_lock),

            heap_size_in_bytes_per_thread,
            index: index.clone(),

            operation_receiver: document_receiver,
            operation_sender: document_sender,

            segment_updater,

            workers_join_handle: vec![],
            num_threads,

            delete_queue,

            committed_opstamp: current_opstamp,
            stamper,

            worker_id: 0,
        };
        index_writer.start_workers()?;
        Ok(index_writer)
    }

    fn drop_sender(&mut self) {
        let (sender, _receiver) = channel::bounded(1);
        self.operation_sender = sender;
    }

    /// If there are some merging threads, blocks until they all finish their work and
    /// then drop the `IndexWriter`.
    pub fn wait_merging_threads(mut self) -> crate::Result<()> {
        // this will stop the indexing thread,
        // dropping the last reference to the segment_updater.
        self.drop_sender();

        let former_workers_handles = mem::replace(&mut self.workers_join_handle, vec![]);
        for join_handle in former_workers_handles {
            join_handle
                .join()
                .expect("Indexing Worker thread panicked")
                .map_err(|_| {
                    TantivyError::ErrorInThread("Error in indexing worker thread.".into())
                })?;
        }

        let result = self
            .segment_updater
            .wait_merging_thread()
            .map_err(|_| TantivyError::ErrorInThread("Failed to join merging thread.".into()));

        if let Err(ref e) = result {
            error!("Some merging thread failed {:?}", e);
        }

        result
    }

    #[doc(hidden)]
    pub fn add_segment(&self, segment_meta: SegmentMeta) -> crate::Result<()> {
        let delete_cursor = self.delete_queue.cursor();
        let segment_entry = SegmentEntry::new(segment_meta, delete_cursor, None);
        block_on(self.segment_updater.schedule_add_segment(segment_entry))
    }

    /// Creates a new segment.
    ///
    /// This method is useful only for users trying to do complex
    /// operations, like converting an index format to another.
    ///
    /// It is safe to start writing file associated to the new `Segment`.
    /// These will not be garbage collected as long as an instance object of
    /// `SegmentMeta` object associated to the new `Segment` is "alive".
    pub fn new_segment(&self) -> Segment {
        self.index.new_segment()
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    fn add_indexing_worker(&mut self) -> crate::Result<()> {
        let document_receiver_clone = self.operation_receiver.clone();
        let mut segment_updater = self.segment_updater.clone();

        let mut delete_cursor = self.delete_queue.cursor();

        let mem_budget = self.heap_size_in_bytes_per_thread;
        let index = self.index.clone();
        let join_handle: JoinHandle<crate::Result<()>> = thread::Builder::new()
            .name(format!("thrd-tantivy-index{}", self.worker_id))
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
                    if let Some(operations) = document_iterator.peek() {
                        if let Some(first) = operations.first() {
                            delete_cursor.skip_to(first.opstamp);
                        } else {
                            return Ok(());
                        }
                    } else {
                        // No more documents.
                        // Happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        return Ok(());
                    }
                    let segment = index.new_segment();
                    index_documents(
                        mem_budget,
                        segment,
                        &mut document_iterator,
                        &mut segment_updater,
                        delete_cursor.clone(),
                    )?;
                }
            })?;
        self.worker_id += 1;
        self.workers_join_handle.push(join_handle);
        Ok(())
    }

    /// Accessor to the merge policy.
    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.segment_updater.get_merge_policy()
    }

    /// Setter for the merge policy.
    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        self.segment_updater.set_merge_policy(merge_policy);
    }

    fn start_workers(&mut self) -> crate::Result<()> {
        for _ in 0..self.num_threads {
            self.add_indexing_worker()?;
        }
        Ok(())
    }

    /// Detects and removes the files that are not used by the index anymore.
    pub fn garbage_collect_files(
        &self,
    ) -> impl Future<Output = crate::Result<GarbageCollectionResult>> {
        self.segment_updater.schedule_garbage_collect()
    }

    /// Deletes all documents from the index
    ///
    /// Requires `commit`ing
    /// Enables users to rebuild the index,
    /// by clearing and resubmitting necessary documents
    ///
    /// ```rust
    /// use tantivy::collector::TopDocs;
    /// use tantivy::query::QueryParser;
    /// use tantivy::schema::*;
    /// use tantivy::{doc, Index};
    ///
    /// fn main() -> tantivy::Result<()> {
    ///     let mut schema_builder = Schema::builder();
    ///     let title = schema_builder.add_text_field("title", TEXT | STORED);
    ///     let schema = schema_builder.build();
    ///
    ///     let index = Index::create_in_ram(schema.clone());
    ///
    ///     let mut index_writer = index.writer_with_num_threads(1, 50_000_000)?;
    ///     index_writer.add_document(doc!(title => "The modern Promotheus"));
    ///     index_writer.commit()?;
    ///
    ///     let clear_res = index_writer.delete_all_documents().unwrap();
    ///     // have to commit, otherwise deleted terms remain available
    ///     index_writer.commit()?;
    ///
    ///     let searcher = index.reader()?.searcher();
    ///     let query_parser = QueryParser::for_index(&index, vec![title]);
    ///     let query_promo = query_parser.parse_query("Promotheus")?;
    ///     let top_docs_promo = searcher.search(&query_promo, &TopDocs::with_limit(1))?;
    ///
    ///     assert!(top_docs_promo.is_empty());
    ///     Ok(())
    /// }
    /// ```
    pub fn delete_all_documents(&self) -> crate::Result<Opstamp> {
        // Delete segments
        self.segment_updater.remove_all_segments();
        // Return new stamp - reverted stamp
        self.stamper.revert(self.committed_opstamp);
        Ok(self.committed_opstamp)
    }

    /// Merges a given list of segments
    ///
    /// `segment_ids` is required to be non-empty.
    pub fn merge(
        &mut self,
        segment_ids: &[SegmentId],
    ) -> impl Future<Output = crate::Result<SegmentMeta>> {
        let merge_operation = self.segment_updater.make_merge_operation(segment_ids);
        let segment_updater = self.segment_updater.clone();
        async move { segment_updater.start_merge(merge_operation)?.await }
    }

    /// Closes the current document channel send.
    /// and replace all the channels by new ones.
    ///
    /// The current workers will keep on indexing
    /// the pending document and stop
    /// when no documents are remaining.
    ///
    /// Returns the former segment_ready channel.
    #[allow(unused_must_use)]
    fn recreate_document_channel(&mut self) -> OperationReceiver {
        let (document_sender, document_receiver): (OperationSender, OperationReceiver) =
            channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);
        mem::replace(&mut self.operation_sender, document_sender);
        mem::replace(&mut self.operation_receiver, document_receiver)
    }

    /// Rollback to the last commit
    ///
    /// This cancels all of the updates that
    /// happened after the last commit.
    /// After calling rollback, the index is in the same
    /// state as it was after the last commit.
    ///
    /// The opstamp at the last commit is returned.
    pub fn rollback(&mut self) -> crate::Result<Opstamp> {
        info!("Rolling back to opstamp {}", self.committed_opstamp);
        // marks the segment updater as killed. From now on, all
        // segment updates will be ignored.
        self.segment_updater.kill();
        let document_receiver = self.operation_receiver.clone();

        // take the directory lock to create a new index_writer.
        let directory_lock = self
            ._directory_lock
            .take()
            .expect("The IndexWriter does not have any lock. This is a bug, please report.");

        let new_index_writer: IndexWriter = IndexWriter::new(
            &self.index,
            self.num_threads,
            self.heap_size_in_bytes_per_thread,
            directory_lock,
        )?;

        // the current `self` is dropped right away because of this call.
        //
        // This will drop the document queue, and the thread
        // should terminate.
        *self = new_index_writer;

        // Drains the document receiver pipeline :
        // Workers don't need to index the pending documents.
        //
        // This will reach an end as the only document_sender
        // was dropped with the index_writer.
        for _ in document_receiver {}

        Ok(self.committed_opstamp)
    }

    /// Prepares a commit.
    ///
    /// Calling `prepare_commit()` will cut the indexing
    /// queue. All pending documents will be sent to the
    /// indexing workers. They will then terminate, regardless
    /// of the size of their current segment and flush their
    /// work on disk.
    ///
    /// Once a commit is "prepared", you can either
    /// call
    /// * `.commit()`: to accept this commit
    /// * `.abort()`: to cancel this commit.
    ///
    /// In the current implementation, `PreparedCommit` borrows
    /// the `IndexWriter` mutably so we are guaranteed that no new
    /// document can be added as long as it is committed or is
    /// dropped.
    ///
    /// It is also possible to add a payload to the `commit`
    /// using this API.
    /// See [`PreparedCommit::set_payload()`](PreparedCommit.html)
    pub fn prepare_commit(&mut self) -> crate::Result<PreparedCommit> {
        // Here, because we join all of the worker threads,
        // all of the segment update for this commit have been
        // sent.
        //
        // No document belonging to the next commit have been
        // pushed too, because add_document can only happen
        // on this thread.
        //
        // This will move uncommitted segments to the state of
        // committed segments.
        info!("Preparing commit");

        // this will drop the current document channel
        // and recreate a new one.
        self.recreate_document_channel();

        let former_workers_join_handle = mem::replace(&mut self.workers_join_handle, Vec::new());

        for worker_handle in former_workers_join_handle {
            let indexing_worker_result = worker_handle
                .join()
                .map_err(|e| TantivyError::ErrorInThread(format!("{:?}", e)))?;
            indexing_worker_result?;
            self.add_indexing_worker()?;
        }

        let commit_opstamp = self.stamper.stamp();
        let prepared_commit = PreparedCommit::new(self, commit_opstamp);
        info!("Prepared commit {}", commit_opstamp);
        Ok(prepared_commit)
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
    pub fn commit(&mut self) -> crate::Result<Opstamp> {
        self.prepare_commit()?.commit()
    }

    pub(crate) fn segment_updater(&self) -> &SegmentUpdater {
        &self.segment_updater
    }

    /// Delete all documents containing a given term.
    ///
    /// Delete operation only affects documents that
    /// were added in previous commits, and documents
    /// that were added previously in the same commit.
    ///
    /// Like adds, the deletion itself will be visible
    /// only after calling `commit()`.
    pub fn delete_term(&self, term: Term) -> Opstamp {
        let opstamp = self.stamper.stamp();
        let delete_operation = DeleteOperation { opstamp, term };
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
    pub fn commit_opstamp(&self) -> Opstamp {
        self.committed_opstamp
    }

    /// Adds a document.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// The opstamp is an increasing `u64` that can
    /// be used by the client to align commits with its own
    /// document queue.
    pub fn add_document(&self, document: Document) -> Opstamp {
        let opstamp = self.stamper.stamp();
        let add_operation = AddOperation { opstamp, document };
        let send_result = self.operation_sender.send(smallvec![add_operation]);
        if let Err(e) = send_result {
            panic!("Failed to index document. Sending to indexing channel failed. This probably means all of the indexing threads have panicked. {:?}", e);
        }
        opstamp
    }

    /// Gets a range of stamps from the stamper and "pops" the last stamp
    /// from the range returning a tuple of the last optstamp and the popped
    /// range.
    ///
    /// The total number of stamps generated by this method is `count + 1`;
    /// each operation gets a stamp from the `stamps` iterator and `last_opstamp`
    /// is for the batch itself.
    fn get_batch_opstamps(&self, count: Opstamp) -> (Opstamp, Range<Opstamp>) {
        let Range { start, end } = self.stamper.stamps(count + 1u64);
        let last_opstamp = end - 1;
        let stamps = Range {
            start,
            end: last_opstamp,
        };
        (last_opstamp, stamps)
    }

    /// Runs a group of document operations ensuring that the operations are
    /// assigned contigous u64 opstamps and that add operations of the same
    /// group are flushed into the same segment.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// Each operation of the given `user_operations` will receive an in-order,
    /// contiguous u64 opstamp. The entire batch itself is also given an
    /// opstamp that is 1 greater than the last given operation. This
    /// `batch_opstamp` is the return value of `run`. An empty group of
    /// `user_operations`, an empty `Vec<UserOperation>`, still receives
    /// a valid opstamp even though no changes were _actually_ made to the index.
    ///
    /// Like adds and deletes (see `IndexWriter.add_document` and
    /// `IndexWriter.delete_term`), the changes made by calling `run` will be
    /// visible to readers only after calling `commit()`.
    pub fn run(&self, user_operations: Vec<UserOperation>) -> Opstamp {
        let count = user_operations.len() as u64;
        if count == 0 {
            return self.stamper.stamp();
        }
        let (batch_opstamp, stamps) = self.get_batch_opstamps(count);

        let mut adds = OperationGroup::default();

        for (user_op, opstamp) in user_operations.into_iter().zip(stamps) {
            match user_op {
                UserOperation::Delete(term) => {
                    let delete_operation = DeleteOperation { opstamp, term };
                    self.delete_queue.push(delete_operation);
                }
                UserOperation::Add(document) => {
                    let add_operation = AddOperation { opstamp, document };
                    adds.push(add_operation);
                }
            }
        }
        let send_result = self.operation_sender.send(adds);
        if let Err(e) = send_result {
            panic!("Failed to index document. Sending to indexing channel failed. This probably means all of the indexing threads have panicked. {:?}", e);
        };

        batch_opstamp
    }
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        self.segment_updater.kill();
        self.drop_sender();
        for work in self.workers_join_handle.drain(..) {
            let _ = work.join();
        }
    }
}

#[cfg(test)]
mod tests {

    use super::super::operation::UserOperation;
    use crate::collector::TopDocs;
    use crate::directory::error::LockError;
    use crate::error::*;
    use crate::indexer::NoMergePolicy;
    use crate::query::TermQuery;
    use crate::schema::{self, IndexRecordOption, STRING};
    use crate::Index;
    use crate::ReloadPolicy;
    use crate::Term;

    #[test]
    fn test_operations_group() {
        // an operations group with 2 items should cause 3 opstamps 0, 1, and 2.
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer = index.writer_for_tests().unwrap();
        let operations = vec![
            UserOperation::Add(doc!(text_field=>"a")),
            UserOperation::Add(doc!(text_field=>"b")),
        ];
        let batch_opstamp1 = index_writer.run(operations);
        assert_eq!(batch_opstamp1, 2u64);
    }

    #[test]
    fn test_no_need_to_rewrite_delete_file_if_no_new_deletes() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(text_field => "hello1"));
        index_writer.add_document(doc!(text_field => "hello2"));
        assert!(index_writer.commit().is_ok());

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(searcher.segment_reader(0u32).num_deleted_docs(), 0);

        index_writer.delete_term(Term::from_field_text(text_field, "hello1"));
        assert!(index_writer.commit().is_ok());

        assert!(reader.reload().is_ok());
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(searcher.segment_reader(0u32).num_deleted_docs(), 1);

        let previous_delete_opstamp = index.load_metas().unwrap().segments[0].delete_opstamp();

        // All docs containing hello1 have been already removed.
        // We should not update the delete meta.
        index_writer.delete_term(Term::from_field_text(text_field, "hello1"));
        assert!(index_writer.commit().is_ok());

        assert!(reader.reload().is_ok());
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(searcher.segment_reader(0u32).num_deleted_docs(), 1);

        let after_delete_opstamp = index.load_metas().unwrap().segments[0].delete_opstamp();
        assert_eq!(after_delete_opstamp, previous_delete_opstamp);
    }

    #[test]
    fn test_ordered_batched_operations() {
        // * one delete for `doc!(field=>"a")`
        // * one add for `doc!(field=>"a")`
        // * one add for `doc!(field=>"b")`
        // * one delete for `doc!(field=>"b")`
        // after commit there is one doc with "a" and 0 doc with "b"
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let mut index_writer = index.writer_for_tests().unwrap();
        let a_term = Term::from_field_text(text_field, "a");
        let b_term = Term::from_field_text(text_field, "b");
        let operations = vec![
            UserOperation::Delete(a_term),
            UserOperation::Add(doc!(text_field=>"a")),
            UserOperation::Add(doc!(text_field=>"b")),
            UserOperation::Delete(b_term),
        ];

        index_writer.run(operations);
        index_writer.commit().expect("failed to commit");
        reader.reload().expect("failed to load searchers");

        let a_term = Term::from_field_text(text_field, "a");
        let b_term = Term::from_field_text(text_field, "b");

        let a_query = TermQuery::new(a_term, IndexRecordOption::Basic);
        let b_query = TermQuery::new(b_term, IndexRecordOption::Basic);

        let searcher = reader.searcher();

        let a_docs = searcher
            .search(&a_query, &TopDocs::with_limit(1))
            .expect("search for a failed");

        let b_docs = searcher
            .search(&b_query, &TopDocs::with_limit(1))
            .expect("search for b failed");

        assert_eq!(a_docs.len(), 1);
        assert_eq!(b_docs.len(), 0);
    }

    #[test]
    fn test_empty_operations_group() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer = index.writer(3_000_000).unwrap();
        let operations1 = vec![];
        let batch_opstamp1 = index_writer.run(operations1);
        assert_eq!(batch_opstamp1, 0u64);
        let operations2 = vec![];
        let batch_opstamp2 = index_writer.run(operations2);
        assert_eq!(batch_opstamp2, 1u64);
    }

    #[test]
    fn test_lockfile_stops_duplicates() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let _index_writer = index.writer(3_000_000).unwrap();
        match index.writer(3_000_000) {
            Err(TantivyError::LockFailure(LockError::LockBusy, _)) => {}
            _ => panic!("Expected a `LockFailure` error"),
        }
    }

    #[test]
    fn test_lockfile_already_exists_error_msg() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let _index_writer = index.writer_for_tests().unwrap();
        match index.writer_for_tests() {
            Err(err) => {
                let err_msg = err.to_string();
                assert!(err_msg.contains("already an `IndexWriter`"));
            }
            _ => panic!("Expected LockfileAlreadyExists error"),
        }
    }

    #[test]
    fn test_set_merge_policy() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer = index.writer(3_000_000).unwrap();
        assert_eq!(
            format!("{:?}", index_writer.get_merge_policy()),
            "LogMergePolicy { min_merge_size: 8, max_merge_size: 10000000, min_layer_size: 10000, \
             level_log_size: 0.75 }"
        );
        let merge_policy = Box::new(NoMergePolicy::default());
        index_writer.set_merge_policy(merge_policy);
        assert_eq!(
            format!("{:?}", index_writer.get_merge_policy()),
            "NoMergePolicy"
        );
    }

    #[test]
    fn test_lockfile_released_on_drop() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        {
            let _index_writer = index.writer(3_000_000).unwrap();
            // the lock should be released when the
            // index_writer leaves the scope.
        }
        let _index_writer_two = index.writer(3_000_000).unwrap();
    }

    #[test]
    fn test_commit_and_rollback() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let num_docs_containing = |s: &str| {
            let searcher = reader.searcher();
            let term = Term::from_field_text(text_field, s);
            searcher.doc_freq(&term).unwrap()
        };

        {
            // writing the segment
            let mut index_writer = index.writer(3_000_000).unwrap();
            index_writer.add_document(doc!(text_field=>"a"));
            index_writer.rollback().unwrap();
            assert_eq!(index_writer.commit_opstamp(), 0u64);
            assert_eq!(num_docs_containing("a"), 0);
            {
                index_writer.add_document(doc!(text_field=>"b"));
                index_writer.add_document(doc!(text_field=>"c"));
            }
            assert!(index_writer.commit().is_ok());
            reader.reload().unwrap();
            assert_eq!(num_docs_containing("a"), 0);
            assert_eq!(num_docs_containing("b"), 1);
            assert_eq!(num_docs_containing("c"), 1);
        }
        reader.reload().unwrap();
        reader.searcher();
    }

    #[test]
    fn test_with_merges() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        {
            // writing the segment
            let mut index_writer = index.writer(12_000_000).unwrap();
            // create 8 segments with 100 tiny docs
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field=>"a"));
            }
            index_writer.commit().expect("commit failed");
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field=>"a"));
            }
            //  this should create 8 segments and trigger a merge.
            index_writer.commit().expect("commit failed");
            index_writer
                .wait_merging_threads()
                .expect("waiting merging thread failed");

            reader.reload().unwrap();

            assert_eq!(num_docs_containing("a"), 200);
            assert!(index.searchable_segments().unwrap().len() < 8);
        }
    }

    #[test]
    fn test_prepare_with_commit_message() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        {
            // writing the segment
            let mut index_writer = index.writer(12_000_000).unwrap();
            // create 8 segments with 100 tiny docs
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "a"));
            }
            {
                let mut prepared_commit = index_writer.prepare_commit().expect("commit failed");
                prepared_commit.set_payload("first commit");
                prepared_commit.commit().expect("commit failed");
            }
            {
                let metas = index.load_metas().unwrap();
                assert_eq!(metas.payload.unwrap(), "first commit");
            }
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "a"));
            }
            index_writer.commit().unwrap();
            {
                let metas = index.load_metas().unwrap();
                assert!(metas.payload.is_none());
            }
        }
    }

    #[test]
    fn test_prepare_but_rollback() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();
            // create 8 segments with 100 tiny docs
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "a"));
            }
            {
                let mut prepared_commit = index_writer.prepare_commit().expect("commit failed");
                prepared_commit.set_payload("first commit");
                prepared_commit.abort().expect("commit failed");
            }
            {
                let metas = index.load_metas().unwrap();
                assert!(metas.payload.is_none());
            }
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "b"));
            }
            index_writer.commit().unwrap();
        }
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .unwrap()
                .searcher()
                .doc_freq(&term_a)
                .unwrap()
        };
        assert_eq!(num_docs_containing("a"), 0);
        assert_eq!(num_docs_containing("b"), 100);
    }

    #[test]
    fn test_add_then_delete_all_documents() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let num_docs_containing = |s: &str| {
            reader.reload().unwrap();
            let searcher = reader.searcher();
            let term = Term::from_field_text(text_field, s);
            searcher.doc_freq(&term).unwrap()
        };
        let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();

        let add_tstamp = index_writer.add_document(doc!(text_field => "a"));
        let commit_tstamp = index_writer.commit().unwrap();
        assert!(commit_tstamp > add_tstamp);
        index_writer.delete_all_documents().unwrap();
        index_writer.commit().unwrap();

        // Search for documents with the same term that we added
        assert_eq!(num_docs_containing("a"), 0);
    }

    #[test]
    fn test_delete_all_documents_rollback_correct_stamp() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();

        let add_tstamp = index_writer.add_document(doc!(text_field => "a"));

        // commit documents - they are now available
        let first_commit = index_writer.commit();
        assert!(first_commit.is_ok());
        let first_commit_tstamp = first_commit.unwrap();
        assert!(first_commit_tstamp > add_tstamp);

        // delete_all_documents the index
        let clear_tstamp = index_writer.delete_all_documents().unwrap();
        assert_eq!(clear_tstamp, add_tstamp);

        // commit the clear command - now documents aren't available
        let second_commit = index_writer.commit();
        assert!(second_commit.is_ok());
        let second_commit_tstamp = second_commit.unwrap();

        // add new documents again
        for _ in 0..100 {
            index_writer.add_document(doc!(text_field => "b"));
        }

        // rollback to last commit, when index was empty
        let rollback = index_writer.rollback();
        assert!(rollback.is_ok());
        let rollback_tstamp = rollback.unwrap();
        assert_eq!(rollback_tstamp, second_commit_tstamp);

        // working with an empty index == no documents
        let term_b = Term::from_field_text(text_field, "b");
        assert_eq!(
            index
                .reader()
                .unwrap()
                .searcher()
                .doc_freq(&term_b)
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_delete_all_documents_then_add() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        // writing the segment
        let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();
        let res = index_writer.delete_all_documents();
        assert!(res.is_ok());

        assert!(index_writer.commit().is_ok());
        // add one simple doc
        index_writer.add_document(doc!(text_field => "a"));
        assert!(index_writer.commit().is_ok());

        let term_a = Term::from_field_text(text_field, "a");
        // expect the document with that term to be in the index
        assert_eq!(
            index
                .reader()
                .unwrap()
                .searcher()
                .doc_freq(&term_a)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_delete_all_documents_and_rollback() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();

        // add one simple doc
        index_writer.add_document(doc!(text_field => "a"));
        let comm = index_writer.commit();
        assert!(comm.is_ok());
        let commit_tstamp = comm.unwrap();

        // clear but don't commit!
        let clear_tstamp = index_writer.delete_all_documents().unwrap();
        // clear_tstamp should reset to before the last commit
        assert!(clear_tstamp < commit_tstamp);

        // rollback
        let _rollback_tstamp = index_writer.rollback().unwrap();
        // Find original docs in the index
        let term_a = Term::from_field_text(text_field, "a");
        // expect the document with that term to be in the index
        assert_eq!(
            index
                .reader()
                .unwrap()
                .searcher()
                .doc_freq(&term_a)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_delete_all_documents_empty_index() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();
        let clear = index_writer.delete_all_documents();
        let commit = index_writer.commit();
        assert!(clear.is_ok());
        assert!(commit.is_ok());
    }

    #[test]
    fn test_delete_all_documents_index_twice() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(4, 12_000_000).unwrap();
        let clear = index_writer.delete_all_documents();
        let commit = index_writer.commit();
        assert!(clear.is_ok());
        assert!(commit.is_ok());
        let clear_again = index_writer.delete_all_documents();
        let commit_again = index_writer.commit();
        assert!(clear_again.is_ok());
        assert!(commit_again.is_ok());
    }

    #[test]
    fn test_index_doc_missing_field() {
        let mut schema_builder = schema::Schema::builder();
        let idfield = schema_builder.add_text_field("id", STRING);
        schema_builder.add_text_field("optfield", STRING);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(idfield=>"myid"));
        let commit = index_writer.commit();
        assert!(commit.is_ok());
    }
}
