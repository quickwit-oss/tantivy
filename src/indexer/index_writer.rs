use std::ops::Range;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use common::BitSet;
use smallvec::smallvec;

use super::operation::{AddOperation, UserOperation};
use super::segment_updater::SegmentUpdater;
use super::{AddBatch, AddBatchReceiver, AddBatchSender, PreparedCommit};
use crate::core::{Index, Segment, SegmentComponent, SegmentId, SegmentMeta, SegmentReader};
use crate::directory::{DirectoryLock, GarbageCollectionResult, TerminatingWrite};
use crate::error::TantivyError;
use crate::fastfield::write_alive_bitset;
use crate::indexer::delete_queue::{DeleteCursor, DeleteQueue};
use crate::indexer::doc_opstamp_mapping::DocToOpstampMapping;
use crate::indexer::index_writer_status::IndexWriterStatus;
use crate::indexer::operation::DeleteOperation;
use crate::indexer::stamper::Stamper;
use crate::indexer::{MergePolicy, SegmentEntry, SegmentWriter};
use crate::query::{EnableScoring, Query, TermQuery};
use crate::schema::{Document, IndexRecordOption, Term};
use crate::{FutureResult, Opstamp};

// Size of the margin for the `memory_arena`. A segment is closed when the remaining memory
// in the `memory_arena` goes below MARGIN_IN_BYTES.
pub const MARGIN_IN_BYTES: usize = 1_000_000;

// We impose the memory per thread to be at least 3 MB.
pub const MEMORY_ARENA_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 3u32) as usize;
pub const MEMORY_ARENA_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

// We impose the number of index writer threads to be at most this.
pub const MAX_NUM_THREAD: usize = 8;

// Add document will block if the number of docs waiting in the queue to be indexed
// reaches `PIPELINE_MAX_SIZE_IN_DOCS`
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

fn error_in_index_worker_thread(context: &str) -> TantivyError {
    TantivyError::ErrorInThread(format!(
        "{}. A worker thread encountered an error (io::Error most likely) or panicked.",
        context
    ))
}

/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independent [`Segment`], via
/// a `SegmentWriter` object.
pub struct IndexWriter {
    // the lock is just used to bind the
    // lifetime of the lock with that of the IndexWriter.
    _directory_lock: Option<DirectoryLock>,

    index: Index,

    memory_arena_in_bytes_per_thread: usize,

    workers_join_handle: Vec<JoinHandle<crate::Result<()>>>,

    index_writer_status: IndexWriterStatus,
    operation_sender: AddBatchSender,

    segment_updater: SegmentUpdater,

    worker_id: usize,

    num_threads: usize,

    delete_queue: DeleteQueue,

    stamper: Stamper,
    committed_opstamp: Opstamp,
}

fn compute_deleted_bitset(
    alive_bitset: &mut BitSet,
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
        // document that were inserted before it.
        delete_op
            .target
            .for_each_no_score(segment_reader, &mut |doc_matching_delete_query| {
                if doc_opstamps.is_deleted(doc_matching_delete_query, delete_op.opstamp) {
                    alive_bitset.remove(doc_matching_delete_query);
                    might_have_changed = true;
                }
            })?;
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

    if segment_entry.alive_bitset().is_none() && segment_entry.delete_cursor().get().is_none() {
        // There has been no `DeleteOperation` between the segment status and `target_opstamp`.
        return Ok(());
    }

    let segment_reader = SegmentReader::open(&segment)?;

    let max_doc = segment_reader.max_doc();
    let mut alive_bitset: BitSet = match segment_entry.alive_bitset() {
        Some(previous_alive_bitset) => (*previous_alive_bitset).clone(),
        None => BitSet::with_max_value_and_full(max_doc),
    };

    let num_deleted_docs_before = segment.meta().num_deleted_docs();

    compute_deleted_bitset(
        &mut alive_bitset,
        &segment_reader,
        segment_entry.delete_cursor(),
        &DocToOpstampMapping::None,
        target_opstamp,
    )?;

    if let Some(seg_alive_bitset) = segment_reader.alive_bitset() {
        alive_bitset.intersect_update(seg_alive_bitset.bitset());
    }

    let num_alive_docs: u32 = alive_bitset.len() as u32;
    let num_deleted_docs = max_doc - num_alive_docs;
    if num_deleted_docs > num_deleted_docs_before {
        // There are new deletes. We need to write a new delete file.
        segment = segment.with_delete_meta(num_deleted_docs, target_opstamp);
        let mut alive_doc_file = segment.open_write(SegmentComponent::Delete)?;
        write_alive_bitset(&alive_bitset, &mut alive_doc_file)?;
        alive_doc_file.terminate()?;
    }

    segment_entry.set_meta(segment.meta().clone());
    Ok(())
}

fn index_documents(
    memory_budget: usize,
    segment: Segment,
    grouped_document_iterator: &mut dyn Iterator<Item = AddBatch>,
    segment_updater: &mut SegmentUpdater,
    mut delete_cursor: DeleteCursor,
) -> crate::Result<()> {
    let mut segment_writer = SegmentWriter::for_segment(memory_budget, segment.clone())?;
    for document_group in grouped_document_iterator {
        for doc in document_group {
            segment_writer.add_document(doc)?;
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
        return Ok(());
    }

    let max_doc = segment_writer.max_doc();

    // this is ensured by the call to peek before starting
    // the worker thread.
    assert!(max_doc > 0);

    let doc_opstamps: Vec<Opstamp> = segment_writer.finalize()?;

    let segment_with_max_doc = segment.with_max_doc(max_doc);

    let alive_bitset_opt = apply_deletes(&segment_with_max_doc, &mut delete_cursor, &doc_opstamps)?;

    let meta = segment_with_max_doc.meta().clone();
    meta.untrack_temp_docstore();
    // update segment_updater inventory to remove tempstore
    let segment_entry = SegmentEntry::new(meta, delete_cursor, alive_bitset_opt);
    segment_updater.schedule_add_segment(segment_entry).wait()?;
    Ok(())
}

/// `doc_opstamps` is required to be non-empty.
fn apply_deletes(
    segment: &Segment,
    delete_cursor: &mut DeleteCursor,
    doc_opstamps: &[Opstamp],
) -> crate::Result<Option<BitSet>> {
    if delete_cursor.get().is_none() {
        // if there are no delete operation in the queue, no need
        // to even open the segment.
        return Ok(None);
    }

    let max_doc_opstamp: Opstamp = doc_opstamps
        .iter()
        .cloned()
        .max()
        .expect("Empty DocOpstamp is forbidden");

    let segment_reader = SegmentReader::open(segment)?;
    let doc_to_opstamps = DocToOpstampMapping::WithMap(doc_opstamps);

    let max_doc = segment.meta().max_doc();
    let mut deleted_bitset = BitSet::with_max_value_and_full(max_doc);
    let may_have_deletes = compute_deleted_bitset(
        &mut deleted_bitset,
        &segment_reader,
        delete_cursor,
        &doc_to_opstamps,
        max_doc_opstamp,
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
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub(crate) fn new(
        index: &Index,
        num_threads: usize,
        memory_arena_in_bytes_per_thread: usize,
        directory_lock: DirectoryLock,
    ) -> crate::Result<IndexWriter> {
        if memory_arena_in_bytes_per_thread < MEMORY_ARENA_NUM_BYTES_MIN {
            let err_msg = format!(
                "The memory arena in bytes per thread needs to be at least {}.",
                MEMORY_ARENA_NUM_BYTES_MIN
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        if memory_arena_in_bytes_per_thread >= MEMORY_ARENA_NUM_BYTES_MAX {
            let err_msg = format!(
                "The memory arena in bytes per thread cannot exceed {}",
                MEMORY_ARENA_NUM_BYTES_MAX
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        let (document_sender, document_receiver): (AddBatchSender, AddBatchReceiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);

        let delete_queue = DeleteQueue::new();

        let current_opstamp = index.load_metas()?.opstamp;

        let stamper = Stamper::new(current_opstamp);

        let segment_updater =
            SegmentUpdater::create(index.clone(), stamper.clone(), &delete_queue.cursor())?;

        let mut index_writer = IndexWriter {
            _directory_lock: Some(directory_lock),

            memory_arena_in_bytes_per_thread,
            index: index.clone(),
            index_writer_status: IndexWriterStatus::from(document_receiver),
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
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        self.operation_sender = sender;
    }

    /// Accessor to the index.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// If there are some merging threads, blocks until they all finish their work and
    /// then drop the `IndexWriter`.
    pub fn wait_merging_threads(mut self) -> crate::Result<()> {
        // this will stop the indexing thread,
        // dropping the last reference to the segment_updater.
        self.drop_sender();

        let former_workers_handles = std::mem::take(&mut self.workers_join_handle);
        for join_handle in former_workers_handles {
            join_handle
                .join()
                .map_err(|_| error_in_index_worker_thread("Worker thread panicked."))?
                .map_err(|_| error_in_index_worker_thread("Worker thread failed."))?;
        }

        let result = self
            .segment_updater
            .wait_merging_thread()
            .map_err(|_| error_in_index_worker_thread("Failed to join merging thread."));

        if let Err(ref e) = result {
            error!("Some merging thread failed {:?}", e);
        }

        result
    }

    #[doc(hidden)]
    pub fn add_segment(&self, segment_meta: SegmentMeta) -> crate::Result<()> {
        let delete_cursor = self.delete_queue.cursor();
        let segment_entry = SegmentEntry::new(segment_meta, delete_cursor, None);
        self.segment_updater
            .schedule_add_segment(segment_entry)
            .wait()
    }

    /// Creates a new segment.
    ///
    /// This method is useful only for users trying to do complex
    /// operations, like converting an index format to another.
    ///
    /// It is safe to start writing file associated with the new `Segment`.
    /// These will not be garbage collected as long as an instance object of
    /// `SegmentMeta` object associated with the new `Segment` is "alive".
    pub fn new_segment(&self) -> Segment {
        self.index.new_segment()
    }

    fn operation_receiver(&self) -> crate::Result<AddBatchReceiver> {
        self.index_writer_status
            .operation_receiver()
            .ok_or_else(|| {
                crate::TantivyError::ErrorInThread(
                    "The index writer was killed. It can happen if an indexing worker encountered \
                     an Io error for instance."
                        .to_string(),
                )
            })
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    fn add_indexing_worker(&mut self) -> crate::Result<()> {
        let document_receiver_clone = self.operation_receiver()?;
        let index_writer_bomb = self.index_writer_status.create_bomb();

        let mut segment_updater = self.segment_updater.clone();

        let mut delete_cursor = self.delete_queue.cursor();

        let mem_budget = self.memory_arena_in_bytes_per_thread;
        let index = self.index.clone();
        let join_handle: JoinHandle<crate::Result<()>> = thread::Builder::new()
            .name(format!("thrd-tantivy-index{}", self.worker_id))
            .spawn(move || {
                loop {
                    let mut document_iterator = document_receiver_clone
                        .clone()
                        .into_iter()
                        .filter(|batch| !batch.is_empty())
                        .peekable();

                    // The peeking here is to avoid creating a new segment's files
                    // if no document are available.
                    //
                    // This is a valid guarantee as the peeked document now belongs to
                    // our local iterator.
                    if let Some(batch) = document_iterator.peek() {
                        assert!(!batch.is_empty());
                        delete_cursor.skip_to(batch[0].opstamp);
                    } else {
                        // No more documents.
                        // It happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        index_writer_bomb.defuse();
                        return Ok(());
                    }

                    index_documents(
                        mem_budget,
                        index.new_segment(),
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
    pub fn garbage_collect_files(&self) -> FutureResult<GarbageCollectionResult> {
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
    ///     index_writer.add_document(doc!(title => "The modern Promotheus"))?;
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

    /// Merges a given list of segments.
    ///
    /// If all segments are empty no new segment will be created.
    ///
    /// `segment_ids` is required to be non-empty.
    pub fn merge(&mut self, segment_ids: &[SegmentId]) -> FutureResult<Option<SegmentMeta>> {
        let merge_operation = self.segment_updater.make_merge_operation(segment_ids);
        let segment_updater = self.segment_updater.clone();
        segment_updater.start_merge(merge_operation)
    }

    /// Closes the current document channel send.
    /// and replace all the channels by new ones.
    ///
    /// The current workers will keep on indexing
    /// the pending document and stop
    /// when no documents are remaining.
    ///
    /// Returns the former segment_ready channel.
    fn recreate_document_channel(&mut self) {
        let (document_sender, document_receiver): (AddBatchSender, AddBatchReceiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);
        self.operation_sender = document_sender;
        self.index_writer_status = IndexWriterStatus::from(document_receiver);
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
        let document_receiver_res = self.operation_receiver();

        // take the directory lock to create a new index_writer.
        let directory_lock = self
            ._directory_lock
            .take()
            .expect("The IndexWriter does not have any lock. This is a bug, please report.");

        let new_index_writer: IndexWriter = IndexWriter::new(
            &self.index,
            self.num_threads,
            self.memory_arena_in_bytes_per_thread,
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
        if let Ok(document_receiver) = document_receiver_res {
            for _ in document_receiver {}
        }

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
    /// In the current implementation, [`PreparedCommit`] borrows
    /// the [`IndexWriter`] mutably so we are guaranteed that no new
    /// document can be added as long as it is committed or is
    /// dropped.
    ///
    /// It is also possible to add a payload to the `commit`
    /// using this API.
    /// See [`PreparedCommit::set_payload()`].
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

        let former_workers_join_handle = std::mem::take(&mut self.workers_join_handle);

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
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        // For backward compatibility, if Term is invalid for the index, do nothing but return an
        // Opstamp
        self.delete_query(Box::new(query))
            .unwrap_or_else(|_| self.stamper.stamp())
    }

    /// Delete all documents matching a given query.
    /// Returns an `Err` if the query can't be executed.
    ///
    /// Delete operation only affects documents that
    /// were added in previous commits, and documents
    /// that were added previously in the same commit.
    ///
    /// Like adds, the deletion itself will be visible
    /// only after calling `commit()`.
    #[doc(hidden)]
    pub fn delete_query(&self, query: Box<dyn Query>) -> crate::Result<Opstamp> {
        let weight = query.weight(EnableScoring::disabled_from_schema(&self.index.schema()))?;
        let opstamp = self.stamper.stamp();
        let delete_operation = DeleteOperation {
            opstamp,
            target: weight,
        };
        self.delete_queue.push(delete_operation);
        Ok(opstamp)
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
    pub fn add_document(&self, document: Document) -> crate::Result<Opstamp> {
        let opstamp = self.stamper.stamp();
        self.send_add_documents_batch(smallvec![AddOperation { opstamp, document }])?;
        Ok(opstamp)
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
        (last_opstamp, start..last_opstamp)
    }

    /// Runs a group of document operations ensuring that the operations are
    /// assigned contiguous u64 opstamps and that add operations of the same
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
    pub fn run<I>(&self, user_operations: I) -> crate::Result<Opstamp>
    where
        I: IntoIterator<Item = UserOperation>,
        I::IntoIter: ExactSizeIterator,
    {
        let user_operations_it = user_operations.into_iter();
        let count = user_operations_it.len() as u64;
        if count == 0 {
            return Ok(self.stamper.stamp());
        }
        let (batch_opstamp, stamps) = self.get_batch_opstamps(count);

        let mut adds = AddBatch::default();

        for (user_op, opstamp) in user_operations_it.zip(stamps) {
            match user_op {
                UserOperation::Delete(term) => {
                    let query = TermQuery::new(term, IndexRecordOption::Basic);
                    let weight =
                        query.weight(EnableScoring::disabled_from_schema(&self.index.schema()))?;
                    let delete_operation = DeleteOperation {
                        opstamp,
                        target: weight,
                    };
                    self.delete_queue.push(delete_operation);
                }
                UserOperation::Add(document) => {
                    let add_operation = AddOperation { opstamp, document };
                    adds.push(add_operation);
                }
            }
        }
        self.send_add_documents_batch(adds)?;
        Ok(batch_opstamp)
    }

    fn send_add_documents_batch(&self, add_ops: AddBatch) -> crate::Result<()> {
        if self.index_writer_status.is_alive() && self.operation_sender.send(add_ops).is_ok() {
            Ok(())
        } else {
            Err(error_in_index_worker_thread("An index writer was killed."))
        }
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
    use std::collections::{HashMap, HashSet};
    use std::net::Ipv6Addr;

    use fastfield_codecs::MonotonicallyMappableToU128;
    use proptest::prelude::*;
    use proptest::prop_oneof;
    use proptest::strategy::Strategy;

    use super::super::operation::UserOperation;
    use crate::collector::TopDocs;
    use crate::directory::error::LockError;
    use crate::error::*;
    use crate::indexer::NoMergePolicy;
    use crate::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
    use crate::schema::{
        self, Cardinality, Facet, FacetOptions, IndexRecordOption, IpAddrOptions, NumericOptions,
        TextFieldIndexing, TextOptions, FAST, INDEXED, STORED, STRING, TEXT,
    };
    use crate::store::DOCSTORE_CACHE_CAPACITY;
    use crate::{
        DateTime, DocAddress, Index, IndexSettings, IndexSortByField, Order, ReloadPolicy, Term,
    };

    const LOREM: &str = "Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
                         eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
                         minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip \
                         ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
                         voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur \
                         sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt \
                         mollit anim id est laborum.";

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
        let batch_opstamp1 = index_writer.run(operations).unwrap();
        assert_eq!(batch_opstamp1, 2u64);
    }

    #[test]
    fn test_no_need_to_rewrite_delete_file_if_no_new_deletes() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(text_field => "hello1"))
            .unwrap();
        index_writer
            .add_document(doc!(text_field => "hello2"))
            .unwrap();
        assert!(index_writer.commit().is_ok());

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(searcher.segment_reader(0u32).num_docs(), 2);

        index_writer.delete_term(Term::from_field_text(text_field, "hello1"));
        assert!(index_writer.commit().is_ok());

        assert!(reader.reload().is_ok());
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(searcher.segment_reader(0u32).num_docs(), 1);

        let previous_delete_opstamp = index.load_metas().unwrap().segments[0].delete_opstamp();

        // All docs containing hello1 have been already removed.
        // We should not update the delete meta.
        index_writer.delete_term(Term::from_field_text(text_field, "hello1"));
        assert!(index_writer.commit().is_ok());

        assert!(reader.reload().is_ok());
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(searcher.segment_reader(0u32).num_docs(), 1);

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

        index_writer.run(operations).unwrap();
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
        let batch_opstamp1 = index_writer.run(operations1).unwrap();
        assert_eq!(batch_opstamp1, 0u64);
        let operations2 = vec![];
        let batch_opstamp2 = index_writer.run(operations2).unwrap();
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
            "LogMergePolicy { min_num_segments: 8, max_docs_before_merge: 10000000, \
             min_layer_size: 10000, level_log_size: 0.75, del_docs_ratio_before_merge: 1.0 }"
        );
        let merge_policy = Box::<NoMergePolicy>::default();
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
    fn test_commit_and_rollback() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let searcher = reader.searcher();
            let term = Term::from_field_text(text_field, s);
            searcher.doc_freq(&term).unwrap()
        };

        {
            // writing the segment
            let mut index_writer = index.writer(3_000_000)?;
            index_writer.add_document(doc!(text_field=>"a"))?;
            index_writer.rollback()?;
            assert_eq!(index_writer.commit_opstamp(), 0u64);
            assert_eq!(num_docs_containing("a"), 0);
            index_writer.add_document(doc!(text_field=>"b"))?;
            index_writer.add_document(doc!(text_field=>"c"))?;
            index_writer.commit()?;
            reader.reload()?;
            assert_eq!(num_docs_containing("a"), 0);
            assert_eq!(num_docs_containing("b"), 1);
            assert_eq!(num_docs_containing("c"), 1);
        }
        reader.reload()?;
        reader.searcher();
        Ok(())
    }

    #[test]
    fn test_merge_on_empty_segments_single_segment() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        // writing the segment
        let mut index_writer = index.writer(12_000_000).unwrap();
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        //  this should create 1 segment

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 1);

        reader.reload().unwrap();
        assert_eq!(num_docs_containing("a"), 1);

        index_writer.delete_term(Term::from_field_text(text_field, "a"));
        index_writer.commit()?;

        reader.reload().unwrap();
        assert_eq!(num_docs_containing("a"), 0);

        index_writer.merge(&segments);
        index_writer.wait_merging_threads().unwrap();

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 0);

        Ok(())
    }

    #[test]
    fn test_merge_on_empty_segments() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        // writing the segment
        let mut index_writer = index.writer(12_000_000).unwrap();
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        //  this should create 4 segments

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 4);

        reader.reload().unwrap();
        assert_eq!(num_docs_containing("a"), 4);

        index_writer.delete_term(Term::from_field_text(text_field, "a"));
        index_writer.commit()?;

        reader.reload().unwrap();
        assert_eq!(num_docs_containing("a"), 0);

        index_writer.merge(&segments);
        index_writer.wait_merging_threads().unwrap();

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 0);

        Ok(())
    }

    #[test]
    fn test_with_merges() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        // writing the segment
        let mut index_writer = index.writer(12_000_000).unwrap();
        // create 8 segments with 100 tiny docs
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"))?;
        }
        index_writer.commit()?;
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"))?;
        }
        //  this should create 8 segments and trigger a merge.
        index_writer.commit()?;
        index_writer.wait_merging_threads()?;
        reader.reload()?;
        assert_eq!(num_docs_containing("a"), 200);
        assert!(index.searchable_segments()?.len() < 8);
        Ok(())
    }

    #[test]
    fn test_prepare_with_commit_message() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        // writing the segment
        let mut index_writer = index.writer(12_000_000)?;
        // create 8 segments with 100 tiny docs
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field => "a"))?;
        }
        {
            let mut prepared_commit = index_writer.prepare_commit()?;
            prepared_commit.set_payload("first commit");
            prepared_commit.commit()?;
        }
        {
            let metas = index.load_metas()?;
            assert_eq!(metas.payload.unwrap(), "first commit");
        }
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field => "a"))?;
        }
        index_writer.commit()?;
        {
            let metas = index.load_metas()?;
            assert!(metas.payload.is_none());
        }
        Ok(())
    }

    #[test]
    fn test_prepare_but_rollback() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(4, 12_000_000)?;
            // create 8 segments with 100 tiny docs
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "a"))?;
            }
            {
                let mut prepared_commit = index_writer.prepare_commit()?;
                prepared_commit.set_payload("first commit");
                prepared_commit.abort()?;
            }
            {
                let metas = index.load_metas()?;
                assert!(metas.payload.is_none());
            }
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "b"))?;
            }
            index_writer.commit()?;
        }
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?
                .searcher()
                .doc_freq(&term_a)
        };
        assert_eq!(num_docs_containing("a")?, 0);
        assert_eq!(num_docs_containing("b")?, 100);
        Ok(())
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

        let add_tstamp = index_writer.add_document(doc!(text_field => "a")).unwrap();
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

        let add_tstamp = index_writer.add_document(doc!(text_field => "a")).unwrap();

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
            index_writer.add_document(doc!(text_field => "b")).unwrap();
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
        index_writer.add_document(doc!(text_field => "a")).unwrap();
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
        assert!(index_writer.add_document(doc!(text_field => "a")).is_ok());
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
    fn test_sort_by_multivalue_field_error() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let options = NumericOptions::default().set_fast(Cardinality::MultiValues);
        schema_builder.add_u64_field("id", options);
        let schema = schema_builder.build();

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "id".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };

        let err = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "An invalid argument was passed: 'Only single value fast field Cardinality supported \
             for sorting index id'"
        );

        Ok(())
    }

    #[test]
    fn test_delete_with_sort_by_field() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let id_field =
            schema_builder.add_u64_field("id", schema::INDEXED | schema::STORED | schema::FAST);
        let schema = schema_builder.build();

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "id".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };

        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let index_reader = index.reader()?;
        let mut index_writer = index.writer_for_tests()?;

        // create and delete docs in same commit
        for id in 0u64..5u64 {
            index_writer.add_document(doc!(id_field => id))?;
        }
        for id in 2u64..4u64 {
            index_writer.delete_term(Term::from_field_u64(id_field, id));
        }
        for id in 5u64..10u64 {
            index_writer.add_document(doc!(id_field => id))?;
        }
        index_writer.commit()?;
        index_reader.reload()?;

        let searcher = index_reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0);
        assert_eq!(segment_reader.num_docs(), 8);
        assert_eq!(segment_reader.max_doc(), 10);
        let fast_field_reader = segment_reader.fast_fields().u64("id")?;
        let in_order_alive_ids: Vec<u64> = segment_reader
            .doc_ids_alive()
            .map(|doc| fast_field_reader.get_val(doc))
            .collect();
        assert_eq!(&in_order_alive_ids[..], &[9, 8, 7, 6, 5, 4, 1, 0]);
        Ok(())
    }

    #[test]
    fn test_delete_query_with_sort_by_field() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let id_field =
            schema_builder.add_u64_field("id", schema::INDEXED | schema::STORED | schema::FAST);
        let schema = schema_builder.build();

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "id".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };

        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let index_reader = index.reader()?;
        let mut index_writer = index.writer_for_tests()?;

        // create and delete docs in same commit
        for id in 0u64..5u64 {
            index_writer.add_document(doc!(id_field => id))?;
        }
        for id in 1u64..4u64 {
            let term = Term::from_field_u64(id_field, id);
            let not_term = Term::from_field_u64(id_field, 2);
            let term = Box::new(TermQuery::new(term, Default::default()));
            let not_term = Box::new(TermQuery::new(not_term, Default::default()));

            let query: BooleanQuery = vec![
                (Occur::Must, term as Box<dyn Query>),
                (Occur::MustNot, not_term as Box<dyn Query>),
            ]
            .into();

            index_writer.delete_query(Box::new(query))?;
        }
        for id in 5u64..10u64 {
            index_writer.add_document(doc!(id_field => id))?;
        }
        index_writer.commit()?;
        index_reader.reload()?;

        let searcher = index_reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0);
        assert_eq!(segment_reader.num_docs(), 8);
        assert_eq!(segment_reader.max_doc(), 10);
        let fast_field_reader = segment_reader.fast_fields().u64("id")?;
        let in_order_alive_ids: Vec<u64> = segment_reader
            .doc_ids_alive()
            .map(|doc| fast_field_reader.get_val(doc))
            .collect();
        assert_eq!(&in_order_alive_ids[..], &[9, 8, 7, 6, 5, 4, 2, 0]);
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum IndexingOp {
        AddDoc { id: u64 },
        DeleteDoc { id: u64 },
        DeleteDocQuery { id: u64 },
        Commit,
        Merge,
    }

    fn balanced_operation_strategy() -> impl Strategy<Value = IndexingOp> {
        prop_oneof![
            (0u64..20u64).prop_map(|id| IndexingOp::DeleteDoc { id }),
            (0u64..20u64).prop_map(|id| IndexingOp::DeleteDocQuery { id }),
            (0u64..20u64).prop_map(|id| IndexingOp::AddDoc { id }),
            (0u64..1u64).prop_map(|_| IndexingOp::Commit),
            (0u64..1u64).prop_map(|_| IndexingOp::Merge),
        ]
    }

    fn adding_operation_strategy() -> impl Strategy<Value = IndexingOp> {
        prop_oneof![
            5 => (0u64..100u64).prop_map(|id| IndexingOp::DeleteDoc { id }),
            5 => (0u64..100u64).prop_map(|id| IndexingOp::DeleteDocQuery { id }),
            50 => (0u64..100u64).prop_map(|id| IndexingOp::AddDoc { id }),
            2 => (0u64..1u64).prop_map(|_| IndexingOp::Commit),
            1 => (0u64..1u64).prop_map(|_| IndexingOp::Merge),
        ]
    }

    fn expected_ids(ops: &[IndexingOp]) -> (HashMap<u64, u64>, HashSet<u64>) {
        let mut existing_ids = HashMap::new();
        let mut deleted_ids = HashSet::new();
        for &op in ops {
            match op {
                IndexingOp::AddDoc { id } => {
                    *existing_ids.entry(id).or_insert(0) += 1;
                    deleted_ids.remove(&id);
                }
                IndexingOp::DeleteDoc { id } => {
                    existing_ids.remove(&id);
                    deleted_ids.insert(id);
                }
                IndexingOp::DeleteDocQuery { id } => {
                    existing_ids.remove(&id);
                    deleted_ids.insert(id);
                }
                _ => {}
            }
        }
        (existing_ids, deleted_ids)
    }

    fn get_id_list(ops: &[IndexingOp]) -> Vec<u64> {
        let mut id_list = Vec::new();
        for &op in ops {
            match op {
                IndexingOp::AddDoc { id } => {
                    id_list.push(id);
                }
                IndexingOp::DeleteDoc { id } => {
                    id_list.retain(|el| *el != id);
                }
                IndexingOp::DeleteDocQuery { id } => {
                    id_list.retain(|el| *el != id);
                }
                _ => {}
            }
        }
        id_list
    }

    fn test_operation_strategy(
        ops: &[IndexingOp],
        sort_index: bool,
        force_end_merge: bool,
    ) -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip", FAST | INDEXED | STORED);
        let ips_field = schema_builder.add_ip_addr_field(
            "ips",
            IpAddrOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_indexed(),
        );
        let id_field = schema_builder.add_u64_field("id", FAST | INDEXED | STORED);
        let i64_field = schema_builder.add_i64_field("i64", INDEXED);
        let f64_field = schema_builder.add_f64_field("f64", INDEXED);
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let bytes_field = schema_builder.add_bytes_field("bytes", FAST | INDEXED | STORED);
        let bool_field = schema_builder.add_bool_field("bool", FAST | INDEXED | STORED);
        let text_field = schema_builder.add_text_field(
            "text_field",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );

        let large_text_field = schema_builder.add_text_field("large_text_field", TEXT | STORED);
        let multi_text_fields = schema_builder.add_text_field("multi_text_fields", TEXT | STORED);

        let multi_numbers = schema_builder.add_u64_field(
            "multi_numbers",
            NumericOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_stored(),
        );
        let multi_bools = schema_builder.add_bool_field(
            "multi_bools",
            NumericOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_stored(),
        );
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let settings = if sort_index {
            IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "id".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            }
        } else {
            IndexSettings {
                ..Default::default()
            }
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        let old_reader = index.reader()?;

        let ip_exists = |id| id % 3 != 0; // 0 does not exist

        let multi_text_field_text1 = "test1 test2 test3 test1 test2 test3";
        // rotate left
        let multi_text_field_text2 = "test2 test3 test1 test2 test3 test1";
        // rotate right
        let multi_text_field_text3 = "test3 test1 test2 test3 test1 test2";

        let ip_from_id = |id| Ipv6Addr::from_u128(id as u128);

        for &op in ops {
            match op {
                IndexingOp::AddDoc { id } => {
                    let facet = Facet::from(&("/cola/".to_string() + &id.to_string()));
                    let ip = ip_from_id(id);

                    if !ip_exists(id) {
                        // every 3rd doc has no ip field
                        index_writer.add_document(doc!(id_field=>id,
                                bytes_field => id.to_le_bytes().as_slice(),
                                multi_numbers=> id,
                                multi_numbers => id,
                                bool_field => (id % 2u64) != 0,
                                i64_field => id as i64,
                                f64_field => id as f64,
                                date_field => DateTime::from_timestamp_secs(id as i64),
                                multi_bools => (id % 2u64) != 0,
                                multi_bools => (id % 2u64) == 0,
                                text_field => id.to_string(),
                                facet_field => facet,
                                large_text_field => LOREM,
                                multi_text_fields => multi_text_field_text1,
                                multi_text_fields => multi_text_field_text2,
                                multi_text_fields => multi_text_field_text3,
                        ))?;
                    } else {
                        index_writer.add_document(doc!(id_field=>id,
                                bytes_field => id.to_le_bytes().as_slice(),
                                ip_field => ip,
                                ips_field => ip,
                                ips_field => ip,
                                multi_numbers=> id,
                                multi_numbers => id,
                                bool_field => (id % 2u64) != 0,
                                i64_field => id as i64,
                                f64_field => id as f64,
                                date_field => DateTime::from_timestamp_secs(id as i64),
                                multi_bools => (id % 2u64) != 0,
                                multi_bools => (id % 2u64) == 0,
                                text_field => id.to_string(),
                                facet_field => facet,
                                large_text_field => LOREM,
                                multi_text_fields => multi_text_field_text1,
                                multi_text_fields => multi_text_field_text2,
                                multi_text_fields => multi_text_field_text3,
                        ))?;
                    }
                }
                IndexingOp::DeleteDoc { id } => {
                    index_writer.delete_term(Term::from_field_u64(id_field, id));
                }
                IndexingOp::DeleteDocQuery { id } => {
                    let term = Term::from_field_u64(id_field, id);
                    let query = TermQuery::new(term, Default::default());
                    index_writer.delete_query(Box::new(query))?;
                }
                IndexingOp::Commit => {
                    index_writer.commit()?;
                }
                IndexingOp::Merge => {
                    let segment_ids = index
                        .searchable_segment_ids()
                        .expect("Searchable segments failed.");
                    if segment_ids.len() >= 2 {
                        index_writer.merge(&segment_ids).wait().unwrap();
                        assert!(index_writer.segment_updater().wait_merging_thread().is_ok());
                    }
                }
            }
        }
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let num_segments_before_merge = searcher.segment_readers().len();
        if force_end_merge {
            index_writer.wait_merging_threads()?;
            let mut index_writer = index.writer_for_tests()?;
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            if segment_ids.len() >= 2 {
                index_writer.merge(&segment_ids).wait().unwrap();
                assert!(index_writer.wait_merging_threads().is_ok());
            }
        }
        let num_segments_after_merge = searcher.segment_readers().len();

        old_reader.reload()?;
        let old_searcher = old_reader.searcher();

        let ids_old_searcher: HashSet<u64> = old_searcher
            .segment_readers()
            .iter()
            .flat_map(|segment_reader| {
                let ff_reader = segment_reader.fast_fields().u64("id").unwrap();
                segment_reader
                    .doc_ids_alive()
                    .map(move |doc| ff_reader.get_val(doc))
            })
            .collect();

        let ids: HashSet<u64> = searcher
            .segment_readers()
            .iter()
            .flat_map(|segment_reader| {
                let ff_reader = segment_reader.fast_fields().u64("id").unwrap();
                segment_reader
                    .doc_ids_alive()
                    .map(move |doc| ff_reader.get_val(doc))
            })
            .collect();

        let (expected_ids_and_num_occurrences, deleted_ids) = expected_ids(ops);

        let id_list = get_id_list(ops);

        // multivalue fast field content
        let mut all_ips = Vec::new();
        let mut num_ips = 0;
        for segment_reader in searcher.segment_readers().iter() {
            let ip_reader = segment_reader.fast_fields().ip_addrs("ips").unwrap();
            for doc in segment_reader.doc_ids_alive() {
                let mut vals = vec![];
                ip_reader.get_vals(doc, &mut vals);
                all_ips.extend_from_slice(&vals);
            }
            num_ips += ip_reader.total_num_vals();
        }

        let num_docs_expected = expected_ids_and_num_occurrences
            .values()
            .map(|id_occurrences| *id_occurrences as usize)
            .sum::<usize>();
        assert_eq!(searcher.num_docs() as usize, num_docs_expected);
        assert_eq!(old_searcher.num_docs() as usize, num_docs_expected);
        assert_eq!(
            ids_old_searcher,
            expected_ids_and_num_occurrences
                .keys()
                .cloned()
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            ids,
            expected_ids_and_num_occurrences
                .keys()
                .cloned()
                .collect::<HashSet<_>>()
        );

        if force_end_merge && num_segments_before_merge > 1 && num_segments_after_merge == 1 {
            let mut expected_multi_ips: Vec<_> = id_list
                .iter()
                .filter(|id| ip_exists(**id))
                .flat_map(|id| vec![ip_from_id(*id), ip_from_id(*id)])
                .collect();
            assert_eq!(num_ips, expected_multi_ips.len() as u32);

            expected_multi_ips.sort();
            all_ips.sort();
            assert_eq!(expected_multi_ips, all_ips);

            // Test fastfield num_docs
            let num_docs: usize = searcher
                .segment_readers()
                .iter()
                .map(|segment_reader| {
                    let ff_reader = segment_reader.fast_fields().ip_addrs("ips").unwrap();
                    ff_reader.get_index_reader().num_docs() as usize
                })
                .sum();
            assert_eq!(num_docs, num_docs_expected);
        }

        // Load all ips addr
        let ips: HashSet<Ipv6Addr> = searcher
            .segment_readers()
            .iter()
            .flat_map(|segment_reader| {
                let ff_reader = segment_reader.fast_fields().ip_addr("ip").unwrap();
                segment_reader.doc_ids_alive().flat_map(move |doc| {
                    let val = ff_reader.get_val(doc);
                    if val == Ipv6Addr::from_u128(0) {
                        // TODO Fix null handling
                        None
                    } else {
                        Some(val)
                    }
                })
            })
            .collect();

        let expected_ips = expected_ids_and_num_occurrences
            .keys()
            .flat_map(|id| {
                if !ip_exists(*id) {
                    None
                } else {
                    Some(Ipv6Addr::from_u128(*id as u128))
                }
            })
            .collect::<HashSet<_>>();
        assert_eq!(ips, expected_ips);

        let expected_ips = expected_ids_and_num_occurrences
            .keys()
            .filter_map(|id| {
                if !ip_exists(*id) {
                    None
                } else {
                    Some(Ipv6Addr::from_u128(*id as u128))
                }
            })
            .collect::<HashSet<_>>();
        let ips: HashSet<Ipv6Addr> = searcher
            .segment_readers()
            .iter()
            .flat_map(|segment_reader| {
                let ff_reader = segment_reader.fast_fields().ip_addrs("ips").unwrap();
                segment_reader.doc_ids_alive().flat_map(move |doc| {
                    let mut vals = vec![];
                    ff_reader.get_vals(doc, &mut vals);
                    vals.into_iter().filter(|val| val.to_u128() != 0) // TODO Fix null handling
                })
            })
            .collect();
        assert_eq!(ips, expected_ips);

        // multivalue fast field tests
        for segment_reader in searcher.segment_readers().iter() {
            let id_reader = segment_reader.fast_fields().u64("id").unwrap();
            let ff_reader = segment_reader.fast_fields().u64s("multi_numbers").unwrap();
            let bool_ff_reader = segment_reader.fast_fields().bools("multi_bools").unwrap();
            for doc in segment_reader.doc_ids_alive() {
                let mut vals = vec![];
                ff_reader.get_vals(doc, &mut vals);
                assert_eq!(vals.len(), 2);
                assert_eq!(vals[0], vals[1]);
                assert_eq!(id_reader.get_val(doc), vals[0]);

                let mut bool_vals = vec![];
                bool_ff_reader.get_vals(doc, &mut bool_vals);
                assert_eq!(bool_vals.len(), 2);
                assert_ne!(bool_vals[0], bool_vals[1]);

                assert!(expected_ids_and_num_occurrences.contains_key(&vals[0]));
            }
        }

        // doc store tests
        for segment_reader in searcher.segment_readers().iter() {
            let store_reader = segment_reader
                .get_store_reader(DOCSTORE_CACHE_CAPACITY)
                .unwrap();
            // test store iterator
            for doc in store_reader.iter(segment_reader.alive_bitset()) {
                let id = doc.unwrap().get_first(id_field).unwrap().as_u64().unwrap();
                assert!(expected_ids_and_num_occurrences.contains_key(&id));
            }
            // test store random access
            for doc_id in segment_reader.doc_ids_alive() {
                let id = store_reader
                    .get(doc_id)
                    .unwrap()
                    .get_first(id_field)
                    .unwrap()
                    .as_u64()
                    .unwrap();
                assert!(expected_ids_and_num_occurrences.contains_key(&id));
                let id2 = store_reader
                    .get(doc_id)
                    .unwrap()
                    .get_first(multi_numbers)
                    .unwrap()
                    .as_u64()
                    .unwrap();
                assert_eq!(id, id2);
                let bool = store_reader
                    .get(doc_id)
                    .unwrap()
                    .get_first(bool_field)
                    .unwrap()
                    .as_bool()
                    .unwrap();
                let doc = store_reader.get(doc_id).unwrap();
                let mut bool2 = doc.get_all(multi_bools);
                assert_eq!(bool, bool2.next().unwrap().as_bool().unwrap());
                assert_ne!(bool, bool2.next().unwrap().as_bool().unwrap());
                assert_eq!(None, bool2.next())
            }
        }
        // test search
        let do_search = |term: &str, field| {
            let query = QueryParser::for_index(&index, vec![field])
                .parse_query(term)
                .unwrap();
            let top_docs: Vec<(f32, DocAddress)> =
                searcher.search(&query, &TopDocs::with_limit(1000)).unwrap();

            top_docs.iter().map(|el| el.1).collect::<Vec<_>>()
        };

        let do_search2 = |term: Term| {
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            let top_docs: Vec<(f32, DocAddress)> =
                searcher.search(&query, &TopDocs::with_limit(1000)).unwrap();

            top_docs.iter().map(|el| el.1).collect::<Vec<_>>()
        };

        for (existing_id, count) in &expected_ids_and_num_occurrences {
            let (existing_id, count) = (*existing_id, *count);
            let get_num_hits = |field| do_search(&existing_id.to_string(), field).len() as u64;
            assert_eq!(get_num_hits(text_field), count);
            assert_eq!(get_num_hits(i64_field), count);
            assert_eq!(get_num_hits(f64_field), count);
            assert_eq!(get_num_hits(id_field), count);

            // Test multi text
            assert_eq!(
                do_search("\"test1 test2\"", multi_text_fields).len(),
                num_docs_expected
            );
            assert_eq!(
                do_search("\"test2 test3\"", multi_text_fields).len(),
                num_docs_expected
            );

            // Test bytes
            let term = Term::from_field_bytes(bytes_field, existing_id.to_le_bytes().as_slice());
            assert_eq!(do_search2(term).len() as u64, count);

            // Test date
            let term = Term::from_field_date(
                date_field,
                DateTime::from_timestamp_secs(existing_id as i64),
            );
            assert_eq!(do_search2(term).len() as u64, count);
        }
        for deleted_id in deleted_ids {
            let assert_field = |field| {
                assert_eq!(do_search(&deleted_id.to_string(), field).len() as u64, 0);
            };
            assert_field(text_field);
            assert_field(f64_field);
            assert_field(i64_field);
            assert_field(id_field);

            // Test bytes
            let term = Term::from_field_bytes(bytes_field, deleted_id.to_le_bytes().as_slice());
            assert_eq!(do_search2(term).len() as u64, 0);

            // Test date
            let term =
                Term::from_field_date(date_field, DateTime::from_timestamp_secs(deleted_id as i64));
            assert_eq!(do_search2(term).len() as u64, 0);
        }
        // search ip address
        //
        for (existing_id, count) in &expected_ids_and_num_occurrences {
            let (existing_id, count) = (*existing_id, *count);
            if !ip_exists(existing_id) {
                continue;
            }
            let do_search_ip_field = |term: &str| do_search(term, ip_field).len() as u64;
            let ip_addr = Ipv6Addr::from_u128(existing_id as u128);
            // Test incoming ip as ipv6
            assert_eq!(do_search_ip_field(&format!("\"{}\"", ip_addr)), count);

            let term = Term::from_field_ip_addr(ip_field, ip_addr);
            assert_eq!(do_search2(term).len() as u64, count);

            // Test incoming ip as ipv4
            if let Some(ip_addr) = ip_addr.to_ipv4_mapped() {
                assert_eq!(do_search_ip_field(&format!("\"{}\"", ip_addr)), count);
            }
        }

        // assert data is like expected
        //
        for (existing_id, count) in expected_ids_and_num_occurrences.iter().take(10) {
            let (existing_id, count) = (*existing_id, *count);
            if !ip_exists(existing_id) {
                continue;
            }
            let gen_query_inclusive = |field: &str, from: Ipv6Addr, to: Ipv6Addr| {
                format!("{}:[{} TO {}]", field, &from.to_string(), &to.to_string())
            };
            let ip = ip_from_id(existing_id);

            let do_search_ip_field = |term: &str| do_search(term, ip_field).len() as u64;
            // Range query on single value field
            // let query = gen_query_inclusive("ip", ip, ip);
            // assert_eq!(do_search_ip_field(&query), count);

            // Range query on multi value field
            let query = gen_query_inclusive("ips", ip, ip);
            assert_eq!(do_search_ip_field(&query), count);
        }

        // ip range query on fast field
        //
        for (existing_id, count) in expected_ids_and_num_occurrences.iter().take(10) {
            let (existing_id, count) = (*existing_id, *count);
            if !ip_exists(existing_id) {
                continue;
            }
            let gen_query_inclusive = |field: &str, from: Ipv6Addr, to: Ipv6Addr| {
                format!("{}:[{} TO {}]", field, &from.to_string(), &to.to_string())
            };
            let ip = ip_from_id(existing_id);

            let do_search_ip_field = |term: &str| do_search(term, ip_field).len() as u64;
            // Range query on single value field
            // let query = gen_query_inclusive("ip", ip, ip);
            // assert_eq!(do_search_ip_field(&query), count);

            // Range query on multi value field
            let query = gen_query_inclusive("ips", ip, ip);
            assert_eq!(do_search_ip_field(&query), count);
        }

        // test facets
        for segment_reader in searcher.segment_readers().iter() {
            let mut facet_reader = segment_reader.facet_reader(facet_field).unwrap();
            let ff_reader = segment_reader.fast_fields().u64("id").unwrap();
            for doc_id in segment_reader.doc_ids_alive() {
                let mut facet_ords = Vec::new();
                facet_reader.facet_ords(doc_id, &mut facet_ords);
                assert_eq!(facet_ords.len(), 1);
                let mut facet = Facet::default();
                facet_reader
                    .facet_from_ord(facet_ords[0], &mut facet)
                    .unwrap();
                let id = ff_reader.get_val(doc_id);
                let facet_expected = Facet::from(&("/cola/".to_string() + &id.to_string()));

                assert_eq!(facet, facet_expected);
            }
        }
        Ok(())
    }

    #[test]
    fn test_ip_range_query_multivalue_bug() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::AddDoc { id: 2 },
                IndexingOp::Commit,
                IndexingOp::AddDoc { id: 1 },
                IndexingOp::AddDoc { id: 1 },
                IndexingOp::Commit,
                IndexingOp::Merge
            ],
            true,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_ff_num_ips_regression() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::AddDoc { id: 13 },
                IndexingOp::AddDoc { id: 1 },
                IndexingOp::Commit,
                IndexingOp::DeleteDocQuery { id: 13 },
                IndexingOp::AddDoc { id: 1 },
                IndexingOp::Commit,
            ],
            false,
            true
        )
        .is_ok());
    }

    #[test]
    fn test_minimal() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::AddDoc { id: 23 },
                IndexingOp::AddDoc { id: 13 },
                IndexingOp::DeleteDoc { id: 13 }
            ],
            true,
            true
        )
        .is_ok());

        assert!(test_operation_strategy(
            &[
                IndexingOp::AddDoc { id: 23 },
                IndexingOp::AddDoc { id: 13 },
                IndexingOp::DeleteDoc { id: 13 }
            ],
            false,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_minimal_sort_merge() {
        assert!(test_operation_strategy(&[IndexingOp::AddDoc { id: 3 },], true, true).is_ok());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        #[test]
        fn test_delete_with_sort_proptest_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
            assert!(test_operation_strategy(&ops[..], true, false).is_ok());
        }
        #[test]
        fn test_delete_without_sort_proptest_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
            assert!(test_operation_strategy(&ops[..], false, false).is_ok());
        }
        #[test]
        fn test_delete_with_sort_proptest_with_merge_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
            assert!(test_operation_strategy(&ops[..], true, true).is_ok());
        }
        #[test]
        fn test_delete_without_sort_proptest_with_merge_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
            assert!(test_operation_strategy(&ops[..], false, true).is_ok());
        }

        #[test]
        fn test_delete_with_sort_proptest(ops in proptest::collection::vec(balanced_operation_strategy(), 1..10)) {
            assert!(test_operation_strategy(&ops[..], true, false).is_ok());
        }
        #[test]
        fn test_delete_without_sort_proptest(ops in proptest::collection::vec(balanced_operation_strategy(), 1..10)) {
            assert!(test_operation_strategy(&ops[..], false, false).is_ok());
        }
        #[test]
        fn test_delete_with_sort_proptest_with_merge(ops in proptest::collection::vec(balanced_operation_strategy(), 1..10)) {
            assert!(test_operation_strategy(&ops[..], true, true).is_ok());
        }
        #[test]
        fn test_delete_without_sort_proptest_with_merge(ops in proptest::collection::vec(balanced_operation_strategy(), 1..100)) {
            assert!(test_operation_strategy(&ops[..], false, true).is_ok());
        }


    }

    #[test]
    fn test_delete_with_sort_by_field_last_opstamp_is_not_max() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let sort_by_field = schema_builder.add_u64_field("sort_by", FAST);
        let id_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "sort_by".to_string(),
                order: Order::Asc,
            }),
            ..Default::default()
        };

        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;

        // We add a doc...
        index_writer.add_document(doc!(sort_by_field => 2u64, id_field => 0u64))?;
        // And remove it.
        index_writer.delete_term(Term::from_field_u64(id_field, 0u64));
        // We add another doc.
        index_writer.add_document(doc!(sort_by_field=>1u64, id_field => 0u64))?;

        // The expected result is a segment with
        // maxdoc = 2
        // numdoc = 1.
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0);
        assert_eq!(segment_reader.max_doc(), 2);
        assert_eq!(segment_reader.num_docs(), 1);
        Ok(())
    }

    #[test]
    fn test_index_doc_missing_field() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let idfield = schema_builder.add_text_field("id", STRING);
        schema_builder.add_text_field("optfield", STRING);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(idfield=>"myid"))?;
        index_writer.commit()?;
        Ok(())
    }

    #[test]
    fn test_bug_1617_3() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::DeleteDoc { id: 0 },
                IndexingOp::AddDoc { id: 6 },
                IndexingOp::DeleteDocQuery { id: 11 },
                IndexingOp::Commit,
                IndexingOp::Merge,
                IndexingOp::Commit,
                IndexingOp::Commit
            ],
            false,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_bug_1617_2() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::AddDoc { id: 13 },
                IndexingOp::DeleteDoc { id: 13 },
                IndexingOp::Commit,
                IndexingOp::AddDoc { id: 30 },
                IndexingOp::Commit,
                IndexingOp::Merge,
            ],
            false,
            true
        )
        .is_ok());
    }

    #[test]
    fn test_bug_1617() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let id_field = schema_builder.add_u64_field("id", INDEXED);

        let schema = schema_builder.build();
        let index = Index::builder().schema(schema).create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        let existing_id = 16u64;
        let deleted_id = 13u64;
        index_writer.add_document(doc!(
            id_field=>existing_id,
        ))?;
        index_writer.add_document(doc!(
            id_field=>deleted_id,
        ))?;
        index_writer.delete_term(Term::from_field_u64(id_field, deleted_id));
        index_writer.commit()?;

        // Merge
        {
            assert!(index_writer.wait_merging_threads().is_ok());
            let mut index_writer = index.writer_for_tests()?;
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer.merge(&segment_ids).wait().unwrap();
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        let searcher = index.reader()?.searcher();

        let query = TermQuery::new(
            Term::from_field_u64(id_field, existing_id),
            IndexRecordOption::Basic,
        );
        let top_docs: Vec<(f32, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        assert_eq!(top_docs.len(), 1); // Fails

        Ok(())
    }

    #[test]
    fn test_bug_1618() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let id_field = schema_builder.add_i64_field("id", INDEXED);

        let schema = schema_builder.build();
        let index = Index::builder().schema(schema).create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        index_writer.add_document(doc!(
            id_field=>10i64,
        ))?;
        index_writer.add_document(doc!(
            id_field=>30i64,
        ))?;
        index_writer.commit()?;

        // Merge
        {
            assert!(index_writer.wait_merging_threads().is_ok());
            let mut index_writer = index.writer_for_tests()?;
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer.merge(&segment_ids).wait().unwrap();
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        let searcher = index.reader()?.searcher();

        let query = TermQuery::new(
            Term::from_field_i64(id_field, 10i64),
            IndexRecordOption::Basic,
        );
        let top_docs: Vec<(f32, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        assert_eq!(top_docs.len(), 1); // Fails

        let query = TermQuery::new(
            Term::from_field_i64(id_field, 30i64),
            IndexRecordOption::Basic,
        );
        let top_docs: Vec<(f32, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        assert_eq!(top_docs.len(), 1); // Fails

        Ok(())
    }
}
