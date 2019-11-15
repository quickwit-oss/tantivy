use super::segment_manager::{get_mergeable_segments, SegmentManager};
use crate::core::Index;
use crate::core::IndexMeta;
use crate::core::Segment;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::core::SerializableSegment;
use crate::core::META_FILEPATH;
use crate::directory::{Directory, DirectoryClone, GarbageCollectionResult};
use crate::error::TantivyError;
use crate::indexer::delete_queue::DeleteCursor;
use crate::indexer::index_writer::advance_deletes;
use crate::indexer::merge_operation::MergeOperationInventory;
use crate::indexer::merger::IndexMerger;
use crate::indexer::stamper::Stamper;
use crate::indexer::MergeOperation;
use crate::indexer::SegmentEntry;
use crate::indexer::SegmentSerializer;
use crate::indexer::{DefaultMergePolicy, MergePolicy};
use crate::schema::Schema;
use crate::Opstamp;
use futures::channel::oneshot;
use futures::executor::{block_on, ThreadPool, ThreadPoolBuilder};
use futures::future::Future;
use futures::future::TryFutureExt;
use serde_json;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::mem;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::thread::JoinHandle;

/// Save the index meta file.
/// This operation is atomic :
/// Either
///  - it fails, in which case an error is returned,
/// and the `meta.json` remains untouched,
/// - it succeeds, and `meta.json` is written
/// and flushed.
///
/// This method is not part of tantivy's public API
pub fn save_new_metas(schema: Schema, directory: &mut dyn Directory) -> crate::Result<()> {
    save_metas(
        &IndexMeta {
            segments: Vec::new(),
            schema,
            opstamp: 0u64,
            payload: None,
        },
        directory,
    )
}

/// Save the index meta file.
/// This operation is atomic:
/// Either
///  - it fails, in which case an error is returned,
/// and the `meta.json` remains untouched,
/// - it success, and `meta.json` is written
/// and flushed.
///
/// This method is not part of tantivy's public API
fn save_metas(metas: &IndexMeta, directory: &mut dyn Directory) -> crate::Result<()> {
    info!("save metas");
    let mut buffer = serde_json::to_vec_pretty(metas)?;
    // Just adding a new line at the end of the buffer.
    writeln!(&mut buffer)?;
    directory.atomic_write(&META_FILEPATH, &buffer[..])?;
    debug!("Saved metas {:?}", serde_json::to_string_pretty(&metas));
    Ok(())
}

// The segment update runner is in charge of processing all
//  of the `SegmentUpdate`s.
//
// All this processing happens on a single thread
// consuming a common queue.
//
// We voluntarily pass a merge_operation ref to guarantee that
// the merge_operation is alive during the process
#[derive(Clone)]
pub struct SegmentUpdater(Arc<InnerSegmentUpdater>);

fn perform_merge(
    merge_operation: &MergeOperation,
    index: &Index,
    mut segment_entries: Vec<SegmentEntry>,
) -> crate::Result<SegmentEntry> {
    let target_opstamp = merge_operation.target_opstamp();

    // first we need to apply deletes to our segment.
    let mut merged_segment = index.new_segment();

    // TODO add logging
    let schema = index.schema();

    for segment_entry in &mut segment_entries {
        let segment = index.segment(segment_entry.meta().clone());
        advance_deletes(segment, segment_entry, target_opstamp)?;
    }

    let delete_cursor = segment_entries[0].delete_cursor().clone();

    let segments: Vec<Segment> = segment_entries
        .iter()
        .map(|segment_entry| index.segment(segment_entry.meta().clone()))
        .collect();

    // An IndexMerger is like a "view" of our merged segments.
    let merger: IndexMerger = IndexMerger::open(schema, &segments[..])?;

    // ... we just serialize this index merger in our new segment
    // to merge the two segments.

    let segment_serializer = SegmentSerializer::for_segment(&mut merged_segment)?;

    let num_docs = merger.write(segment_serializer)?;

    let segment_meta = index.new_segment_meta(merged_segment.id(), num_docs);

    let after_merge_segment_entry = SegmentEntry::new(segment_meta.clone(), delete_cursor, None);
    Ok(after_merge_segment_entry)
}

async fn garbage_collect_files(
    segment_updater: SegmentUpdater,
) -> crate::Result<GarbageCollectionResult> {
    info!("Running garbage collection");
    let mut index = segment_updater.0.index.clone();
    index
        .directory_mut()
        .garbage_collect(move || segment_updater.list_files())
}

struct InnerSegmentUpdater {
    // we keep a copy of the current active IndexMeta to
    // avoid loading the file everytime we need it in the
    // `SegmentUpdater`.
    //
    // This should be up to date as all update happen through
    // the unique active `SegmentUpdater`.
    active_metas: RwLock<Arc<IndexMeta>>,
    pool: ThreadPool,
    index: Index,
    segment_manager: SegmentManager,
    merge_policy: RwLock<Arc<Box<dyn MergePolicy>>>,
    merging_thread_id: AtomicUsize,
    merging_threads: RwLock<HashMap<usize, JoinHandle<crate::Result<()>>>>,
    killed: AtomicBool,
    stamper: Stamper,
    merge_operations: MergeOperationInventory,
}

impl SegmentUpdater {
    pub fn create(
        index: Index,
        stamper: Stamper,
        delete_cursor: &DeleteCursor,
    ) -> crate::Result<SegmentUpdater> {
        let segments = index.searchable_segment_metas()?;
        let segment_manager = SegmentManager::from_segments(segments, delete_cursor);
        let pool = ThreadPoolBuilder::new()
            .name_prefix("segment_updater")
            .pool_size(1)
            .create()
            .unwrap(); // TODO fixme
        let index_meta = index.load_metas()?;
        Ok(SegmentUpdater(Arc::new(InnerSegmentUpdater {
            active_metas: RwLock::new(Arc::new(index_meta)),
            pool,
            index,
            segment_manager,
            merge_policy: RwLock::new(Arc::new(Box::new(DefaultMergePolicy::default()))),
            merging_thread_id: AtomicUsize::default(),
            merging_threads: RwLock::new(HashMap::new()),
            killed: AtomicBool::new(false),
            stamper,
            merge_operations: Default::default(),
        })))
    }

    pub fn get_merge_policy(&self) -> Arc<Box<dyn MergePolicy>> {
        self.0.merge_policy.read().unwrap().clone()
    }

    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        let arc_merge_policy = Arc::new(merge_policy);
        *self.0.merge_policy.write().unwrap() = arc_merge_policy;
    }

    fn get_merging_thread_id(&self) -> usize {
        self.0.merging_thread_id.fetch_add(1, Ordering::SeqCst)
    }

    fn schedule_future<T: 'static + Send, F: Future<Output = crate::Result<T>> + 'static + Send>(
        &self,
        f: F,
    ) -> impl Future<Output = crate::Result<T>> {
        let (sender, receiver) = oneshot::channel();
        self.0.pool.spawn_ok(async move {
            let _ = sender.send(f.await);
        });
        receiver.unwrap_or_else(|_| {
            Err(crate::Error::SystemError(
                "A segment_updater future did not success. This should never happen.".to_string(),
            ))
        })
    }

    pub fn add_segment(&self, segment_entry: SegmentEntry) {
        let segment_updater = self.clone();
        let _ = self.schedule_future(async move {
            segment_updater.0.segment_manager.add_segment(segment_entry);
            segment_updater.consider_merge_options().await;
            Ok(())
        });
    }

    /// Orders `SegmentManager` to remove all segments
    pub(crate) fn remove_all_segments(&self) {
        self.0.segment_manager.remove_all_segments();
    }

    pub fn kill(&mut self) {
        self.0.killed.store(true, Ordering::Release);
    }

    pub fn is_alive(&self) -> bool {
        !self.0.killed.load(Ordering::Acquire)
    }

    /// Apply deletes up to the target opstamp to all segments.
    ///
    /// The method returns copies of the segment entries,
    /// updated with the delete information.
    fn purge_deletes(&self, target_opstamp: Opstamp) -> crate::Result<Vec<SegmentEntry>> {
        let mut segment_entries = self.0.segment_manager.segment_entries();
        for segment_entry in &mut segment_entries {
            let segment = self.0.index.segment(segment_entry.meta().clone());
            advance_deletes(segment, segment_entry, target_opstamp)?;
        }
        Ok(segment_entries)
    }

    pub fn save_metas(
        &self,
        opstamp: Opstamp,
        commit_message: Option<String>,
    ) -> crate::Result<()> {
        if self.is_alive() {
            let index = &self.0.index;
            let directory = index.directory();
            let mut commited_segment_metas = self.0.segment_manager.committed_segment_metas();

            // We sort segment_readers by number of documents.
            // This is an heuristic to make multithreading more efficient.
            //
            // This is not done at the searcher level because I had a strange
            // use case in which I was dealing with a large static index,
            // dispatched over 5 SSD drives.
            //
            // A `UnionDirectory` makes it possible to read from these
            // 5 different drives and creates a meta.json on the fly.
            // In order to optimize the throughput, it creates a lasagna of segments
            // from the different drives.
            //
            // Segment 1 from disk 1, Segment 1 from disk 2, etc.
            commited_segment_metas.sort_by_key(|segment_meta| -(segment_meta.max_doc() as i32));
            let index_meta = IndexMeta {
                segments: commited_segment_metas,
                schema: index.schema(),
                opstamp,
                payload: commit_message,
            };
            // TODO add context to the error.
            save_metas(&index_meta, directory.box_clone().borrow_mut())?;
            self.store_meta(&index_meta);
        }
        Ok(())
    }

    pub fn garbage_collect_files(
        &self,
    ) -> impl Future<Output = crate::Result<GarbageCollectionResult>> {
        let garbage_collect_future = garbage_collect_files(self.clone());
        self.schedule_future(garbage_collect_future)
    }

    /// List the files that are useful to the index.
    ///
    /// This does not include lock files, or files that are obsolete
    /// but have not yet been deleted by the garbage collector.
    fn list_files(&self) -> HashSet<PathBuf> {
        let mut files = HashSet::new();
        files.insert(META_FILEPATH.to_path_buf());
        for segment_meta in self.0.index.list_all_segment_metas() {
            files.extend(segment_meta.list_files());
        }
        files
    }

    pub fn schedule_commit(
        &self,
        opstamp: Opstamp,
        payload: Option<String>,
    ) -> impl Future<Output = crate::Result<()>> {
        let segment_updater: SegmentUpdater = self.clone();
        self.schedule_future(async move {
            if segment_updater.is_alive() {
                let segment_entries = segment_updater.purge_deletes(opstamp)?;
                segment_updater.0.segment_manager.commit(segment_entries);
                segment_updater.save_metas(opstamp, payload)?;
                let _ = garbage_collect_files(segment_updater.clone()).await;
                segment_updater.consider_merge_options().await;
            }
            Ok(())
        })
    }

    pub fn commit(&self, opstamp: Opstamp, payload: Option<String>) -> crate::Result<()> {
        block_on(self.schedule_commit(opstamp, payload))
    }

    pub fn start_merge(
        &self,
        segment_ids: &[SegmentId],
    ) -> impl Future<Output = crate::Result<SegmentMeta>> {
        let commit_opstamp = self.load_metas().opstamp;
        let merge_operation = MergeOperation::new(
            &self.0.merge_operations,
            commit_opstamp,
            segment_ids.to_vec(),
        );
        let segment_updater = self.clone();
        let start_merge_future =
            self.schedule_future(async move { segment_updater.start_merge_impl(merge_operation) });
        async move {
            let future = start_merge_future.await?;
            future
                .await
                .map_err(|err| crate::Error::SystemError(format!("{:?}", err)))
        }
    }

    fn store_meta(&self, index_meta: &IndexMeta) {
        *self.0.active_metas.write().unwrap() = Arc::new(index_meta.clone());
    }

    fn load_metas(&self) -> Arc<IndexMeta> {
        self.0.active_metas.read().unwrap().clone()
    }

    // `segment_ids` is required to be non-empty.
    fn start_merge_impl(
        &self,
        merge_operation: MergeOperation,
    ) -> crate::Result<impl Future<Output = Result<SegmentMeta, oneshot::Canceled>>> {
        assert!(
            !merge_operation.segment_ids().is_empty(),
            "Segment_ids cannot be empty."
        );

        let segment_updater = self.clone();
        let segment_entries: Vec<SegmentEntry> = self
            .0
            .segment_manager
            .start_merge(merge_operation.segment_ids())?;

        let merging_thread_id = self.get_merging_thread_id();
        info!(
            "Starting merge thread #{} - {:?}",
            merging_thread_id,
            merge_operation.segment_ids()
        );
        let (merging_future_send, merging_future_recv) = oneshot::channel();

        // We acquire the lock preemptively as way to remove the following race condition :
        // Thread starting and trying to remove the merging thread join handle before the
        // the current thread has populated the join handle map.
        let mut merging_threads_wlock = self.0.merging_threads.write().unwrap();

        // first we need to apply deletes to our segment.
        let merging_join_handle = thread::Builder::new()
            .name(format!("mergingthread-{}", merging_thread_id))
            .spawn(move || {
                // first we need to apply deletes to our segment.
                let merge_result: crate::Result<SegmentEntry> =
                    perform_merge(&merge_operation, &segment_updater.0.index, segment_entries);
                match merge_result {
                    Ok(after_merge_segment_entry) => {
                        let merged_segment_meta = after_merge_segment_entry.meta().clone();
                        segment_updater
                            .end_merge(merge_operation, after_merge_segment_entry)
                            .expect("Segment updater thread is corrupted. Please report.");

                        // the future may fail if the listener of the oneshot future
                        // has been destroyed.
                        //
                        // This is not a problem here, so we just ignore any
                        // possible error.
                        let _send_result = merging_future_send.send(merged_segment_meta);
                    }
                    Err(e) => {
                        warn!(
                            "Merge of {:?} was cancelled: {:?}",
                            merge_operation.segment_ids(),
                            e
                        );
                        // ... cancel merge
                        if cfg!(test) {
                            panic!("Merge failed.");
                        }
                        // As `merge_operation` will be dropped, the segment in merge state will
                        // be available for merge again.
                        // `merging_future_send` will be dropped, sending an error to the future.
                    }
                }
                segment_updater
                    .0
                    .merging_threads
                    .write()
                    .unwrap()
                    .remove(&merging_thread_id);
                Ok(())
            })
            .map_err(|err| {
                crate::Error::ErrorInThread(format!("Failed to spawn thread. {:?}", err))
            })?;

        merging_threads_wlock.insert(merging_thread_id, merging_join_handle);
        Ok(merging_future_recv)
    }

    async fn consider_merge_options(&self) {
        let merge_segment_ids: HashSet<SegmentId> = self.0.merge_operations.segment_in_merge();
        let (committed_segments, uncommitted_segments) =
            get_mergeable_segments(&merge_segment_ids, &self.0.segment_manager);

        // Committed segments cannot be merged with uncommitted_segments.
        // We therefore consider merges using these two sets of segments independently.
        let merge_policy = self.get_merge_policy();

        let current_opstamp = self.0.stamper.stamp();
        let mut merge_candidates: Vec<MergeOperation> = merge_policy
            .compute_merge_candidates(&uncommitted_segments)
            .into_iter()
            .map(|merge_candidate| {
                MergeOperation::new(&self.0.merge_operations, current_opstamp, merge_candidate.0)
            })
            .collect();

        let commit_opstamp = self.load_metas().opstamp;
        let committed_merge_candidates = merge_policy
            .compute_merge_candidates(&committed_segments)
            .into_iter()
            .map(|merge_candidate| {
                MergeOperation::new(&self.0.merge_operations, commit_opstamp, merge_candidate.0)
            })
            .collect::<Vec<_>>();
        merge_candidates.extend(committed_merge_candidates.into_iter());

        for merge_operation in merge_candidates {
            if let Err(err) = self.start_merge_impl(merge_operation) {
                warn!(
                    "Starting the merge failed for the following reason. This is not fatal. {}",
                    err
                );
            }
        }
    }

    fn end_merge(
        &self,
        merge_operation: MergeOperation,
        mut after_merge_segment_entry: SegmentEntry,
    ) -> crate::Result<()> {
        let segment_updater = self.clone();
        let end_merge_future = self.schedule_future(async move {
            info!("End merge {:?}", after_merge_segment_entry.meta());
            {
                let mut delete_cursor = after_merge_segment_entry.delete_cursor().clone();
                if let Some(delete_operation) = delete_cursor.get() {
                    let committed_opstamp = segment_updater.load_metas().opstamp;
                    if delete_operation.opstamp < committed_opstamp {
                        let index = &segment_updater.0.index;
                        let segment = index.segment(after_merge_segment_entry.meta().clone());
                        if let Err(e) = advance_deletes(
                            segment,
                            &mut after_merge_segment_entry,
                            committed_opstamp,
                        ) {
                            error!(
                                "Merge of {:?} was cancelled (advancing deletes failed): {:?}",
                                merge_operation.segment_ids(),
                                e
                            );
                            if cfg!(test) {
                                panic!("Merge failed.");
                            }
                            // ... cancel merge
                            // `merge_operations` are tracked. As it is dropped, the
                            // the segment_ids will be available again for merge.
                            return Err(e);
                        }
                    }
                }
                let previous_metas = segment_updater.load_metas();
                segment_updater
                    .0
                    .segment_manager
                    .end_merge(merge_operation.segment_ids(), after_merge_segment_entry);
                // TODO no need to save meta if the merge was on uncommitted segments
                segment_updater
                    .save_metas(previous_metas.opstamp, previous_metas.payload.clone())?;
                segment_updater.consider_merge_options().await;
            } // we drop all possible handle to a now useless `SegmentMeta`.
            let _ = garbage_collect_files(segment_updater).await;
            Ok(())
        });
        block_on(end_merge_future)
    }

    /// Wait for current merging threads.
    ///
    /// Upon termination of the current merging threads,
    /// merge opportunity may appear.
    //
    /// We keep waiting until the merge policy judges that
    /// no opportunity is available.
    ///
    /// Note that it is not required to call this
    /// method in your application.
    /// Terminating your application without letting
    /// merge terminate is perfectly safe.
    ///
    /// Obsolete files will eventually be cleaned up
    /// by the directory garbage collector.
    pub fn wait_merging_thread(&self) -> crate::Result<()> {
        loop {
            let merging_threads: HashMap<usize, JoinHandle<crate::Result<()>>> = {
                let mut merging_threads = self.0.merging_threads.write().unwrap();
                mem::replace(merging_threads.deref_mut(), HashMap::new())
            };
            if merging_threads.is_empty() {
                return Ok(());
            }
            debug!("wait merging thread {}", merging_threads.len());
            for (_, merging_thread_handle) in merging_threads {
                merging_thread_handle
                    .join()
                    .map(|_| ())
                    .map_err(|_| TantivyError::ErrorInThread("Merging thread failed.".into()))?;
            }
            // Our merging thread may have queued their completed merged segment.
            // Let's wait for that too.
            block_on(self.schedule_future(async { Ok(()) }))?;
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::indexer::merge_policy::tests::MergeWheneverPossible;
    use crate::schema::*;
    use crate::Index;

    #[test]
    fn test_delete_during_merge() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        // writing the segment
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        index_writer.set_merge_policy(Box::new(MergeWheneverPossible));

        {
            for _ in 0..100 {
                index_writer.add_document(doc!(text_field=>"a"));
                index_writer.add_document(doc!(text_field=>"b"));
            }
            assert!(index_writer.commit().is_ok());
        }

        {
            for _ in 0..100 {
                index_writer.add_document(doc!(text_field=>"c"));
                index_writer.add_document(doc!(text_field=>"d"));
            }
            assert!(index_writer.commit().is_ok());
        }

        {
            index_writer.add_document(doc!(text_field=>"e"));
            index_writer.add_document(doc!(text_field=>"f"));
            assert!(index_writer.commit().is_ok());
        }

        {
            let term = Term::from_field_text(text_field, "a");
            index_writer.delete_term(term);
            assert!(index_writer.commit().is_ok());
        }
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 302);

        {
            index_writer
                .wait_merging_threads()
                .expect("waiting for merging threads");
        }

        reader.reload().unwrap();
        assert_eq!(reader.searcher().segment_readers().len(), 1);
        assert_eq!(reader.searcher().num_docs(), 302);
    }

    #[test]
    fn delete_all_docs() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        // writing the segment
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();

        {
            for _ in 0..100 {
                index_writer.add_document(doc!(text_field=>"a"));
                index_writer.add_document(doc!(text_field=>"b"));
            }
            assert!(index_writer.commit().is_ok());
        }

        {
            for _ in 0..100 {
                index_writer.add_document(doc!(text_field=>"c"));
                index_writer.add_document(doc!(text_field=>"d"));
            }
            assert!(index_writer.commit().is_ok());
        }

        {
            index_writer.add_document(doc!(text_field=>"e"));
            index_writer.add_document(doc!(text_field=>"f"));
            assert!(index_writer.commit().is_ok());
        }

        {
            let seg_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            // docs exist, should have at least 1 segment
            assert!(seg_ids.len() > 0);
        }

        {
            let term_vals = vec!["a", "b", "c", "d", "e", "f"];
            for term_val in term_vals {
                let term = Term::from_field_text(text_field, term_val);
                index_writer.delete_term(term);
                assert!(index_writer.commit().is_ok());
            }
        }

        {
            index_writer
                .wait_merging_threads()
                .expect("waiting for merging threads");
        }

        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);

        let seg_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");
        assert!(seg_ids.is_empty());

        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
        // empty segments should be erased
        assert!(index.searchable_segment_metas().unwrap().is_empty());
        assert!(reader.searcher().segment_readers().is_empty());
    }

    #[test]
    fn test_remove_all_segments() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        // writing the segment
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();

        {
            for _ in 0..100 {
                index_writer.add_document(doc!(text_field=>"a"));
                index_writer.add_document(doc!(text_field=>"b"));
            }
            assert!(index_writer.commit().is_ok());
        }
        index_writer.segment_updater().remove_all_segments();
        let seg_vec = index_writer
            .segment_updater()
            .0
            .segment_manager
            .segment_entries();
        assert!(seg_vec.is_empty());
    }
}
