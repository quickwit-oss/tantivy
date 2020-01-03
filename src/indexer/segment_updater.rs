use super::segment_manager::{get_mergeable_segments, SegmentManager};
use crate::core::Index;
use crate::core::IndexMeta;
use crate::core::Segment;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::core::SerializableSegment;
use crate::core::META_FILEPATH;
use crate::directory::{Directory, DirectoryClone, GarbageCollectionResult};
use crate::indexer::index_writer::advance_deletes;
use crate::indexer::merge_operation::MergeOperationInventory;
use crate::indexer::merger::IndexMerger;
use crate::indexer::segment_manager::{SegmentRegisters, SegmentsStatus};
use crate::indexer::stamper::Stamper;
use crate::indexer::SegmentEntry;
use crate::indexer::SegmentSerializer;
use crate::indexer::{DefaultMergePolicy, MergePolicy};
use crate::indexer::{MergeCandidate, MergeOperation};
use crate::schema::Schema;
use crate::Opstamp;
use futures::channel::oneshot;
use futures::executor::{ThreadPool, ThreadPoolBuilder};
use futures::future::Future;
use futures::future::TryFutureExt;
use serde_json;
use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

const NUM_MERGE_THREADS: usize = 4;

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
pub(crate) struct SegmentUpdater(Arc<InnerSegmentUpdater>);

impl Deref for SegmentUpdater {
    type Target = InnerSegmentUpdater;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

async fn garbage_collect_files(
    segment_updater: SegmentUpdater,
) -> crate::Result<GarbageCollectionResult> {
    info!("Running garbage collection");
    let mut index = segment_updater.index.clone();
    index
        .directory_mut()
        .garbage_collect(move || segment_updater.list_files())
}

/// Merges a list of segments the list of segment givens in the `segment_entries`.
/// This function happens in the calling thread and is computationally expensive.
fn merge(
    index: &Index,
    mut segment_entries: Vec<SegmentEntry>,
    target_opstamp: Opstamp,
) -> crate::Result<SegmentEntry> {
    // first we need to apply deletes to our segment.
    let mut merged_segment = index.new_segment();

    // First we apply all of the delet to the merged segment, up to the target opstamp.
    for segment_entry in &mut segment_entries {
        advance_deletes(segment_entry, target_opstamp)?;
    }

    let delete_cursor = segment_entries[0].delete_cursor().clone();

    let segments: Vec<Segment> = segment_entries
        .iter()
        .map(|segment_entry| index.segment(segment_entry.meta().clone()))
        .collect();

    // An IndexMerger is like a "view" of our merged segments.
    let merger: IndexMerger = IndexMerger::open(index.schema(), &segments[..])?;

    // ... we just serialize this index merger in our new segment to merge the two segments.
    let segment_serializer = SegmentSerializer::for_segment(&mut merged_segment)?;

    let max_doc = merger.write(segment_serializer)?;

    Ok(SegmentEntry::new(
        merged_segment.with_max_doc(max_doc),
        delete_cursor,
        None,
    ))
}

pub(crate) struct InnerSegmentUpdater {
    // we keep a copy of the current active IndexMeta to
    // avoid loading the file everytime we need it in the
    // `SegmentUpdater`.
    //
    // This should be up to date as all update happen through
    // the unique active `SegmentUpdater`.
    active_metas: RwLock<Arc<IndexMeta>>,
    pool: ThreadPool,
    merge_thread_pool: ThreadPool,

    index: Index,
    segment_manager: SegmentManager,
    merge_policy: RwLock<Arc<Box<dyn MergePolicy>>>,
    killed: AtomicBool,
    stamper: Stamper,
    merge_operations: MergeOperationInventory,
}

impl SegmentUpdater {
    pub fn create(
        segment_registers: Arc<RwLock<SegmentRegisters>>,
        index: Index,
        stamper: Stamper,
    ) -> crate::Result<SegmentUpdater> {
        let segment_manager = SegmentManager::new(segment_registers);
        let pool = ThreadPoolBuilder::new()
            .name_prefix("segment_updater")
            .pool_size(1)
            .create()
            .map_err(|_| {
                crate::TantivyError::SystemError(
                    "Failed to spawn segment updater thread".to_string(),
                )
            })?;
        let merge_thread_pool = ThreadPoolBuilder::new()
            .name_prefix("merge_thread")
            .pool_size(NUM_MERGE_THREADS)
            .create()
            .map_err(|_| {
                crate::TantivyError::SystemError(
                    "Failed to spawn segment merging thread".to_string(),
                )
            })?;
        let index_meta = index.load_metas()?;
        Ok(SegmentUpdater(Arc::new(InnerSegmentUpdater {
            active_metas: RwLock::new(Arc::new(index_meta)),
            pool,
            merge_thread_pool,
            index,
            segment_manager,
            merge_policy: RwLock::new(Arc::new(Box::new(DefaultMergePolicy::default()))),
            killed: AtomicBool::new(false),
            stamper,
            merge_operations: Default::default(),
        })))
    }

    pub fn get_merge_policy(&self) -> Arc<Box<dyn MergePolicy>> {
        self.merge_policy.read().unwrap().clone()
    }

    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        let arc_merge_policy = Arc::new(merge_policy);
        *self.merge_policy.write().unwrap() = arc_merge_policy;
    }

    fn schedule_future<T: 'static + Send, F: Future<Output = crate::Result<T>> + 'static + Send>(
        &self,
        f: F,
    ) -> impl Future<Output = crate::Result<T>> {
        let (sender, receiver) = oneshot::channel();
        if self.is_alive() {
            self.pool.spawn_ok(async move {
                let _ = sender.send(f.await);
            });
        } else {
            let _ = sender.send(Err(crate::TantivyError::SystemError(
                "Segment updater killed".to_string(),
            )));
        }
        receiver.unwrap_or_else(|_| {
            let err_msg =
                "A segment_updater future did not success. This should never happen.".to_string();
            Err(crate::TantivyError::SystemError(err_msg))
        })
    }

    pub fn schedule_add_segment(
        &self,
        segment_entry: SegmentEntry,
    ) -> impl Future<Output = crate::Result<()>> {
        // TODO temporary: serializing the segment at this point.
        let segment_updater = self.clone();
        self.schedule_future(async move {
            segment_updater.segment_manager.add_segment(segment_entry);
            segment_updater.consider_merge_options().await;
            Ok(())
        })
    }

    /// Orders `SegmentManager` to remove all segments
    pub(crate) fn remove_all_segments(&self) {
        self.segment_manager.remove_all_segments();
    }

    pub fn kill(&mut self) {
        self.killed.store(true, Ordering::Release);
    }

    pub fn is_alive(&self) -> bool {
        !self.killed.load(Ordering::Acquire)
    }

    /// Apply deletes up to the target opstamp to all segments.
    ///
    /// The method returns copies of the segment entries,
    /// updated with the delete information.
    fn purge_deletes(&self, target_opstamp: Opstamp) -> crate::Result<Vec<SegmentEntry>> {
        let mut segment_entries = self.segment_manager.segment_entries();
        for segment_entry in &mut segment_entries {
            advance_deletes(segment_entry, target_opstamp)?;
        }
        Ok(segment_entries)
    }

    pub fn save_metas(
        &self,
        opstamp: Opstamp,
        commit_message: Option<String>,
    ) -> crate::Result<()> {
        if self.is_alive() {
            let index = &self.index;
            let directory = index.directory();
            let mut commited_segment_metas = self.segment_manager.committed_segment_metas();

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

    pub fn schedule_garbage_collect(
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
        let mut files: HashSet<PathBuf> = self
            .index
            .list_all_segment_metas()
            .into_iter()
            .flat_map(|segment_meta| segment_meta.list_files())
            .collect();
        files.insert(META_FILEPATH.to_path_buf());
        files
    }

    pub fn schedule_commit(
        &self,
        opstamp: Opstamp,
        payload: Option<String>,
        soft_commit: bool,
    ) -> impl Future<Output = crate::Result<()>> {
        let segment_updater: SegmentUpdater = self.clone();
        let directory = self.index.directory().clone();
        self.schedule_future(async move {
            let mut segment_entries = segment_updater.purge_deletes(opstamp)?;
            if !soft_commit {
                for segment_entry in &mut segment_entries {
                    segment_entry.persist(directory.clone())?;
                }
            }
            segment_updater.segment_manager.commit(segment_entries);
            if !soft_commit {
                segment_updater.save_metas(opstamp, payload)?;
            }
            let _ = garbage_collect_files(segment_updater.clone()).await;
            segment_updater.consider_merge_options().await;
            Ok(())
        })
    }

    fn store_meta(&self, index_meta: &IndexMeta) {
        *self.active_metas.write().unwrap() = Arc::new(index_meta.clone());
    }

    fn load_metas(&self) -> Arc<IndexMeta> {
        self.active_metas.read().unwrap().clone()
    }

    pub(crate) fn make_merge_operation(&self, segment_ids: &[SegmentId]) -> MergeOperation {
        let commit_opstamp = self.load_metas().opstamp;
        MergeOperation::new(&self.merge_operations, commit_opstamp, segment_ids.to_vec())
    }

    // Starts a merge operation. This function will block until the merge operation is effectively
    // started. Note that it does not wait for the merge to terminate.
    // The calling thread should not be block for a long time, as this only involve waiting for the
    // `SegmentUpdater` queue which in turns only contains lightweight operations.
    //
    // The merge itself happens on a different thread.
    //
    // When successful, this function returns a `Future` for a `Result<SegmentMeta>` that represents
    // the actual outcome of the merge operation.
    //
    // It returns an error if for some reason the merge operation could not be started.
    //
    // At this point an error is not necessarily the sign of a malfunction.
    // (e.g. A rollback could have happened, between the instant when the merge operaiton was
    // suggested and the moment when it ended up being executed.)
    //
    // `segment_ids` is required to be non-empty.
    pub fn start_merge(
        &self,
        merge_operation: MergeOperation,
    ) -> crate::Result<impl Future<Output = crate::Result<SegmentMeta>>> {
        assert!(
            !merge_operation.segment_ids().is_empty(),
            "Segment_ids cannot be empty."
        );

        let segment_updater = self.clone();
        let segment_entries: Vec<SegmentEntry> = self
            .segment_manager
            .start_merge(merge_operation.segment_ids())?;

        info!("Starting merge  - {:?}", merge_operation.segment_ids());

        let (merging_future_send, merging_future_recv) =
            oneshot::channel::<crate::Result<SegmentMeta>>();

        self.merge_thread_pool.spawn_ok(async move {
            // The fact that `merge_operation` is moved here is important.
            // Its lifetime is used to track how many merging thread are currently running,
            // as well as which segment is currently in merge and therefore should not be
            // candidate for another merge.
            match merge(
                &segment_updater.index,
                segment_entries,
                merge_operation.target_opstamp(),
            ) {
                Ok(after_merge_segment_entry) => {
                    let segment_meta = segment_updater
                        .end_merge(merge_operation, after_merge_segment_entry)
                        .await;
                    let _send_result = merging_future_send.send(segment_meta);
                }
                Err(e) => {
                    warn!(
                        "Merge of {:?} was cancelled: {:?}",
                        merge_operation.segment_ids().to_vec(),
                        e
                    );
                    // ... cancel merge
                    if cfg!(test) {
                        panic!("Merge failed.");
                    }
                }
            }
        });

        Ok(merging_future_recv
            .unwrap_or_else(|_| Err(crate::TantivyError::SystemError("Merge failed".to_string()))))
    }

    async fn consider_merge_options(&self) {
        let merge_segment_ids: HashSet<SegmentId> = self.merge_operations.segment_in_merge();
        let (committed_segments, uncommitted_segments) =
            get_mergeable_segments(&merge_segment_ids, &self.segment_manager);

        // Committed segments cannot be merged with uncommitted_segments.
        // We therefore consider merges using these two sets of segments independently.
        let merge_policy = self.get_merge_policy();

        let current_opstamp = self.stamper.stamp();
        let mut merge_candidates: Vec<MergeOperation> = merge_policy
            .compute_merge_candidates(&uncommitted_segments)
            .into_iter()
            .map(|merge_candidate| {
                MergeOperation::new(&self.merge_operations, current_opstamp, merge_candidate.0)
            })
            .collect();

        let commit_opstamp = self.load_metas().opstamp;
        let committed_merge_candidates = merge_policy
            .compute_merge_candidates(&committed_segments)
            .into_iter()
            .map(|merge_candidate: MergeCandidate| {
                MergeOperation::new(&self.merge_operations, commit_opstamp, merge_candidate.0)
            })
            .collect::<Vec<_>>();
        merge_candidates.extend(committed_merge_candidates.into_iter());

        for merge_operation in merge_candidates {
            if let Err(err) = self.start_merge(merge_operation) {
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
    ) -> impl Future<Output = crate::Result<SegmentMeta>> {
        let segment_updater = self.clone();
        let after_merge_segment_meta = after_merge_segment_entry.meta().clone();
        let end_merge_future = self.schedule_future(async move {
            info!("End merge {:?}", after_merge_segment_entry.meta());
            {
                let mut delete_cursor = after_merge_segment_entry.delete_cursor();
                if let Some(delete_operation) = delete_cursor.get() {
                    let committed_opstamp = segment_updater.load_metas().opstamp;
                    if delete_operation.opstamp < committed_opstamp {
                        let _index = &segment_updater.index;
                        if let Err(e) =
                            advance_deletes(&mut after_merge_segment_entry, committed_opstamp)
                        {
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
                let segments_status = segment_updater
                    .segment_manager
                    .end_merge(merge_operation.segment_ids(), after_merge_segment_entry)?;

                if segments_status == SegmentsStatus::Committed {
                    segment_updater
                        .save_metas(previous_metas.opstamp, previous_metas.payload.clone())?;
                }

                segment_updater.consider_merge_options().await;
            } // we drop all possible handle to a now useless `SegmentMeta`.
            let _ = garbage_collect_files(segment_updater).await;
            Ok(())
        });
        end_merge_future.map_ok(|_| after_merge_segment_meta)
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
        self.merge_operations.wait_until_empty();
        Ok(())
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
            .segment_manager
            .segment_entries();
        assert!(seg_vec.is_empty());
    }
}
