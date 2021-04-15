use super::segment_manager::{get_mergeable_segments, SegmentManager};
use crate::core::Index;
use crate::core::IndexMeta;
use crate::core::Segment;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::core::SerializableSegment;
use crate::core::META_FILEPATH;
use crate::directory::{Directory, DirectoryClone, GarbageCollectionResult};
use crate::indexer::delete_queue::DeleteCursor;
use crate::indexer::index_writer::advance_deletes;
use crate::indexer::merge_operation::MergeOperationInventory;
use crate::indexer::merger::IndexMerger;
use crate::indexer::segment_manager::SegmentsStatus;
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
pub fn save_new_metas(schema: Schema, directory: &dyn Directory) -> crate::Result<()> {
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
fn save_metas(metas: &IndexMeta, directory: &dyn Directory) -> crate::Result<()> {
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
    let merged_segment = index.new_segment();

    // First we apply all of the delete to the merged segment, up to the target opstamp.
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
    let merger: IndexMerger = IndexMerger::open(index.schema(), &segments[..])?;

    // ... we just serialize this index merger in our new segment to merge the two segments.
    let segment_serializer = SegmentSerializer::for_segment(merged_segment.clone())?;

    let num_docs = merger.write(segment_serializer)?;

    let merged_segment_id = merged_segment.id();

    let segment_meta = index.new_segment_meta(merged_segment_id, num_docs);
    Ok(SegmentEntry::new(segment_meta, delete_cursor, None))
}

/// Advanced: Merges a list of segments from different indices in a new index.
///
/// Returns `TantivyError` if the the indices list is empty or their
/// schemas don't match.
///
/// `output_directory`: is assumed to be empty.
///
/// # Warning
/// This function does NOT check or take the `IndexWriter` is running. It is not
/// meant to work if you have an IndexWriter running for the origin indices, or
/// the destination Index.
#[doc(hidden)]
pub fn merge_segments<Dir: Directory>(
    indices: &[Index],
    output_directory: Dir,
) -> crate::Result<Index> {
    if indices.is_empty() {
        // If there are no indices to merge, there is no need to do anything.
        return Err(crate::TantivyError::InvalidArgument(
            "No indices given to marge".to_string(),
        ));
    }

    let target_schema = indices[0].schema();

    // let's check that all of the indices have the same schema
    if indices
        .iter()
        .skip(1)
        .any(|index| index.schema() != target_schema)
    {
        return Err(crate::TantivyError::InvalidArgument(
            "Attempt to merge different schema indices".to_string(),
        ));
    }

    let mut segments: Vec<Segment> = Vec::new();
    for index in indices {
        segments.extend(index.searchable_segments()?);
    }

    let mut merged_index = Index::create(output_directory, target_schema.clone())?;
    let merged_segment = merged_index.new_segment();
    let merged_segment_id = merged_segment.id();
    let merger: IndexMerger = IndexMerger::open(merged_index.schema(), &segments[..])?;
    let segment_serializer = SegmentSerializer::for_segment(merged_segment)?;
    let num_docs = merger.write(segment_serializer)?;

    let segment_meta = merged_index.new_segment_meta(merged_segment_id, num_docs);

    let stats = format!(
        "Segments Merge: [{}]",
        segments
            .iter()
            .fold(String::new(), |sum, current| format!(
                "{}{} ",
                sum,
                current.meta().id().uuid_string()
            ))
            .trim_end()
    );

    let index_meta = IndexMeta {
        segments: vec![segment_meta],
        schema: target_schema,
        opstamp: 0u64,
        payload: Some(stats),
    };

    // save the meta.json
    save_metas(&index_meta, merged_index.directory_mut())?;

    Ok(merged_index)
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
    merge_policy: RwLock<Arc<dyn MergePolicy>>,
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
            merge_policy: RwLock::new(Arc::new(DefaultMergePolicy::default())),
            killed: AtomicBool::new(false),
            stamper,
            merge_operations: Default::default(),
        })))
    }

    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.merge_policy.read().unwrap().clone()
    }

    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        let arc_merge_policy = Arc::from(merge_policy);
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
            let segment = self.index.segment(segment_entry.meta().clone());
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
    ) -> impl Future<Output = crate::Result<()>> {
        let segment_updater: SegmentUpdater = self.clone();
        self.schedule_future(async move {
            let segment_entries = segment_updater.purge_deletes(opstamp)?;
            segment_updater.segment_manager.commit(segment_entries);
            segment_updater.save_metas(opstamp, payload)?;
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
            });
        merge_candidates.extend(committed_merge_candidates);

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
                let mut delete_cursor = after_merge_segment_entry.delete_cursor().clone();
                if let Some(delete_operation) = delete_cursor.get() {
                    let committed_opstamp = segment_updater.load_metas().opstamp;
                    if delete_operation.opstamp < committed_opstamp {
                        let index = &segment_updater.index;
                        let segment = index.segment(after_merge_segment_entry.meta().clone());
                        if let Err(advance_deletes_err) = advance_deletes(
                            segment,
                            &mut after_merge_segment_entry,
                            committed_opstamp,
                        ) {
                            error!(
                                "Merge of {:?} was cancelled (advancing deletes failed): {:?}",
                                merge_operation.segment_ids(),
                                advance_deletes_err
                            );
                            if cfg!(test) {
                                panic!("Merge failed.");
                            }
                            // ... cancel merge
                            // `merge_operations` are tracked. As it is dropped, the
                            // the segment_ids will be available again for merge.
                            return Err(advance_deletes_err);
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
    ///
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
    use super::merge_segments;
    use crate::directory::RAMDirectory;
    use crate::indexer::merge_policy::tests::MergeWheneverPossible;
    use crate::schema::*;
    use crate::Index;

    #[test]
    fn test_delete_during_merge() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        // writing the segment
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(MergeWheneverPossible));

        for _ in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"));
            index_writer.add_document(doc!(text_field=>"b"));
        }
        index_writer.commit()?;

        for _ in 0..100 {
            index_writer.add_document(doc!(text_field=>"c"));
            index_writer.add_document(doc!(text_field=>"d"));
        }
        index_writer.commit()?;

        index_writer.add_document(doc!(text_field=>"e"));
        index_writer.add_document(doc!(text_field=>"f"));
        index_writer.commit()?;

        let term = Term::from_field_text(text_field, "a");
        index_writer.delete_term(term);
        index_writer.commit()?;

        let reader = index.reader()?;
        assert_eq!(reader.searcher().num_docs(), 302);

        index_writer.wait_merging_threads()?;

        reader.reload()?;
        assert_eq!(reader.searcher().segment_readers().len(), 1);
        assert_eq!(reader.searcher().num_docs(), 302);
        Ok(())
    }

    #[test]
    fn delete_all_docs() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        // writing the segment
        let mut index_writer = index.writer_for_tests()?;

        for _ in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"));
            index_writer.add_document(doc!(text_field=>"b"));
        }
        index_writer.commit()?;

        for _ in 0..100 {
            index_writer.add_document(doc!(text_field=>"c"));
            index_writer.add_document(doc!(text_field=>"d"));
        }
        index_writer.commit()?;

        index_writer.add_document(doc!(text_field=>"e"));
        index_writer.add_document(doc!(text_field=>"f"));
        index_writer.commit()?;

        let seg_ids = index.searchable_segment_ids()?;
        // docs exist, should have at least 1 segment
        assert!(seg_ids.len() > 0);

        let term_vals = vec!["a", "b", "c", "d", "e", "f"];
        for term_val in term_vals {
            let term = Term::from_field_text(text_field, term_val);
            index_writer.delete_term(term);
            index_writer.commit()?;
        }

        index_writer.wait_merging_threads()?;

        let reader = index.reader()?;
        assert_eq!(reader.searcher().num_docs(), 0);

        let seg_ids = index.searchable_segment_ids()?;
        assert!(seg_ids.is_empty());

        reader.reload()?;
        assert_eq!(reader.searcher().num_docs(), 0);
        // empty segments should be erased
        assert!(index.searchable_segment_metas()?.is_empty());
        assert!(reader.searcher().segment_readers().is_empty());

        Ok(())
    }

    #[test]
    fn test_remove_all_segments() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        // writing the segment
        let mut index_writer = index.writer_for_tests()?;
        for _ in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"));
            index_writer.add_document(doc!(text_field=>"b"));
        }
        index_writer.commit()?;

        index_writer.segment_updater().remove_all_segments();
        let seg_vec = index_writer
            .segment_updater()
            .segment_manager
            .segment_entries();
        assert!(seg_vec.is_empty());
        Ok(())
    }

    #[test]
    fn test_merge_segments() -> crate::Result<()> {
        let mut indices = vec![];
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        for _ in 0..3 {
            let index = Index::create_in_ram(schema.clone());

            // writing two segments
            let mut index_writer = index.writer_for_tests()?;
            for _ in 0..100 {
                index_writer.add_document(doc!(text_field=>"fizz"));
                index_writer.add_document(doc!(text_field=>"buzz"));
            }
            index_writer.commit()?;

            for _ in 0..1000 {
                index_writer.add_document(doc!(text_field=>"foo"));
                index_writer.add_document(doc!(text_field=>"bar"));
            }
            index_writer.commit()?;
            indices.push(index);
        }

        assert_eq!(indices.len(), 3);
        let output_directory = RAMDirectory::default();
        let index = merge_segments(&indices, output_directory)?;
        assert_eq!(index.schema(), schema);

        let segments = index.searchable_segments()?;
        assert_eq!(segments.len(), 1);

        let segment_metas = segments[0].meta();
        assert_eq!(segment_metas.num_deleted_docs(), 0);
        assert_eq!(segment_metas.num_docs(), 6600);
        Ok(())
    }

    #[test]
    fn test_merge_empty_indices_array() {
        let merge_result = merge_segments(&[], RAMDirectory::default());
        assert!(merge_result.is_err());
    }

    #[test]
    fn test_merge_mismatched_schema() -> crate::Result<()> {
        let first_index = {
            let mut schema_builder = Schema::builder();
            let text_field = schema_builder.add_text_field("text", TEXT);
            let index = Index::create_in_ram(schema_builder.build());
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"some text"));
            index_writer.commit()?;
            index
        };

        let second_index = {
            let mut schema_builder = Schema::builder();
            let body_field = schema_builder.add_text_field("body", TEXT);
            let index = Index::create_in_ram(schema_builder.build());
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(body_field=>"some body"));
            index_writer.commit()?;
            index
        };

        // mismatched schema index list
        let result = merge_segments(&[first_index, second_index], RAMDirectory::default());
        assert!(result.is_err());

        Ok(())
    }
}
