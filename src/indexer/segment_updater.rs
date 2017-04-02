#![allow(for_kv_map)]

use core::Index;
use core::IndexMeta;
use core::META_FILEPATH;
use core::Segment;
use core::SegmentId;
use core::SegmentMeta;
use core::SerializableSegment;
use directory::Directory;
use indexer::stamper::Stamper;
use Error;
use futures_cpupool::CpuPool;
use futures::Future;
use futures::Canceled;
use futures::oneshot;
use indexer::{MergePolicy, DefaultMergePolicy};
use indexer::index_writer::advance_deletes;
use indexer::MergeCandidate;
use indexer::merger::IndexMerger;
use indexer::SegmentEntry;
use indexer::SegmentSerializer;
use Result;
use futures_cpupool::CpuFuture;
use rustc_serialize::json;
use indexer::delete_queue::DeleteCursor;
use schema::Schema;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io::Write;
use std::mem;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::thread;
use std::thread::JoinHandle;
use super::segment_manager::{SegmentManager, get_mergeable_segments};


/// Save the index meta file.
/// This operation is atomic :
/// Either
//  - it fails, in which case an error is returned,
/// and the `meta.json` remains untouched,
/// - it success, and `meta.json` is written
/// and flushed.
///
/// This method is not part of tantivy's public API
pub fn save_new_metas(schema: Schema,
                  opstamp: u64,
                  directory: &mut Directory)
                  -> Result<()> {
    save_metas(vec!(), schema, opstamp, directory)
}



/// Save the index meta file.
/// This operation is atomic :
/// Either
//  - it fails, in which case an error is returned,
/// and the `meta.json` remains untouched,
/// - it success, and `meta.json` is written
/// and flushed.
///
/// This method is not part of tantivy's public API
pub fn save_metas(segment_metas: Vec<SegmentMeta>,
                  schema: Schema,
                  opstamp: u64,
                  directory: &mut Directory)
                  -> Result<()> {
    let metas = IndexMeta {
        segments: segment_metas,
        schema: schema,
        opstamp: opstamp,
    };
    let mut w = vec!();
    try!(write!(&mut w, "{}\n", json::as_pretty_json(&metas)));
    let res = directory.atomic_write(&META_FILEPATH, &w[..])?;
    debug!("Saved metas {}", json::as_pretty_json(&metas));
    Ok(res)
        
}


// The segment update runner is in charge of processing all
//  of the `SegmentUpdate`s.
//
// All this processing happens on a single thread
// consuming a common queue. 
#[derive(Clone)]
pub struct SegmentUpdater(Arc<InnerSegmentUpdater>);



fn perform_merge(segment_ids: &[SegmentId],
                 segment_updater: &SegmentUpdater,
                 mut merged_segment: Segment,
                 target_opstamp: u64) -> Result<SegmentEntry> {
    // first we need to apply deletes to our segment.
    info!("Start merge: {:?}", segment_ids);
    
    let ref index = segment_updater.0.index;
    let schema = index.schema();
    let mut segment_entries = vec!();
    for segment_id in segment_ids {
        if let Some(mut segment_entry) = segment_updater.0
            .segment_manager
            .segment_entry(segment_id) {
            let segment = index.segment(segment_entry.meta().clone());
            advance_deletes(segment, &mut segment_entry, target_opstamp)?;
            segment_entries.push(segment_entry);
        }
        else {
            error!("Error, had to abort merge as some of the segment is not managed anymore.a");
            return Err(Error::InvalidArgument(format!("Segment {:?} requested for merge is not managed.", segment_id)));
        }
    }
    
    // TODO REMOVEEEEE THIIIIIS
    {
    let living_files = segment_updater.0.segment_manager.list_files();
    let mut index = merged_segment.index().clone();
    index.directory_mut().garbage_collect(living_files);
    }


    let delete_cursor = segment_entries[0].delete_cursor().clone();

    let segments: Vec<Segment> = segment_entries
        .iter()
        .map(|segment_entry| {
            index.segment(segment_entry.meta().clone())
        })
        .collect();
    
    // An IndexMerger is like a "view" of our merged segments.
    let merger: IndexMerger = IndexMerger::open(schema, &segments[..])?;
    
    // ... we just serialize this index merger in our new segment
    // to merge the two segments.

    let segment_serializer =
        SegmentSerializer::for_segment(&mut merged_segment)
        .expect("Creating index serializer failed");

    let num_docs = merger.write(segment_serializer).expect("Serializing merged index failed");
    let mut segment_meta = SegmentMeta::new(merged_segment.id());
    segment_meta.set_max_doc(num_docs);
    
    let after_merge_segment_entry = SegmentEntry::new(segment_meta.clone(), delete_cursor, None);
    Ok(after_merge_segment_entry)
}


struct InnerSegmentUpdater {
    pool: CpuPool,
    index: Index,
    segment_manager: SegmentManager,
    merge_policy: RwLock<Box<MergePolicy>>,
    merging_thread_id: AtomicUsize,
    merging_threads: RwLock<HashMap<usize, JoinHandle<Result<()>>>>,
    generation: AtomicUsize,
    killed: AtomicBool, 
    stamper: Stamper,
}

impl SegmentUpdater {

    pub fn new(index: Index,
               stamper: Stamper,
               delete_cursor: DeleteCursor) -> Result<SegmentUpdater> {
        let segments = index.segments()?;
        let segment_manager = SegmentManager::from_segments(segments, delete_cursor);
        Ok(
            SegmentUpdater(Arc::new(InnerSegmentUpdater {
                pool: CpuPool::new(1),
                index: index,
                segment_manager: segment_manager,
                merge_policy: RwLock::new(box DefaultMergePolicy::default()),
                merging_thread_id: AtomicUsize::default(),
                merging_threads: RwLock::new(HashMap::new()),
                generation: AtomicUsize::default(),
                killed: AtomicBool::new(false),
                stamper: stamper,
            }))
        )
    }

    pub fn new_segment(&self) -> Segment {
        let new_segment = self.0.index.new_segment();
        let segment_id = new_segment.id();
        self.0.segment_manager.write_segment(segment_id);
        new_segment
    }

    pub fn get_merge_policy(&self) -> Box<MergePolicy> {
        self.0.merge_policy.read().unwrap().box_clone()
    }

    pub fn set_merge_policy(&self, merge_policy: Box<MergePolicy>) {
        *self.0.merge_policy.write().unwrap()= merge_policy;
    }

    fn get_merging_thread_id(&self) -> usize {
        self.0.merging_thread_id.fetch_add(1, Ordering::SeqCst)
    }

    fn run_async<T: 'static + Send, F: 'static + Send + FnOnce(SegmentUpdater) -> T>(&self, f: F) -> CpuFuture<T, Error> {
        let me_clone = self.clone();
        self.0.pool.spawn_fn(move || {
            Ok(f(me_clone))
        })
    }


    pub fn add_segment(&self, generation: usize, segment_entry: SegmentEntry) -> bool {
        if generation >= self.0.generation.load(Ordering::Acquire) {
            self.run_async(|segment_updater| {
                segment_updater.0.segment_manager.add_segment(segment_entry);
                segment_updater.consider_merge_options();
                true
            }).forget();
            true
        }
        else {
            false
        }
    }

    pub fn kill(&mut self,) {
        self.0.killed.store(true, Ordering::Release);
    }

    fn is_alive(&self,) -> bool {
        !self.0.killed.load(Ordering::Acquire)
    }

    fn purge_deletes(&self, target_opstamp: u64) -> Result<Vec<SegmentEntry>> {
        let mut segment_entries = self.0.segment_manager.segment_entries(); 
        for segment_entry in &mut segment_entries {
            let segment = self.0.index.segment(segment_entry.meta().clone());
            advance_deletes(segment, segment_entry, target_opstamp)?;
        }
        Ok(segment_entries)
        
    }

    pub fn save_metas(&self, opstamp: u64) {
        if self.is_alive() {
            let index = &self.0.index;
            let directory = index.directory();
            save_metas(
                self.0.segment_manager.committed_segment_metas(),
                index.schema(),
                opstamp,
                directory.box_clone().borrow_mut()).expect("Could not save metas.");
        }
    }

    pub fn commit(&self, opstamp: u64) -> Result<()> {
        self.run_async(move |segment_updater| {
            let mut index = segment_updater.0.index.clone();

            if segment_updater.is_alive() {
                let segment_entries = segment_updater
                    .purge_deletes(opstamp)
                    .expect("Failed purge deletes");
                segment_updater.0.segment_manager.commit(segment_entries);
                segment_updater.save_metas(opstamp);
                let living_files = segment_updater.0.segment_manager.list_files();
                index.directory_mut().garbage_collect(living_files);
                segment_updater.consider_merge_options();
            }
        }).wait()
    }


    pub fn start_merge(&self, segment_ids: &[SegmentId]) -> impl Future<Item=SegmentMeta, Error=Canceled> {
        
        self.0.segment_manager.start_merge(segment_ids);
        let segment_updater_clone = self.clone();
        
        let segment_ids_vec = segment_ids.to_vec(); 
        
        let merging_thread_id = self.get_merging_thread_id();
        let (merging_future_send, merging_future_recv) = oneshot();
        
        if segment_ids.is_empty() {
            return merging_future_recv;
        }
        
        let target_opstamp = self.0.stamper.stamp();
        let merging_join_handle = thread::spawn(move || {
            
            // first we need to apply deletes to our segment.
            info!("Start merge: {:?}", segment_ids_vec);
            
            let merged_segment = segment_updater_clone.new_segment(); 
            let merged_segment_id = merged_segment.id();
            let merge_result = perform_merge(&segment_ids_vec, &segment_updater_clone, merged_segment, target_opstamp);

            match merge_result {
                Ok(after_merge_segment_entry) => {
                    let merged_segment_meta = after_merge_segment_entry.meta().clone();
                    segment_updater_clone
                        .end_merge(segment_ids_vec, after_merge_segment_entry)
                        .expect("Segment updater thread is corrupted.");
                    
                    // the future may fail if the listener of the oneshot future 
                    // has been destroyed.
                    //
                    // This is not a problem here, so we just ignore any 
                    // possible error.
                    let _merging_future_res = merging_future_send.send(merged_segment_meta);
                }
                Err(e) => {
                    // ... cancel merge
                    if cfg!(test) {
                        panic!("Merge failed.");
                    }
                    else {
                        error!("Merge of {:?} was cancelled: {:?}", segment_ids_vec, e);
                    }
                    segment_updater_clone.cancel_merge(&segment_ids_vec, merged_segment_id);
                    // merging_future_send will be dropped, sending an error to the future.
                }
            }
            segment_updater_clone.0.merging_threads.write().unwrap().remove(&merging_thread_id);
            Ok(())
        });
        self.0.merging_threads.write().unwrap().insert(merging_thread_id, merging_join_handle);
        merging_future_recv
    }


    fn consider_merge_options(&self) {
        let (committed_segments, uncommitted_segments) = get_mergeable_segments(&self.0.segment_manager);
        // Committed segments cannot be merged with uncommitted_segments.
        // We therefore consider merges using these two sets of segments independently.
        let merge_policy = self.get_merge_policy();
        let mut merge_candidates = merge_policy.compute_merge_candidates(&uncommitted_segments);
        let committed_merge_candidates = merge_policy.compute_merge_candidates(&committed_segments);
        merge_candidates.extend_from_slice(&committed_merge_candidates[..]);
        for MergeCandidate(segment_metas) in merge_candidates {
            self.start_merge(&segment_metas);
        }
    }

    fn cancel_merge(&self, 
        before_merge_segment_ids: &[SegmentId],
        after_merge_segment_entry: SegmentId)  {
        self.0.segment_manager.cancel_merge(&before_merge_segment_ids, after_merge_segment_entry);
    }

    
    fn end_merge(&self, 
        before_merge_segment_ids: Vec<SegmentId>,
        mut after_merge_segment_entry: SegmentEntry) -> Result<()> {
        
        self.run_async(move |segment_updater| {
            debug!("End merge {:?}", after_merge_segment_entry.meta());
            let mut delete_cursor = after_merge_segment_entry.delete_cursor().clone();
            if let Some(delete_operation) = delete_cursor.get() {
                let committed_opstamp = segment_updater.0.index.opstamp();
                if delete_operation.opstamp < committed_opstamp {
                    let segment = segment_updater.0.index.segment(after_merge_segment_entry.meta().clone());
                    // TODO check unwrap
                    advance_deletes(segment, &mut after_merge_segment_entry, committed_opstamp).unwrap();
                }
            }
            segment_updater.0.segment_manager.end_merge(&before_merge_segment_ids, after_merge_segment_entry);
            segment_updater.save_metas(segment_updater.0.index.opstamp());
        }).wait()
    }

    pub fn wait_merging_thread(&self) -> Result<()> {
        let mut new_merging_threads = HashMap::new();
        {
            let mut merging_threads = self.0.merging_threads.write().unwrap();
            mem::swap(&mut new_merging_threads, merging_threads.deref_mut());
        }
        debug!("wait merging thread {}", new_merging_threads.len());
        for (_, merging_thread_handle) in new_merging_threads {
            merging_thread_handle
                .join()
                .map(|_| ())
                .map_err(|_| {
                    Error::ErrorInThread("Merging thread failed.".to_string())
                })?
        }
        // Our merging thread may have queued their completed
        self.run_async(move |_| {}).wait()
    }

}




#[cfg(test)]
mod tests {

    use Index;
    use schema::*;
    use indexer::merge_policy::tests::MergeWheneverPossible;

    #[test]
    fn test_delete_during_merge() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        
        // writing the segment
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        index_writer.set_merge_policy(box MergeWheneverPossible);

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

        index.load_searchers().unwrap();
        assert_eq!(index.searcher().segment_readers().len(), 2);
        assert_eq!(index.searcher().num_docs(), 302);

        {
            index_writer.wait_merging_threads()
                        .expect(    "waiting for merging threads");
        }

        index.load_searchers().unwrap();
        assert_eq!(index.searcher().segment_readers().len(), 1);
        assert_eq!(index.searcher().num_docs(), 302);
    }
}
