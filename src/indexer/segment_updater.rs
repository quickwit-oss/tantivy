#![allow(for_kv_map)]

use core::Index;
use Error;
use core::Segment;
use indexer::{MergePolicy, DefaultMergePolicy};
use core::SegmentId;
use core::SegmentMeta;
use std::mem;
use std::sync::atomic::Ordering;
use std::ops::DerefMut;
use futures::{Future, future};
use fastfield::delete::write_delete_bitset;
use futures::oneshot;
use futures::Canceled;
use std::thread;
use core::SegmentComponent;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;
use core::SerializableSegment;
use indexer::MergeCandidate;
use indexer::merger::IndexMerger;
use std::borrow::BorrowMut;
use indexer::SegmentSerializer;
use indexer::SegmentEntry;
use schema::Schema;
use indexer::index_writer::{advance_deletes, DocToOpstampMapping};
use directory::Directory;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::collections::HashMap;
use rustc_serialize::json;
use indexer::delete_queue::{DeleteQueueCursor, DeleteQueue};
use Result;
use futures_cpupool::CpuPool;
use core::IndexMeta;
use core::META_FILEPATH;
use std::io::Write;
use super::segment_manager::{SegmentManager, get_segments};


fn create_metas(segment_manager: &SegmentManager, schema: Schema, opstamp: u64) -> IndexMeta {
    let (committed_segments, uncommitted_segments) = segment_manager.segment_metas();
    IndexMeta {
        committed_segments: committed_segments,
        uncommitted_segments: uncommitted_segments,
        schema: schema,
        opstamp: opstamp,
    }
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
pub fn save_new_metas(schema: Schema,
                  opstamp: u64,
                  directory: &mut Directory)
                  -> Result<()> {
    let segment_manager = SegmentManager::default();
    save_metas(&segment_manager, schema, opstamp, directory)
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
pub fn save_metas(segment_manager: &SegmentManager,
                  schema: Schema,
                  opstamp: u64,
                  directory: &mut Directory)
                  -> Result<()> {
    let metas = create_metas(segment_manager, schema, opstamp);
    let mut w = Vec::new();
    try!(write!(&mut w, "{}\n", json::as_pretty_json(&metas)));
    Ok(directory
        .atomic_write(&META_FILEPATH, &w[..])?)
        
}



// The segment update runner is in charge of processing all
//  of the `SegmentUpdate`s.
//
// All this processing happens on a single thread
// consuming a common queue. 
#[derive(Clone)]
pub struct SegmentUpdater(Arc<InnerSegmentUpdater>);


struct InnerSegmentUpdater {
    pool: CpuPool,
    index: Index,
    segment_manager: SegmentManager,
    merge_policy: RwLock<Box<MergePolicy>>,
    merging_thread_id: AtomicUsize,
    merging_threads: RwLock<HashMap<usize, JoinHandle<Result<SegmentEntry>>>>,
    generation: AtomicUsize,
}

impl SegmentUpdater {

    pub fn new(
            index: Index,
            delete_cursor: DeleteQueueCursor)
            -> Result<SegmentUpdater>
    {   
        let committed_segments = index.committed_segments()?;
        let segment_manager = SegmentManager::from_segments(committed_segments, delete_cursor);
        Ok(
            SegmentUpdater(Arc::new(InnerSegmentUpdater {
                pool: CpuPool::new(1),
                index: index,
                segment_manager: segment_manager,
                merge_policy: RwLock::new(box DefaultMergePolicy::default()),
                merging_thread_id: AtomicUsize::default(),
                merging_threads: RwLock::new(HashMap::new()),
                generation: AtomicUsize::default(),
            }))
        )
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


    fn run_async<T: 'static + Send, F: 'static + Send + FnOnce(SegmentUpdater) -> T>(&self, f: F) -> impl Future<Item=T, Error=&'static str> {
        let me_clone = self.clone();
        self.0.pool.spawn_fn(move || {
            Ok(f(me_clone))
        })
    }

    pub fn new_generation(&mut self, generation: usize) -> impl Future<Item=(), Error=&'static str> {
        self.0.generation.store(generation, Ordering::Release);
        self.run_async(|segment_updater| {
            segment_updater.0.segment_manager.rollback();
        })
    }

    pub fn add_segment(&self, generation: usize, segment_entry: SegmentEntry) -> impl Future<Item=bool, Error=&'static str> {
        if generation >= self.0.generation.load(Ordering::Acquire) {
            future::Either::A(self.run_async(|segment_updater| {
                segment_updater.0.segment_manager.add_segment(segment_entry);
                segment_updater.consider_merge_options();
                true
            }))
        }
        else {
            future::Either::B(future::ok(false))
        }
    }

    fn purge_deletes(&self, target_opstamp: u64) -> Result<()> {
        let uncommitted = self.0.segment_manager.segment_entries();
        for mut segment_entry in uncommitted {
            let mut segment = self.0.index.segment(segment_entry.meta().segment_id, segment_entry.meta().opstamp); 
            let (_, deleted_docset) = advance_deletes(
                 &segment,
                 segment_entry.delete_cursor(),
                 DocToOpstampMapping::None).unwrap();
            {
                let mut delete_file = segment.with_opstamp(target_opstamp).open_write(SegmentComponent::DELETE)?;
                write_delete_bitset(&deleted_docset, &mut delete_file)?;
            }
            
        }
        Ok(())
    }

    pub fn commit(&self, opstamp: u64) -> impl Future<Item=(), Error=&'static str> {
        self.run_async(move |segment_updater| {
            segment_updater.purge_deletes(opstamp).expect("Failed purge deletes");
            segment_updater.0.segment_manager.commit(opstamp);
            let mut directory = segment_updater.0.index.directory().box_clone();
            save_metas(
                    &segment_updater.0.segment_manager,
                    segment_updater.0.index.schema(),
                    opstamp,
                    directory.borrow_mut()).expect("Could not save metas.");
            segment_updater.consider_merge_options();
        })
    }


    pub fn start_merge(&self, segment_ids: &[SegmentId]) -> impl Future<Item=SegmentEntry, Error=Canceled> {
        
        self.0.segment_manager.start_merge(segment_ids);
        let segment_updater_clone = self.clone();
        
        let segment_ids_vec = segment_ids.to_vec(); 
        
        let merging_thread_id = self.get_merging_thread_id();
        let (merging_future_send, merging_future_recv) = oneshot();
        
        if segment_ids.is_empty() {
            return merging_future_recv;
        }
        
        let merging_join_handle = thread::spawn(move || {
            
            info!("Start merge: {:?}", segment_ids_vec);
            
            let ref index = segment_updater_clone.0.index;
            let schema = index.schema();
            let segment_metas: Vec<SegmentMeta> = segment_ids_vec
                .iter()
                .map(|segment_id| 
                    segment_updater_clone.0.segment_manager
                        .segment_entry(segment_id)
                        .map(|segment_entry| segment_entry.meta().clone())
                        .ok_or(Error::InvalidArgument(format!("Segment({:?}) does not exist anymore", segment_id)))
                )
                .collect::<Result<_>>()?;
            
            let segments: Vec<Segment> = segment_metas
                .iter()
                .map(|ref segment_metas| index.segment(segment_metas.segment_id, segment_metas.opstamp))
                .collect();
            
            // An IndexMerger is like a "view" of our merged segments. 
            // TODO unwrap
            let merger: IndexMerger = IndexMerger::open(schema, &segments[..]).expect("Creating index merger failed");
            
            let opstamp = segment_metas
                .iter()
                .map(|meta| meta.opstamp)
                .max()
                .unwrap();
            
            let mut merged_segment = index.new_segment(opstamp); 
            
            // ... we just serialize this index merger in our new segment
            // to merge the two segments.
            let segment_serializer = SegmentSerializer::for_segment(&mut merged_segment).expect("Creating index serializer failed");
            let num_docs = merger.write(segment_serializer).expect("Serializing merged index failed");
            let segment_meta = SegmentMeta {
                segment_id: merged_segment.id(),
                num_docs: num_docs,
                num_deleted_docs: 0u32,
                opstamp: opstamp,
            };
            
            // TODO fix delete cursor
            let delete_queue = DeleteQueue::default();
            
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor());
            segment_updater_clone
                .end_merge(segment_metas.clone(), segment_entry.clone())
                .wait()
                .unwrap();
            merging_future_send.complete(segment_entry.clone());
            segment_updater_clone.0.merging_threads.write().unwrap().remove(&merging_thread_id);
            Ok(segment_entry)
        });
        self.0.merging_threads.write().unwrap().insert(merging_thread_id, merging_join_handle);
        merging_future_recv
    }


    fn consider_merge_options(&self) {
        let (committed_segments, uncommitted_segments) = get_segments(&self.0.segment_manager);
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

    
    fn end_merge(&self, 
        merged_segment_metas: Vec<SegmentMeta>,
        resulting_segment_entry: SegmentEntry) -> impl Future<Item=(), Error=&'static str> {
        
        self.run_async(move |segment_updater| {
            segment_updater.0.segment_manager.end_merge(&merged_segment_metas, resulting_segment_entry);
            let mut directory = segment_updater.0.index.directory().box_clone();
            save_metas(
                &segment_updater.0.segment_manager,
                segment_updater.0.index.schema(),
                segment_updater.0.index.opstamp(),
                directory.borrow_mut()).expect("Could not save metas.");
            for segment_meta in merged_segment_metas {
                segment_updater.0.index.delete_segment(segment_meta.segment_id);
            }
        })
        
    }

    pub fn wait_merging_thread(&self) -> thread::Result<()> {
        let mut new_merging_threads = HashMap::new();
        {
            let mut merging_threads = self.0.merging_threads.write().unwrap();
            mem::swap(&mut new_merging_threads, merging_threads.deref_mut());
        }
        for (_, merging_thread_handle) in new_merging_threads {
            merging_thread_handle
                .join()
                .map(|_| ())?
        }
        Ok(())
    }

}
