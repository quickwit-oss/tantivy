#![allow(for_kv_map)]

use chan;
use core::Index;
use std::sync::Mutex;
use core::Segment;
use core::SegmentId;
use core::SegmentMeta;
use std::mem;
use core::SerializableSegment;
use indexer::MergePolicy;
use indexer::MergeCandidate;
use indexer::merger::IndexMerger;
use indexer::SegmentSerializer;
use std::thread;
use schema::Schema;
use directory::Directory;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::collections::HashMap;
use rustc_serialize::json;
use Result;
use core::IndexMeta;
use core::META_FILEPATH;
use std::io::Write;
use super::segment_manager::{SegmentManager, get_segment_ready_for_commit};
use super::super::core::index::get_segment_manager;

pub type SegmentUpdateSender = chan::Sender<SegmentUpdate>;
pub type SegmentUpdateReceiver = chan::Receiver<SegmentUpdate>;


fn create_metas(segment_manager: &SegmentManager, schema: Schema, docstamp: u64) -> IndexMeta {
    let (committed_segments, uncommitted_segments) = segment_manager.segment_metas();
    IndexMeta {
        committed_segments: committed_segments,
        uncommitted_segments: uncommitted_segments,
        schema: schema,
        docstamp: docstamp,
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
                  docstamp: u64,
                  directory: &mut Directory)
                  -> Result<()> {
    let segment_manager = SegmentManager::from_segments(Vec::new());
    save_metas(&segment_manager, schema, docstamp, directory)
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
                  docstamp: u64,
                  directory: &mut Directory)
                  -> Result<()> {
    let metas = create_metas(segment_manager, schema, docstamp);
    let mut w = Vec::new();
    try!(write!(&mut w, "{}\n", json::as_pretty_json(&metas)));
    directory.atomic_write(&META_FILEPATH, &w[..])
        .map_err(From::from)
}


#[derive(Debug, Clone)]
pub enum SegmentUpdate {
    
    /// New segment added.
    /// Created by the indexing worker thread 
    AddSegment(SegmentMeta),
    
    /// A merge is ended.
    /// Remove the merged segment and record the new 
    /// large merged segment.
    EndMerge(usize, Vec<SegmentId>, SegmentMeta),
    
    /// Happens when rollback is called.
    /// The current generation of segments is cancelled.
    CancelGeneration,
    
    /// Starts a new generation... This 
    /// happens at the end of Rollback.
    NewGeneration,
    
    /// Just dropping the Segment updater object
    /// is safe, but some merge might be happening in 
    /// the background and the user may want to wait for these 
    /// threads to terminate.
    /// 
    /// When receiving the Terminate signal, the segment updater stops
    /// receiving segment updates and just waits for the merging threads
    /// to terminate.
	Terminate,
    
    /// Commit marks uncommmitted segments as committed.
	Commit(u64),
}





/// The segment updater is in charge of processing all of the 
/// `SegmentUpdate`s.
///
/// All this processing happens on a single thread
/// consuming a common queue. 
/// 
/// The segment updates producers are :
/// - indexing threads are sending new segments 
/// - merging threads are sending merge operations
/// - the index writer sends "terminate"
pub struct SegmentUpdater {
	index: Index,
	is_cancelled_generation: bool,
	segment_update_receiver: SegmentUpdateReceiver,
	segment_update_sender: SegmentUpdateSender,
    segment_manager_arc: Arc<SegmentManager>,
    merge_policy: Arc<Mutex<Box<MergePolicy>>>,
    merging_thread_id: usize,
    merging_threads: HashMap<usize, JoinHandle<(Vec<SegmentId>, SegmentMeta)> >, 
}


impl SegmentUpdater {
	    
    pub fn start_updater(index: Index, merge_policy: Arc<Mutex<Box<MergePolicy>>>) -> (SegmentUpdateSender, JoinHandle<()>) {
        let segment_updater = SegmentUpdater::new(index, merge_policy);
        (segment_updater.segment_update_sender.clone(), segment_updater.start())
    }
    
    fn new(index: Index, merge_policy: Arc<Mutex<Box<MergePolicy>>>) -> SegmentUpdater {
        let segment_manager_arc = get_segment_manager(&index);
        let (segment_update_sender, segment_update_receiver): (SegmentUpdateSender, SegmentUpdateReceiver) = chan::async();
		SegmentUpdater {
			index: index,
            is_cancelled_generation: false,
            segment_update_sender: segment_update_sender,
            segment_update_receiver: segment_update_receiver,
            segment_manager_arc: segment_manager_arc,
            merge_policy: merge_policy,
            merging_thread_id: 0,
            merging_threads: HashMap::new(), 
		}
	}
    
    fn new_merging_thread_id(&mut self,) -> usize {
        self.merging_thread_id += 1;
        self.merging_thread_id
    }
    
    
    fn end_merge(
        &mut self,
        segment_ids: Vec<SegmentId>,
        segment_meta: SegmentMeta) {
        
        let segment_manager = self.segment_manager_arc.clone();
        segment_manager.end_merge(&segment_ids, &segment_meta);
        save_metas(
            &*segment_manager,
            self.index.schema(),
            self.index.docstamp(),
            self.index.directory_mut()).expect("Could not save metas.");
        for segment_id in segment_ids {
            self.index.delete_segment(segment_id);
        }
    }


    fn start_merges(&mut self,) {
        
        let merge_candidates = self.consider_merge_options();
        
        for MergeCandidate(segment_ids) in merge_candidates {
            
            let merging_thread_id = self.new_merging_thread_id();
            
            self.segment_manager().start_merge(&segment_ids);

            let index_clone = self.index.clone();
            let segment_update_sender_clone = self.segment_update_sender.clone();
            
            let merge_thread_handle = thread::Builder::new()
                .name(format!("merge_thread_{:?}", merging_thread_id))
                .spawn(move || {
                    info!("Start merge: {:?}", segment_ids);
                    let schema = index_clone.schema();
                    let segments: Vec<Segment> = segment_ids
                        .iter()
                        .map(|&segment_id| index_clone.segment(segment_id))
                        .collect();
                    // An IndexMerger is like a "view" of our merged segments. 
                    // TODO unwrap
                    let merger: IndexMerger = IndexMerger::open(schema, &segments[..]).expect("Creating index merger failed");
                    let mut merged_segment = index_clone.new_segment();
                    // ... we just serialize this index merger in our new segment
                    // to merge the two segments.
                    let segment_serializer = SegmentSerializer::for_segment(&mut merged_segment).expect("Creating index serializer failed");
                    let num_docs = merger.write(segment_serializer).expect("Serializing merged index failed");
                    let segment_meta = SegmentMeta {
                        segment_id: merged_segment.id(),
                        num_docs: num_docs,
                        num_deleted_docs: 0u32,
                    };
                    let segment_update = SegmentUpdate::EndMerge(merging_thread_id, segment_ids.clone(), segment_meta.clone());
                    segment_update_sender_clone.send(segment_update.clone());
                    (segment_ids, segment_meta)
                })
                .expect("Failed to spawn merge thread");
            
            self.merging_threads.insert(merging_thread_id, merge_thread_handle);
        }
    }
    
    fn consider_merge_options(&self,) -> Vec<MergeCandidate> {
        let segment_manager = self.segment_manager();
        let (committed_segments, uncommitted_segments) = get_segment_ready_for_commit(segment_manager);
        // Committed segments cannot be merged with uncommitted_segments.
        // We therefore consider merges using these two sets of segments independantly.
        let merge_policy_lock = self.merge_policy.lock().unwrap();
        let mut merge_candidates = merge_policy_lock.compute_merge_candidates(&uncommitted_segments);
        let committed_merge_candidates = merge_policy_lock.compute_merge_candidates(&committed_segments);
        merge_candidates.extend_from_slice(&committed_merge_candidates[..]);
        merge_candidates
    }
    
    
	fn segment_manager(&self,) -> &SegmentManager {
		&*self.segment_manager_arc
	}
    
    pub fn start(self,) -> JoinHandle<()> {
		thread::Builder::new()
            .name("segment_update".to_string())
            .spawn(move || {
                self.process();
		    })
            .expect("Failed to start segment updater thread.")
    }
    
	fn process(mut self,) {
        
        let segment_manager = self.segment_manager_arc.clone();
        
        for segment_update in self.segment_update_receiver.clone() {
            
            if let SegmentUpdate::Terminate = segment_update {
                break;
            }
                
            // we check the generation number as if it was 
            // dirty-bit. If the value is different 
            // to our generation, then the segment_manager has
            // been update updated.
            let generation_before_update = segment_manager.generation();
            
            self.process_one(segment_update);
            
            if generation_before_update != segment_manager.generation() {
                // The segment manager has changed, we need to   
                // - save meta.json	
                save_metas(
                    &*segment_manager,
                    self.index.schema(),
                    self.index.docstamp(),
                    self.index.directory_mut()).expect("Could not save metas.");


                // - update the searchers
                
                // update the searchers so that they eventually will
                // use the new segments.
                // TODO eventually have this work through watching meta.json
                // so that an external process stays up to date as well. 
                match self.index.load_searchers() {
                    Ok(()) => {
                    }
                    Err(e) => {
                        error!("Failure while loading new searchers {:?}", e);
                        panic!(format!("Failure while loading new searchers {:?}", e));
                    }
                }
                
                // - start merges if required
                self.start_merges();
            }
        }
        
        let mut merging_threads = HashMap::new();
        mem::swap(&mut merging_threads, &mut self.merging_threads);
        for (_, merging_thread_handle) in merging_threads {
            match merging_thread_handle.join() {
                Ok((segment_ids, segment_meta)) => {
                    self.end_merge(segment_ids, segment_meta);
                }
                Err(e) => {
                    error!("Error in merging thread {:?}", e);
                    break;
                } 
            }
        }
    }

    

    
	// Process a single segment update.
	pub fn process_one(
		&mut self,
		segment_update: SegmentUpdate) {
		
		info!("Segment update: {:?}", segment_update);
        
		match segment_update {
			SegmentUpdate::AddSegment(segment_meta) => {
				if !self.is_cancelled_generation {
					self.segment_manager().add_segment(segment_meta);
				}
				else {
					// rollback has been called and this
					// segment actually belong to the 
					// documents that have been dropped.
					//
					// Let's just remove its files.
					self.index.delete_segment(segment_meta.segment_id);
				}
			}
			SegmentUpdate::EndMerge(merging_thread_id, segment_ids, segment_meta) => {
                self.end_merge(
                    segment_ids,
                    segment_meta);
                self.merging_threads.remove(&merging_thread_id);
			}
			SegmentUpdate::CancelGeneration => {
				// Called during rollback. The segment 
				// that will arrive will be ignored
				// until a NewGeneration is update arrives.
				self.is_cancelled_generation = true;
			}
			SegmentUpdate::NewGeneration => {
				// After rollback, we can resume
				// indexing new documents.
				self.is_cancelled_generation = false;
			}
			SegmentUpdate::Commit(docstamp) => {
				self.segment_manager().commit(docstamp);
			}
			SegmentUpdate::Terminate => {
                panic!("We should have left the loop before processing it.");
			}
		}
	}
}
