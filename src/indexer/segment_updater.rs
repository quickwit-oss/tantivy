use chan;
use common::LivingCounterLatch;
use core::Index;
use core::Segment;
use core::SegmentId;
use core::SegmentMeta;
use core::SerializableSegment;
use indexer::{MergePolicy, SimpleMergePolicy};
use indexer::index_writer::save_metas;
use indexer::MergeCandidate;
use indexer::merger::IndexMerger;
use indexer::SegmentSerializer;
use std::thread;
use std::thread::JoinHandle;
use std::sync::Arc;
use super::segment_manager::{SegmentManager, get_segment_ready_for_commit};
use super::super::core::index::get_segment_manager;

pub type SegmentUpdateSender = chan::Sender<SegmentUpdate>;
pub type SegmentUpdateReceiver = chan::Receiver<SegmentUpdate>;


#[derive(Debug)]
pub enum SegmentUpdate {
    
    /// New segment added.
    /// Created by the indexing worker thread 
    AddSegment(SegmentMeta),
    
    /// A merge is ended.
    /// Remove the merged segment and record the new 
    /// large merged segment.
    EndMerge(Vec<SegmentId>, SegmentMeta),
    
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





/// The segment updater is in charge of 
/// receiving different SegmentUpdate
/// - indexing threads are sending new segments 
/// - merging threads are sending merge operations
/// - the index writer sends "terminate"
pub struct SegmentUpdater {
	index: Index,
	is_cancelled_generation: bool,
    has_received_terminate_command: bool,
	segment_update_receiver: SegmentUpdateReceiver,
	segment_update_sender: SegmentUpdateSender,
    segment_manager_arc: Arc<SegmentManager>,
    merge_policy: Box<MergePolicy>,
    living_threads: LivingCounterLatch,
}


impl SegmentUpdater {
	
    pub fn new(index: Index) -> SegmentUpdater {
        let segment_manager_arc = get_segment_manager(&index);
        let (segment_update_sender, segment_update_receiver): (SegmentUpdateSender, SegmentUpdateReceiver) = chan::sync(0);
		SegmentUpdater {
			index: index,
            is_cancelled_generation: false,
            has_received_terminate_command: false,
            segment_update_sender: segment_update_sender,
            segment_update_receiver: segment_update_receiver,
            segment_manager_arc: segment_manager_arc,
            merge_policy: Box::new(SimpleMergePolicy::default()), // TODO make that configurable
            living_threads: LivingCounterLatch::default(),
		}
	}
    
	pub fn update_channel(&self,) -> SegmentUpdateSender {
		self.segment_update_sender.clone()
	}
    
    fn start_merges(&self,) {
        
        let merge_candidates = self.consider_merge_options();
        
        for MergeCandidate(segment_ids) in merge_candidates {
            self.segment_manager().start_merge(&segment_ids);
            let living_threads_clone = self.living_threads.clone(); 
            let index_clone = self.index.clone();
            let segment_update_sender_clone = self.segment_update_sender.clone();
            thread::Builder::new().name(format!("merge_thread_{:?}", segment_ids[0])).spawn(move || {
                info!("Start merge: {:?}", segment_ids);
                let schema = index_clone.schema();
                let segments: Vec<Segment> = segment_ids
                    .iter()
                    .map(|&segment_id| index_clone.segment(segment_id))
                    .collect();
                // An IndexMerger is like a "view" of our merged segments. 
                // TODO unwrap
                let merger: IndexMerger = IndexMerger::open(schema, &segments[..]).unwrap();
                let mut merged_segment = index_clone.new_segment();
                // ... we just serialize this index merger in our new segment
                // to merge the two segments.
                let segment_serializer = SegmentSerializer::for_segment(&mut merged_segment).unwrap();
                let num_docs = merger.write(segment_serializer).unwrap();
                let segment_meta = SegmentMeta {
                    segment_id: merged_segment.id(),
                    num_docs: num_docs,
                };
                let segment_update = SegmentUpdate::EndMerge(segment_ids, segment_meta);
                segment_update_sender_clone.send(segment_update);
                drop(living_threads_clone);
            }).expect("Failed to spawn merge thread");
        }
    }
    
    fn consider_merge_options(&self,) -> Vec<MergeCandidate> {
        let segment_manager = self.segment_manager();
        let (committed_segments, uncommitted_segments) = get_segment_ready_for_commit(segment_manager);
        // Committed segments cannot be merged with uncommitted_segments.
        // We therefore consider merges using these two sets of segments independantly.
        let mut merge_candidates = self.merge_policy.compute_merge_candidates(&uncommitted_segments);
        let committed_merge_candidates = self.merge_policy.compute_merge_candidates(&committed_segments);
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
	
    fn still_has_work(&self,) -> bool {
        if self.has_received_terminate_command {
            !self.living_threads.is_zero()
        }
        else {
            true
        }
    }
    
	fn process(mut self,) {
        
        let segment_manager = self.segment_manager_arc.clone();
                
        let mut segment_updates_it = self.segment_update_receiver.clone().into_iter();
        
        while self.still_has_work() {
            
            let segment_update = segment_updates_it.next().expect("");
                
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
                self.index.load_searchers().expect("Could not load new searchers.");
                
                // - start merges if required
                self.start_merges();
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
			SegmentUpdate::EndMerge(segment_ids, segment_meta) => {
				self.segment_manager().end_merge(&segment_ids, &segment_meta);
				for segment_id in segment_ids {
					self.index.delete_segment(segment_id);
				}
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
                self.has_received_terminate_command = true;
			}
		}
	}
}
