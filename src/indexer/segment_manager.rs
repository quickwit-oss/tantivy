use super::segment_register::SegmentRegister;
use std::sync::RwLock;
use core::SegmentMeta;
use core::SegmentId;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

struct SegmentRegisters {
    uncommitted: SegmentRegister,
    committed: SegmentRegister,
}

impl Default for SegmentRegisters {
    fn default() -> SegmentRegisters {
        SegmentRegisters {
            uncommitted: SegmentRegister::default(),
            committed: SegmentRegister::default(),
        }
    }
}




/// The segment manager stores the list of segments
/// as well as their state.
///
/// It guarantees the atomicity of the 
/// changes (merges especially)
pub struct SegmentManager {
    registers: RwLock<SegmentRegisters>,
}

/// Returns the segment_metas for (committed segment, uncommitted segments).
/// The result is consistent with other transactions.
///
/// For instance, a segment will not appear in both committed and uncommitted 
/// segments
pub fn get_segment_ready_for_commit(segment_manager: &SegmentManager,) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
    let registers_lock = segment_manager
        .registers
        .read()
        .expect("Segment manager lock is poisoned");
    (registers_lock.committed.get_segment_ready_for_commit(),
     registers_lock.uncommitted.get_segment_ready_for_commit())
}

impl SegmentManager {

    pub fn from_segments(segment_metas: Vec<SegmentMeta>) -> SegmentManager {
        SegmentManager {
            registers: RwLock::new( SegmentRegisters {
                uncommitted: SegmentRegister::default(),
                committed: SegmentRegister::from(segment_metas),
            })
        }
    }


    // Lock poisoning should never happen :
    // The lock is acquired and released within this class,
    // and the operations cannot panic. 
    fn read(&self,) -> RwLockReadGuard<SegmentRegisters> { 
        self.registers.read().expect("Failed to acquire read lock on SegmentManager.")
    }
    fn write(&self,) -> RwLockWriteGuard<SegmentRegisters> {
        self.registers.write().expect("Failed to acquire write lock on SegmentManager.")
    }

    /// Removes all of the uncommitted segments
    /// and returns them.
    pub fn rollback(&self,) -> Vec<SegmentId> {
        let mut registers_lock = self.write();
        let segment_ids = registers_lock.uncommitted.segment_ids();
        registers_lock.uncommitted.clear();
        segment_ids
    }

    pub fn commit(&self,) {
        let mut registers_lock = self.write();
        let segment_metas = registers_lock.uncommitted.segment_metas();
        for segment_meta in segment_metas {
            registers_lock.committed.add_segment(segment_meta.clone());
        }
        registers_lock.uncommitted.clear();       
    }
    
    pub fn add_segment(&self, segment_meta: SegmentMeta) {
        let mut registers_lock = self.write();
        registers_lock.uncommitted.add_segment(segment_meta);
    }
    
    pub fn start_merge(&self, segment_ids: &[SegmentId]) {
        let mut registers_lock = self.write();
        if registers_lock.uncommitted.contains_all(segment_ids) {
            for segment_id in segment_ids {
                registers_lock.uncommitted.start_merge(segment_id);
            }
        }
        else if registers_lock.committed.contains_all(segment_ids) {
            for segment_id in segment_ids {
                registers_lock.committed.start_merge(segment_id);
            }
        }
    }
    
    pub fn end_merge(&self, merged_segment_ids: &[SegmentId], merged_segment_meta: &SegmentMeta) {
        let mut registers_lock = self.write();
        if registers_lock.uncommitted.contains_all(merged_segment_ids) {
            for segment_id in merged_segment_ids {
                registers_lock.uncommitted.remove_segment(segment_id);
            }
            registers_lock.uncommitted.add_segment(merged_segment_meta.clone());
        }
        else if registers_lock.committed.contains_all(merged_segment_ids) {
            for segment_id in merged_segment_ids {
                registers_lock.committed.remove_segment(segment_id);
            }
            registers_lock.committed.add_segment(merged_segment_meta.clone());
        }
    }
    
    pub fn committed_segments(&self,) -> Vec<SegmentId> {
        let registers_lock = self.read();
        registers_lock.committed.segment_ids()
    }
    
    pub fn segment_metas(&self,) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
        let registers_lock = self.read();
        (registers_lock.committed.segment_metas(), registers_lock.uncommitted.segment_metas())
    }
}

