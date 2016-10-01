use super::segment_register::SegmentRegister;
use std::sync::RwLock;
use core::SegmentMeta;
use error::Result;
use core::SegmentId;

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

impl SegmentManager {
    
    pub fn from_segments(segment_metas: Vec<SegmentMeta>) -> SegmentManager {
        SegmentManager {
            registers: RwLock::new( SegmentRegisters {
                uncommitted: SegmentRegister::default(),
                committed: SegmentRegister::from(segment_metas),
            })
        }
    }

    /// Removes all of the uncommitted segments
    /// and returns them.
    pub fn rollback(&self,) -> Result<Vec<SegmentId>> {
        let mut registers_lock = try!(self.registers.write());
        let segment_ids = registers_lock.uncommitted.segment_ids();
        registers_lock.uncommitted.clear();
        Ok(segment_ids)
    }

    pub fn commit(&self,) -> Result<()> {
        let mut registers_lock = try!(self.registers.write());
        let segment_metas = registers_lock.uncommitted.segment_metas();
        for segment_meta in segment_metas {
            registers_lock.committed.add_segment(segment_meta.clone());
        }
        registers_lock.uncommitted.clear();
        Ok(())        
    }
    
    pub fn add_segment(&self, segment_meta: SegmentMeta) -> Result<()> {
        let mut registers_lock = try!(self.registers.write());
        registers_lock.uncommitted.add_segment(segment_meta);
        Ok(())
    }
    
    pub fn start_merge(&self, segment_ids: &[SegmentId]) -> Result<()> {
        let mut registers_lock = try!(self.registers.write());
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
        Ok(())
    }
    
    pub fn end_merge(&self, merged_segment_ids: &[SegmentId], merged_segment_meta: &SegmentMeta) -> Result<()> {
        let mut registers_lock = try!(self.registers.write());
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
        Ok(())
    }
    
    pub fn committed_segments(&self,) -> Result<Vec<SegmentId>> {
        let registers_lock = try!(self.registers.read());
        Ok(registers_lock.committed.segment_ids())
    }
    
    pub fn segment_metas(&self,) -> Result<(Vec<SegmentMeta>, Vec<SegmentMeta>)> {
        let registers_lock = try!(self.registers.read());
        Ok((registers_lock.committed.segment_metas(), registers_lock.uncommitted.segment_metas()))
    }
}

