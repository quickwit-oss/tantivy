use std::sync::RwLock;
use error::Result;
use core::SegmentId;
use std::collections::HashMap;
use core::SegmentMeta;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SegmentState {
    Ready,
    InMerge,    
}

pub enum SegmentUpdate {
    StartMerge(Vec<SegmentId>),
    EndMerge(Vec<SegmentId>, SegmentMeta),
    NewSegment(SegmentMeta),
}

#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
    state: SegmentState,
}

impl SegmentEntry {
    fn start_merge(&mut self,) {
        self.state = SegmentState::InMerge;
    }
}




/// The segment register keeps track
/// of the list of segment, their size as well
/// as the state they are in.
/// 
/// It is consumed by indexes to get the list of 
/// segments that are currently searchable,
/// and by the index merger to identify 
/// merge candidates.
pub struct SegmentRegister {
    segment_states: RwLock<HashMap<SegmentId, SegmentEntry>>, 
}

impl SegmentRegister {
    
    pub fn segment_metas(&self,) -> Result<Vec<SegmentMeta>> {
        let segment_register_lock = try!(self.segment_states.read());
        let mut segment_ids: Vec<SegmentMeta> = segment_register_lock
            .values()
            .map(|segment_entry| segment_entry.meta.clone())
            .collect();
        segment_ids.sort_by_key(|meta| meta.segment_id);
        Ok(segment_ids)
    }
    
    pub fn segment_ids(&self,) -> Result<Vec<SegmentId>> {
        let segment_ids: Vec<SegmentId> = try!(self.segment_metas())
            .into_iter()
            .map(|segment_meta| segment_meta.segment_id)
            .collect();
        Ok(segment_ids)
    }
    
    #[cfg(test)]
    pub fn segment_entry(&self, segment_id: &SegmentId) -> Option<SegmentEntry> {
        self.segment_states
            .read()
            .expect("Could not acquire lock")
            .get(&segment_id)
            .map(|segment_entry| segment_entry.clone())
    }

    pub fn remove_segment(&self, segment_id: &SegmentId) -> Result<()> {
        try!(self.segment_states.write())
            .remove(segment_id);
        Ok(())
    }
    
    pub fn segment_update(&self, segment_update: SegmentUpdate) -> Result<()> {
        let mut segment_register_lock = try!(self.segment_states.write());
        match segment_update {
            SegmentUpdate::StartMerge(segment_ids) => {
                for segment_id in segment_ids {
                    segment_register_lock
                        .get_mut(&segment_id)
                        .expect("Received a merge notification for a segment that is not registered")
                        .start_merge();
                }
            }
            SegmentUpdate::EndMerge(merged_segment_ids, resulting_segment_meta) => {
                 for segment_id in merged_segment_ids {
                     segment_register_lock
                         .remove(&segment_id)
                         .expect(&format!("The segment {:?} is not within the segment register.", segment_id));
                 }
                 let segment_id_clone = resulting_segment_meta.segment_id.clone();
                 let segment_entry = SegmentEntry {
                     meta: resulting_segment_meta,
                     state: SegmentState::Ready,
                 };
                 segment_register_lock.insert(segment_id_clone, segment_entry);
            }
            SegmentUpdate::NewSegment(segment_meta) => {
                let segment_id = segment_meta.segment_id.clone();
                let segment_entry = SegmentEntry {
                    meta: segment_meta,
                    state: SegmentState::Ready,
                };
                segment_register_lock.insert(segment_id, segment_entry);
            }
        }
        Ok(())        
    }
    
}


impl From<Vec<SegmentMeta>> for SegmentRegister {
    fn from(segment_metas: Vec<SegmentMeta>) -> SegmentRegister {
        let mut segment_states = HashMap::new();
        for segment_meta in segment_metas {
            let segment_id = segment_meta.segment_id.clone();
            let segment_entry = SegmentEntry {
                meta: segment_meta,
                state: SegmentState::Ready,
                
            };
            segment_states.insert(segment_id, segment_entry);
        }
        SegmentRegister {
            segment_states: RwLock::new(segment_states),
        }
    }
}

impl Default for SegmentRegister {
    fn default() -> SegmentRegister {
        SegmentRegister {
            segment_states: RwLock::new(HashMap::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    
    use core::SegmentId;
    use core::SegmentMeta;
    use super::*;
    
    #[test]
    fn test_segment_register() {
        let segment_register = SegmentRegister::default();
        let segment_id_a = SegmentId::generate_random();
        let segment_id_b = SegmentId::generate_random();
        let segment_id_merged = SegmentId::generate_random();
        let segment_meta_merged = SegmentMeta::new(segment_id_merged, 10 + 20);
        segment_register.segment_update(SegmentUpdate::NewSegment(SegmentMeta::new(segment_id_a, 10))).unwrap();
        assert_eq!(segment_register.segment_entry(&segment_id_a).unwrap().state, SegmentState::Ready);
        assert_eq!(segment_register.segment_ids().unwrap(), vec!(segment_id_a));
        segment_register.segment_update(SegmentUpdate::NewSegment(SegmentMeta::new(segment_id_b, 20))).unwrap();
        assert_eq!(segment_register.segment_entry(&segment_id_b).unwrap().state, SegmentState::Ready);
        segment_register.segment_update(SegmentUpdate::StartMerge(vec!(segment_id_a, segment_id_b))).unwrap();
        assert_eq!(segment_register.segment_entry(&segment_id_a).unwrap().state, SegmentState::InMerge);
        assert_eq!(segment_register.segment_entry(&segment_id_b).unwrap().state, SegmentState::InMerge);
        segment_register.segment_update(SegmentUpdate::EndMerge(vec!(segment_id_a, segment_id_b), segment_meta_merged)).unwrap();
        assert_eq!(segment_register.segment_ids().unwrap(), vec!(segment_id_merged));        
    }
    
}