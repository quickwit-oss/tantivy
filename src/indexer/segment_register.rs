use core::SegmentId;
use std::collections::HashMap;
use core::SegmentMeta;
use std::fmt;
use std::fmt::{Debug, Formatter};
use indexer::segment_entry::SegmentEntry;

/// The segment register keeps track
/// of the list of segment, their size as well
/// as the state they are in.
/// 
/// It is consumed by indexes to get the list of 
/// segments that are currently searchable,
/// and by the index merger to identify 
/// merge candidates.
#[derive(Default)]
pub struct SegmentRegister {
    segment_states: HashMap<SegmentId, SegmentEntry>, 
}


impl Debug for SegmentRegister {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        try!(write!(f, "SegmentRegister("));
        for (k, v) in &self.segment_states {
            try!(write!(f, "{}:{}, ", k.short_uuid_string(), v.state().letter_code()));
        }
        try!(write!(f, ")"));
        Ok(())
    }
}

impl SegmentRegister {
    
    pub fn clear(&mut self,) {
        self.segment_states.clear();
    }
    
    pub fn get_segments(&self,) -> Vec<SegmentMeta> {
        self.segment_states
            .values()
            .filter(|segment_entry| segment_entry.is_ready())
            .map(|segment_entry| segment_entry.meta().clone())
            .collect()
    }
    
    pub fn segment_entries(&self,) -> Vec<SegmentEntry> {
        self.segment_states
            .values()
            .cloned()
            .collect()
    }
    
    pub fn segment_metas(&self,) -> Vec<SegmentMeta> {
        let mut segment_ids: Vec<SegmentMeta> = self.segment_states
            .values()
            .map(|segment_entry| segment_entry.meta().clone())
            .collect();
        segment_ids.sort_by_key(|meta| meta.id());
        segment_ids
    }
    
    pub fn segment_ids(&self,) -> Vec<SegmentId> {
        self.segment_metas()
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect()
    }
    
    pub fn segment_entry(&self, segment_id: &SegmentId) -> Option<SegmentEntry> {
        self.segment_states
            .get(&segment_id)
            .map(|segment_entry| segment_entry.clone())
    }
    
    pub fn contains_all(&mut self, segment_ids: &[SegmentId]) -> bool {
        segment_ids
            .iter()
            .all(|segment_id| self.segment_states.contains_key(segment_id))
    }
    
    pub fn add_segment_entry(&mut self, segment_entry: SegmentEntry) {
        let segment_id = segment_entry.segment_id();
        self.segment_states.insert(segment_id, segment_entry);
    }
    
    pub fn remove_segment(&mut self, segment_id: &SegmentId) {
        self.segment_states.remove(segment_id);
    }   
    
    pub fn start_merge(&mut self, segment_id: &SegmentId) {
        self.segment_states
            .get_mut(segment_id)
            .expect("Received a merge notification for a segment that is not registered")
            .start_merge();
    } 
    
    pub fn new(segment_metas: Vec<SegmentMeta>) -> SegmentRegister {
        SegmentRegister {
            segment_states: segment_metas
                .into_iter()
                .map(|segment_meta| {
                    let segment_id = segment_meta.id();
                    let segment_entry = SegmentEntry::new(segment_meta  );
                    (segment_id, segment_entry)
                })
                .collect(),
        }
    }
}


#[cfg(test)]
mod tests {
    use indexer::SegmentState;
    use core::SegmentId;
    use core::SegmentMeta;
    use super::*;
    
    #[test]
    fn test_segment_register() {
        let mut segment_register = SegmentRegister::default();
        let segment_id_a = SegmentId::generate_random();
        let segment_id_b = SegmentId::generate_random();
        let segment_id_merged = SegmentId::generate_random();
        
        {
            let segment_meta = SegmentMeta::new(segment_id_a);
            let segment_entry = SegmentEntry::new(segment_meta);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(segment_register.segment_entry(&segment_id_a).unwrap().state(), SegmentState::Ready);
        assert_eq!(segment_register.segment_ids(), vec!(segment_id_a));
        {
            let segment_meta = SegmentMeta::new(segment_id_b);
            let segment_entry = SegmentEntry::new(segment_meta);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(segment_register.segment_entry(&segment_id_b).unwrap().state(), SegmentState::Ready);
        segment_register.start_merge(&segment_id_a);
        segment_register.start_merge(&segment_id_b);
        assert_eq!(segment_register.segment_entry(&segment_id_a).unwrap().state(), SegmentState::InMerge);
        assert_eq!(segment_register.segment_entry(&segment_id_b).unwrap().state(), SegmentState::InMerge);
        segment_register.remove_segment(&segment_id_a);
        segment_register.remove_segment(&segment_id_b);
        {
            let segment_meta_merged = SegmentMeta::new(segment_id_merged);
            let segment_entry = SegmentEntry::new(segment_meta_merged);
            segment_register.add_segment_entry(segment_entry);        
        }
        assert_eq!(segment_register.segment_ids(), vec!(segment_id_merged));        
    }
    
}