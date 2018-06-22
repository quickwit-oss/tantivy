use core::SegmentId;
use core::SegmentMeta;
use indexer::delete_queue::DeleteCursor;
use indexer::segment_entry::SegmentEntry;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};

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
        write!(f, "SegmentRegister(")?;
        for (k, v) in &self.segment_states {
            write!(f, "{}:{}, ", k.short_uuid_string(), v.state().letter_code())?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl SegmentRegister {
    pub fn clear(&mut self) {
        self.segment_states.clear();
    }

    pub fn len(&self) -> usize {
        self.segment_states.len()
    }

    pub fn get_all_segments(&self) -> Vec<SegmentMeta> {
        self.segment_states
            .values()
            .map(|segment_entry| segment_entry.meta().clone())
            .collect()
    }

    pub fn get_mergeable_segments(&self) -> Vec<SegmentMeta> {
        self.segment_states
            .values()
            .filter(|segment_entry| segment_entry.is_ready())
            .map(|segment_entry| segment_entry.meta().clone())
            .collect()
    }

    pub fn segment_entries(&self) -> Vec<SegmentEntry> {
        self.segment_states.values().cloned().collect()
    }

    pub fn segment_metas(&self) -> Vec<SegmentMeta> {
        let mut segment_ids: Vec<SegmentMeta> = self
            .segment_states
            .values()
            .map(|segment_entry| segment_entry.meta().clone())
            .collect();
        segment_ids.sort_by_key(|meta| meta.id());
        segment_ids
    }

    pub fn segment_entry(&self, segment_id: &SegmentId) -> Option<SegmentEntry> {
        self.segment_states.get(segment_id).cloned()
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

    pub fn cancel_merge(&mut self, segment_id: &SegmentId) {
        self.segment_states
            .get_mut(segment_id)
            .expect("Received a merge notification for a segment that is not registered")
            .cancel_merge();
    }

    pub fn start_merge(&mut self, segment_id: &SegmentId) {
        self.segment_states
            .get_mut(segment_id)
            .expect("Received a merge notification for a segment that is not registered")
            .start_merge();
    }

    pub fn new(segment_metas: Vec<SegmentMeta>, delete_cursor: &DeleteCursor) -> SegmentRegister {
        let mut segment_states = HashMap::new();
        for segment_meta in segment_metas {
            let segment_id = segment_meta.id();
            let segment_entry = SegmentEntry::new(segment_meta, delete_cursor.clone(), None);
            segment_states.insert(segment_id, segment_entry);
        }
        SegmentRegister { segment_states }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::SegmentId;
    use core::SegmentMeta;
    use indexer::delete_queue::*;
    use indexer::SegmentState;

    fn segment_ids(segment_register: &SegmentRegister) -> Vec<SegmentId> {
        segment_register
            .segment_metas()
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect()
    }

    #[test]
    fn test_segment_register() {
        let delete_queue = DeleteQueue::new();

        let mut segment_register = SegmentRegister::default();
        let segment_id_a = SegmentId::generate_random();
        let segment_id_b = SegmentId::generate_random();
        let segment_id_merged = SegmentId::generate_random();

        {
            let segment_meta = SegmentMeta::new(segment_id_a);
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor(), None);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(
            segment_register
                .segment_entry(&segment_id_a)
                .unwrap()
                .state(),
            SegmentState::Ready
        );
        assert_eq!(segment_ids(&segment_register), vec![segment_id_a]);
        {
            let segment_meta = SegmentMeta::new(segment_id_b);
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor(), None);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(
            segment_register
                .segment_entry(&segment_id_b)
                .unwrap()
                .state(),
            SegmentState::Ready
        );
        segment_register.start_merge(&segment_id_a);
        segment_register.start_merge(&segment_id_b);
        assert_eq!(
            segment_register
                .segment_entry(&segment_id_a)
                .unwrap()
                .state(),
            SegmentState::InMerge
        );
        assert_eq!(
            segment_register
                .segment_entry(&segment_id_b)
                .unwrap()
                .state(),
            SegmentState::InMerge
        );
        segment_register.remove_segment(&segment_id_a);
        segment_register.remove_segment(&segment_id_b);
        {
            let segment_meta_merged = SegmentMeta::new(segment_id_merged);
            let segment_entry = SegmentEntry::new(segment_meta_merged, delete_queue.cursor(), None);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_merged]);
    }

}
