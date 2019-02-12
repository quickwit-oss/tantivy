use core::SegmentId;
use core::SegmentMeta;
use indexer::delete_queue::DeleteCursor;
use indexer::segment_entry::SegmentEntry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};

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
    opstamp_constraint: Option<u64>
}

impl Debug for SegmentRegister {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "SegmentRegister(")?;
        for k in self.segment_states.keys() {
            write!(f, "{}, ", k.short_uuid_string())?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl SegmentRegister {
    pub fn clear(&mut self) {
        self.segment_states.clear();
    }

    pub fn get_mergeable_segments(
        &self,
        in_merge_segment_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentMeta> {
        self.segment_states
            .values()
            .filter(|segment_entry| !in_merge_segment_ids.contains(&segment_entry.segment_id()))
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

    pub fn contains_all(&self, segment_ids: &[SegmentId]) -> bool {
        segment_ids
            .iter()
            .all(|segment_id| self.segment_states.contains_key(segment_id))
    }

    /// Registers a `SegmentEntry`.
    ///
    /// If a segment entry associated to this `SegmentId` is already there,
    /// override it with the new `SegmentEntry`.
    pub fn register_segment_entry(&mut self, segment_entry: SegmentEntry) {
        if let Some(expected_opstamp) = self.opstamp_constraint {
            if expected_opstamp != segment_entry.opstamp() {
                panic!(format!("Invalid segment. Expect opstamp {}, got {}.", expected_opstamp, segment_entry.opstamp()));
            }
        }
        let segment_id = segment_entry.segment_id();
        self.segment_states.insert(segment_id, segment_entry);
    }

    pub fn set_commit(&mut self, opstamp: u64, segment_entries: Vec<SegmentEntry>) {
        assert!(self.segment_states.is_empty());
        self.opstamp_constraint = Some(opstamp);
        for segment_entry in segment_entries {
            self.register_segment_entry(segment_entry);
        }
    }

    pub fn remove_segment(&mut self, segment_id: &SegmentId) {
        self.segment_states.remove(segment_id);
    }

    pub fn get(&self, segment_id: &SegmentId) -> Option<SegmentEntry> {
        self.segment_states.get(segment_id).cloned()
    }

    pub fn new(segment_metas: Vec<SegmentMeta>, delete_cursor: &DeleteCursor, opstamp: u64) -> SegmentRegister {
        let mut segment_states = HashMap::new();
        for segment_meta in segment_metas {
            let segment_id = segment_meta.id();
            let segment_entry = SegmentEntry::new(segment_meta, delete_cursor.clone(), None, opstamp);
            segment_states.insert(segment_id, segment_entry);
        }
        SegmentRegister {
            segment_states,
            opstamp_constraint: Some(opstamp)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::SegmentId;
    use core::SegmentMeta;
    use indexer::delete_queue::*;

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
            let segment_meta = SegmentMeta::new(segment_id_a, 0u32);
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor(), None, 1u64);
            segment_register.register_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_a]);
        {
            let segment_meta = SegmentMeta::new(segment_id_b, 0u32);
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor(), None, 2u64);
            segment_register.register_segment_entry(segment_entry);
        }
        segment_register.remove_segment(&segment_id_a);
        segment_register.remove_segment(&segment_id_b);
        {
            let segment_meta_merged = SegmentMeta::new(segment_id_merged, 0u32);
            let segment_entry = SegmentEntry::new(segment_meta_merged, delete_queue.cursor(), None, 3u64);
            segment_register.register_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_merged]);
    }

}
