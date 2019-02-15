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
    segment_states: HashMap<SegmentId, SegmentMeta>,
    opstamp_constraint: u64,
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
            .filter(|segment_meta| !in_merge_segment_ids.contains(&segment_meta.id()))
            .cloned()
            .collect()
    }

    pub fn segment_metas(&self) -> Vec<SegmentMeta> {
        let mut segment_metas: Vec<SegmentMeta> = self
            .segment_states
            .values()
            .cloned()
            .collect();
        segment_metas.sort_by_key(|meta| meta.id());
        segment_metas
    }

    pub fn segment_entries(&self, delete_cursor: &DeleteCursor) -> Vec<SegmentEntry> {
        self.segment_states
            .values()
            .map(|segment_meta| {
                SegmentEntry::new(segment_meta.clone(), delete_cursor.clone(), None, self.opstamp_constraint)
            })
            .collect()
    }

    pub fn contains_all(&self, segment_ids: &[SegmentId]) -> bool {
        segment_ids
            .iter()
            .all(|segment_id| self.segment_states.contains_key(segment_id))
    }

    pub fn receive_merge(&mut self,
                         before_merge_segment_ids: &[SegmentId],
                         after_merge_segment_entry: &SegmentEntry) {
        if after_merge_segment_entry.opstamp() != self.opstamp_constraint {
            return;
        }
        if !self.contains_all(before_merge_segment_ids) {
            return;
        }
        for segment_id in before_merge_segment_ids {
            self.segment_states.remove(segment_id);
        }
        self.register_segment_entry(after_merge_segment_entry.clone());
    }

    /// Registers a `SegmentEntry`.
    ///
    /// If a segment entry associated to this `SegmentId` is already there,
    /// override it with the new `SegmentEntry`.
    pub fn register_segment_entry(&mut self, segment_entry: SegmentEntry) {
        if self.opstamp_constraint != segment_entry.opstamp() {
            panic!(format!(
                "Invalid segment. Expect opstamp {}, got {}.",
                self.opstamp_constraint,
                segment_entry.opstamp()
            ));
        }
        if segment_entry.meta().num_docs() == 0 {
            return;
        }
        let segment_id = segment_entry.segment_id();
        // Check that we are ok with deletes.
        self.segment_states.insert(segment_id, segment_entry.meta().clone());
    }

    pub fn set_commit(&mut self, opstamp: u64, segment_entries: Vec<SegmentEntry>) {
        self.segment_states.clear();
        self.opstamp_constraint = opstamp;
        for segment_entry in segment_entries {
            self.register_segment_entry(segment_entry);
        }
    }

    pub fn remove_segment(&mut self, segment_id: &SegmentId) {
        self.segment_states.remove(&segment_id);
    }

    pub fn get(&self, segment_id: &SegmentId, delete_cursor: &DeleteCursor) -> Option<SegmentEntry> {
        self.segment_states
            .get(&segment_id)
            .map(|segment_meta|
                SegmentEntry::new(
                    segment_meta.clone(),
                    delete_cursor.clone(),
                    None,
                    self.opstamp_constraint
                ))
    }

    pub fn new(
        segment_metas: Vec<SegmentMeta>,
        opstamp: u64,
    ) -> SegmentRegister {
        let mut segment_states = HashMap::new();
        for segment_meta in segment_metas {
            segment_states.insert(segment_meta.id(), segment_meta);
        }
        SegmentRegister {
            segment_states,
            opstamp_constraint: opstamp,
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
            let segment_meta = SegmentMeta::new(segment_id_a, 1u32);
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor(), None, 0u64);
            segment_register.register_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_a]);
        {
            let segment_meta = SegmentMeta::new(segment_id_b, 2u32);
            let segment_entry = SegmentEntry::new(segment_meta, delete_queue.cursor(), None, 0u64);
            segment_register.register_segment_entry(segment_entry);
        }
        {
            let segment_meta_merged = SegmentMeta::new(segment_id_merged, 3u32);
            let segment_entry =
                SegmentEntry::new(segment_meta_merged, delete_queue.cursor(), None, 0u64);
            segment_register.receive_merge(&[segment_id_a, segment_id_b], &segment_entry);
            segment_register.register_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_merged]);
    }

}
