use super::segment_register::SegmentRegister;
use core::SegmentId;
use core::SegmentMeta;
use core::{LOCKFILE_FILEPATH, META_FILEPATH};
use indexer::delete_queue::DeleteCursor;
use indexer::SegmentEntry;
use std::collections::hash_set::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

#[derive(Default)]
struct SegmentRegisters {
    uncommitted: SegmentRegister,
    committed: SegmentRegister,
    writing: HashSet<SegmentId>,
}

/// The segment manager stores the list of segments
/// as well as their state.
///
/// It guarantees the atomicity of the
/// changes (merges especially)
#[derive(Default)]
pub struct SegmentManager {
    registers: RwLock<SegmentRegisters>,
}

impl Debug for SegmentManager {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        let lock = self.read();
        write!(
            f,
            "{{ uncommitted: {:?}, committed: {:?} }}",
            lock.uncommitted, lock.committed
        )
    }
}

pub fn get_mergeable_segments(
    segment_manager: &SegmentManager,
) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
    let registers_lock = segment_manager.read();
    (
        registers_lock.committed.get_mergeable_segments(),
        registers_lock.uncommitted.get_mergeable_segments(),
    )
}

impl SegmentManager {
    pub fn from_segments(
        segment_metas: Vec<SegmentMeta>,
        delete_cursor: &DeleteCursor,
    ) -> SegmentManager {
        SegmentManager {
            registers: RwLock::new(SegmentRegisters {
                uncommitted: SegmentRegister::default(),
                committed: SegmentRegister::new(segment_metas, delete_cursor),
                writing: HashSet::new(),
            }),
        }
    }

    /// Returns all of the segment entries (committed or uncommitted)
    pub fn segment_entries(&self) -> Vec<SegmentEntry> {
        let mut segment_entries = self.read().uncommitted.segment_entries();
        segment_entries.extend(self.read().committed.segment_entries());
        segment_entries
    }

    /// Returns the overall number of segments in the `SegmentManager`
    pub fn num_segments(&self) -> usize {
        let registers_lock = self.read();
        registers_lock.committed.len() + registers_lock.uncommitted.len()
    }

    pub fn list_files(&self) -> HashSet<PathBuf> {
        let mut files = HashSet::new();
        files.insert(META_FILEPATH.clone());
        files.insert(LOCKFILE_FILEPATH.clone());
        for segment_meta in SegmentMeta::all() {
            files.extend(segment_meta.list_files());
        }
        files
    }

    pub fn segment_entry(&self, segment_id: &SegmentId) -> Option<SegmentEntry> {
        let registers = self.read();
        registers
            .committed
            .segment_entry(segment_id)
            .or_else(|| registers.uncommitted.segment_entry(segment_id))
    }

    // Lock poisoning should never happen :
    // The lock is acquired and released within this class,
    // and the operations cannot panic.
    fn read(&self) -> RwLockReadGuard<SegmentRegisters> {
        self.registers
            .read()
            .expect("Failed to acquire read lock on SegmentManager.")
    }

    fn write(&self) -> RwLockWriteGuard<SegmentRegisters> {
        self.registers
            .write()
            .expect("Failed to acquire write lock on SegmentManager.")
    }

    pub fn commit(&self, segment_entries: Vec<SegmentEntry>) {
        let mut registers_lock = self.write();
        registers_lock.committed.clear();
        registers_lock.uncommitted.clear();
        for segment_entry in segment_entries {
            registers_lock.committed.add_segment_entry(segment_entry);
        }
    }

    pub fn start_merge(&self, segment_ids: &[SegmentId]) {
        let mut registers_lock = self.write();
        if registers_lock.uncommitted.contains_all(segment_ids) {
            for segment_id in segment_ids {
                registers_lock.uncommitted.start_merge(segment_id);
            }
        } else if registers_lock.committed.contains_all(segment_ids) {
            for segment_id in segment_ids {
                registers_lock.committed.start_merge(segment_id);
            }
        } else {
            error!("Merge operation sent for segments that are not all uncommited or commited.");
        }
    }

    pub fn cancel_merge(
        &self,
        before_merge_segment_ids: &[SegmentId],
        after_merge_segment_id: SegmentId,
    ) {
        let mut registers_lock = self.write();

        // we mark all segments are ready for merge.
        {
            let target_segment_register: &mut SegmentRegister;
            target_segment_register = {
                if registers_lock
                    .uncommitted
                    .contains_all(before_merge_segment_ids)
                {
                    &mut registers_lock.uncommitted
                } else if registers_lock
                    .committed
                    .contains_all(before_merge_segment_ids)
                {
                    &mut registers_lock.committed
                } else {
                    warn!("couldn't find segment in SegmentManager");
                    return;
                }
            };
            for segment_id in before_merge_segment_ids {
                target_segment_register.cancel_merge(segment_id);
            }
        }

        // ... and we make sure the target segment entry
        // can be garbage collected.
        registers_lock.writing.remove(&after_merge_segment_id);
    }

    pub fn write_segment(&self, segment_id: SegmentId) {
        let mut registers_lock = self.write();
        registers_lock.writing.insert(segment_id);
    }

    pub fn add_segment(&self, segment_entry: SegmentEntry) {
        let mut registers_lock = self.write();
        registers_lock.writing.remove(&segment_entry.segment_id());
        registers_lock.uncommitted.add_segment_entry(segment_entry);
    }

    pub fn end_merge(
        &self,
        before_merge_segment_ids: &[SegmentId],
        after_merge_segment_entry: SegmentEntry,
    ) {
        let mut registers_lock = self.write();
        registers_lock
            .writing
            .remove(&after_merge_segment_entry.segment_id());

        let target_register: &mut SegmentRegister = {
            if registers_lock
                .uncommitted
                .contains_all(before_merge_segment_ids)
            {
                &mut registers_lock.uncommitted
            } else if registers_lock
                .committed
                .contains_all(before_merge_segment_ids)
            {
                &mut registers_lock.committed
            } else {
                warn!("couldn't find segment in SegmentManager");
                return;
            }
        };
        for segment_id in before_merge_segment_ids {
            target_register.remove_segment(segment_id);
        }
        target_register.add_segment_entry(after_merge_segment_entry);
    }

    pub fn committed_segment_metas(&self) -> Vec<SegmentMeta> {
        let registers_lock = self.read();
        registers_lock.committed.segment_metas()
    }
}
