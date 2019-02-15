use super::segment_register::SegmentRegister;
use core::SegmentId;
use core::SegmentMeta;
use core::META_FILEPATH;
use error::TantivyError;
use indexer::delete_queue::DeleteCursor;
use indexer::SegmentEntry;
use std::collections::hash_set::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use Result as TantivyResult;
use std::sync::Arc;
use std::collections::HashMap;

/// Provides a read-only view of the available segments.
#[derive(Clone)]
pub struct AvailableSegments {
    registers: Arc<RwLock<SegmentRegisters>>,
}

impl AvailableSegments {
    pub fn committed(&self) -> Vec<SegmentMeta> {
        self.registers
            .read()
            .unwrap()
            .committed
            .segment_metas()
    }

    pub fn soft_committed(&self) -> Vec<SegmentMeta> {
        self.registers
            .read()
            .unwrap()
            .soft_committed
            .segment_metas()
    }
}


struct SegmentRegisters {
    uncommitted: HashMap<SegmentId, SegmentEntry>,
    committed: SegmentRegister,
    /// soft commits can advance committed segment to a future delete
    /// opstamp.
    ///
    /// In that case the same `SegmentId` can appear in both `committed`
    /// and in `committed_in_the_future`.
    ///
    /// We do not consider these segments for merges.
    soft_committed: SegmentRegister,
    /// `DeleteCursor`, positionned on the soft commit.
    delete_cursor: DeleteCursor,
}

/// The segment manager stores the list of segments
/// as well as their state.
///
/// It guarantees the atomicity of the
/// changes (merges especially)
pub struct SegmentManager {
    registers: Arc<RwLock<SegmentRegisters>>
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
    in_merge_segment_ids: &HashSet<SegmentId>,
    segment_manager: &SegmentManager,
) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
    let registers_lock = segment_manager.read();
    (
        registers_lock
            .soft_committed
            .get_mergeable_segments(in_merge_segment_ids),
        registers_lock
            .uncommitted
            .values()
            .map(|segment_entry| segment_entry.meta())
            .filter(|segment_meta| {
                !in_merge_segment_ids.contains(&segment_meta.id())
            })
            .cloned()
            .collect::<Vec<_>>()
    )
}

impl SegmentManager {
    pub fn from_segments(
        segment_metas: Vec<SegmentMeta>,
        delete_cursor: &DeleteCursor,
        opstamp: u64,
    ) -> SegmentManager {
        SegmentManager {
            registers: Arc::new(RwLock::new(SegmentRegisters {
                uncommitted: HashMap::default(),
                committed: SegmentRegister::new(segment_metas.clone(), opstamp),
                soft_committed: SegmentRegister::new(segment_metas, opstamp),
                delete_cursor: delete_cursor.clone(),
            }))
        }
    }

    pub fn available_segments_view(&self) -> AvailableSegments {
        AvailableSegments {
            registers: self.registers.clone()
        }
    }

    /// List the files that are useful to the index.
    ///
    /// This does not include lock files, or files that are obsolete
    /// but have not yet been deleted by the garbage collector.
    pub fn list_files(&self) -> HashSet<PathBuf> {
        let mut files = HashSet::new();
        files.insert(META_FILEPATH.clone());
        for segment_meta in SegmentMeta::all() {
            files.extend(segment_meta.list_files());
        }
        files
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

    /// Deletes all empty segments
    fn remove_empty_segments(&self) {
        let mut registers_lock = self.write();
        registers_lock
            .committed
            .segment_metas()
            .iter()
            .filter(|segment_meta| segment_meta.num_docs() == 0)
            .for_each(|segment_meta| {
                registers_lock
                    .committed
                    .remove_segment(&segment_meta.id())
            });
        registers_lock
            .soft_committed
            .segment_metas()
            .iter()
            .filter(|segment_meta| segment_meta.num_docs() == 0)
            .for_each(|segment_meta| {
                registers_lock
                    .committed
                    .remove_segment(&segment_meta.id())
            });
    }

    /// Returns all of the segment entries (soft committed or uncommitted)
    pub fn segment_entries(&self) -> Vec<SegmentEntry> {
        let registers_lock = self.read();
        let mut segment_entries: Vec<SegmentEntry   > = registers_lock.uncommitted.values().cloned().collect();
        segment_entries.extend(registers_lock.soft_committed.segment_entries(&registers_lock.delete_cursor).into_iter());
        segment_entries
    }


    pub fn commit(&self, opstamp: u64, segment_entries: Vec<SegmentEntry>) {
        let mut registers_lock = self.write();
        registers_lock.uncommitted.clear();
        registers_lock
            .committed
            .set_commit(opstamp, segment_entries.clone());
        registers_lock
            .soft_committed
            .set_commit(opstamp, segment_entries);
        registers_lock.delete_cursor.skip_to(opstamp);
    }

    pub fn soft_commit(&self, opstamp: u64, segment_entries: Vec<SegmentEntry>) {
        let mut registers_lock = self.write();
        registers_lock.uncommitted.clear();
        registers_lock
            .soft_committed
            .set_commit(opstamp, segment_entries);
        registers_lock.delete_cursor.skip_to(opstamp);
    }

    /// Gets the list of segment_entries associated to a list of `segment_ids`.
    /// This method is used when starting a merge operations.
    ///
    /// Returns an error if some segments are missing, or if
    /// the `segment_ids` are not either all soft_committed or all
    /// uncommitted.
    pub fn start_merge(&self, segment_ids: &[SegmentId]) -> TantivyResult<Vec<SegmentEntry>> {
        let registers_lock = self.read();
        let mut segment_entries = vec![];
        if segment_ids.iter().all(|segment_id| registers_lock.uncommitted.contains_key(segment_id)) {
            for segment_id in segment_ids {
                let segment_entry = registers_lock.uncommitted
                    .get(segment_id)
                    .expect("Segment id not found {}. Should never happen because of the contains all if-block.");
                segment_entries.push(segment_entry.clone());
            }
        } else if registers_lock.soft_committed.contains_all(segment_ids) {
            for segment_id in segment_ids {
                let segment_entry = registers_lock.soft_committed
                    .get(segment_id, &registers_lock.delete_cursor)
                    .expect("Segment id not found {}. Should never happen because of the contains all if-block.");
                segment_entries.push(segment_entry);
            }
        } else {
            let error_msg = "Merge operation sent for segments that are not \
                             all uncommited or commited."
                .to_string();
            return Err(TantivyError::InvalidArgument(error_msg));
        }
        Ok(segment_entries)
    }

    pub fn add_segment(&self, segment_entry: SegmentEntry) {
        let mut registers_lock = self.write();
        registers_lock
            .uncommitted
            .insert(segment_entry.segment_id(), segment_entry);
    }

    pub fn end_merge(
        &self,
        before_merge_segment_ids: &[SegmentId],
        after_merge_segment_entry: SegmentEntry
    ) {
        let mut registers_lock = self.write();

        if before_merge_segment_ids.iter().all(|seg_id|
            registers_lock
                .uncommitted
                .contains_key(seg_id))
        {
            for segment_id in before_merge_segment_ids {
                registers_lock.uncommitted.remove(&segment_id);
            }
            registers_lock.uncommitted.insert(after_merge_segment_entry.segment_id(),
                                              after_merge_segment_entry);
        } else {
            registers_lock.committed.receive_merge(&before_merge_segment_ids, &after_merge_segment_entry);
            registers_lock.soft_committed.receive_merge(&before_merge_segment_ids, &after_merge_segment_entry)
        }
    }

    pub fn committed_segment_metas(&self) -> Vec<SegmentMeta> {
        self.remove_empty_segments();
        let registers_lock = self.read();
        registers_lock.committed.segment_metas()
    }
}
