use stacker::{ArenaHashMap, MemoryArena};

use crate::indexer::path_to_unordered_id::PathToUnorderedId;
use crate::postings::compute_table_memory_size;

/// Computes the initial size of the term hash table.
///
/// Returns the recommended initial table size as a power of 2.
///
/// Note this is a very dumb way to compute log2, but it is easier to proofread that way.
pub(crate) fn compute_initial_table_size(per_thread_memory_budget: usize) -> crate::Result<usize> {
    let table_memory_upper_bound = per_thread_memory_budget / 3;
    (10..20) // We cap it at 2^19 = 512K capacity.
        // TODO: There are cases where this limit causes a
        // reallocation in the hashmap. Check if this affects performance.
        .map(|power| 1 << power)
        .take_while(|capacity| compute_table_memory_size(*capacity) < table_memory_upper_bound)
        .last()
        .ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!(
                "per thread memory budget (={per_thread_memory_budget}) is too small. Raise the \
                 memory budget or lower the number of threads."
            ))
        })
}

/// IndexingContext contains all of the transient memory arenas
/// required for building the inverted index.
pub(crate) struct IndexingContext {
    /// The term index is an adhoc hashmap,
    /// itself backed by a dedicated memory arena.
    pub term_index: ArenaHashMap,
    /// Arena is a memory arena that stores posting lists / term frequencies / positions.
    pub arena: MemoryArena,
    pub path_to_unordered_id: PathToUnorderedId,
}

impl IndexingContext {
    /// Create a new IndexingContext given the size of the term hash map.
    pub(crate) fn new(table_size: usize) -> IndexingContext {
        let term_index = ArenaHashMap::with_capacity(table_size);
        IndexingContext {
            arena: MemoryArena::default(),
            term_index,
            path_to_unordered_id: PathToUnorderedId::default(),
        }
    }

    /// Returns the memory usage for the inverted index memory arenas, in bytes.
    pub(crate) fn mem_usage(&self) -> usize {
        self.term_index.mem_usage() + self.arena.mem_usage()
    }
}
