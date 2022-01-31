use crate::postings::stacker::{MemoryArena, TermHashMap};

/// IndexingContext contains all of the transient memory arenas
/// required for building the inverted index.
pub(crate) struct IndexingContext {
    /// The term index is an adhoc hashmap,
    /// itself backed by a dedicated memory arena.
    pub term_index: TermHashMap,
    /// Arena is a memory arena that stores posting lists / term frequencies / positions.
    pub arena: MemoryArena,
}

impl IndexingContext {
    /// Create a new IndexingContext given the size of the term hash map.
    pub(crate) fn new(table_size: usize) -> IndexingContext {
        let term_index = TermHashMap::new(table_size);
        IndexingContext {
            arena: MemoryArena::new(),
            term_index,
        }
    }

    /// Returns the memory usage for the inverted index memory arenas, in bytes.
    pub(crate) fn mem_usage(&self) -> usize {
        self.term_index.mem_usage() + self.arena.mem_usage()
    }
}
