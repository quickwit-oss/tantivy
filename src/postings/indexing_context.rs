use crate::postings::stacker::MemoryArena;

/// IndexingContext contains all of the transient memory arenas
/// required for building the inverted index.
pub(crate) struct IndexingContext {
    /// Arena is a memory arena that stores posting lists / term frequencies / positions.
    pub arena: MemoryArena,
    pub arena_terms: MemoryArena,
}

impl IndexingContext {
    /// Create a new IndexingContext given the size of the term hash map.
    pub(crate) fn new() -> IndexingContext {
        IndexingContext {
            arena: MemoryArena::new(),
            arena_terms: MemoryArena::new(),
        }
    }

    /// Returns the memory usage for the inverted index memory arenas, in bytes.
    pub(crate) fn mem_usage(&self) -> usize {
        self.arena.mem_usage() + self.arena_terms.mem_usage()
    }
}
