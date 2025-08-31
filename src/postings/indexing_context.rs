use std::cell::RefCell;

use stacker::{ArenaHashMap, MemoryArena};

use crate::indexer::path_to_unordered_id::PathToUnorderedId;

thread_local! {
    static CONTEXT_POOL: RefCell<Vec<IndexingContext>> = RefCell::new(Vec::new());
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

impl Default for IndexingContext {
    fn default() -> Self {
        Self::create(1)
    }
}

impl IndexingContext {
    /// Gets an IndexingContext from the pool or creates a new one
    pub(crate) fn new(table_size: usize) -> IndexingContext {
        CONTEXT_POOL
            .with(|pool| pool.borrow_mut().pop())
            .unwrap_or_else(|| Self::create(table_size))
    }

    /// Returns the memory usage for the inverted index memory arenas, in bytes.
    pub(crate) fn mem_usage(&self) -> usize {
        self.term_index.mem_usage() + self.arena.mem_usage()
    }

    /// Create a new IndexingContext given the size of the term hash map.
    fn create(table_size: usize) -> IndexingContext {
        let term_index = ArenaHashMap::with_capacity(table_size);
        IndexingContext {
            arena: MemoryArena::default(),
            term_index,
            path_to_unordered_id: PathToUnorderedId::default(),
        }
    }

    pub fn checkin(mut ctx: IndexingContext) {
        CONTEXT_POOL.with(|pool| {
            ctx.term_index.reset();
            ctx.arena.reset();
            ctx.path_to_unordered_id = PathToUnorderedId::default();
            pool.borrow_mut().push(ctx);
        });
    }
}
