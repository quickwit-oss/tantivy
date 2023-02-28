mod arena_hashmap;
mod expull;
mod memory_arena;

pub use self::arena_hashmap::{compute_table_size, ArenaHashMap};
pub use self::expull::ExpUnrolledLinkedList;
pub use self::memory_arena::{Addr, MemoryArena};

/// When adding an element in a `ArenaHashMap`, we get a unique id associated to the given key.
pub type UnorderedId = u64;
