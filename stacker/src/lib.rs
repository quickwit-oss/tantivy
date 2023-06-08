#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(all(test, feature = "unstable"))]
extern crate test;

mod arena_hashmap;
mod expull;
#[allow(dead_code)]
mod fastcmp;
mod fastcpy;
mod memory_arena;

pub use self::arena_hashmap::{compute_table_memory_size, ArenaHashMap};
pub use self::expull::ExpUnrolledLinkedList;
pub use self::memory_arena::{Addr, MemoryArena};

/// When adding an element in a `ArenaHashMap`, we get a unique id associated to the given key.
pub type UnorderedId = u32;
