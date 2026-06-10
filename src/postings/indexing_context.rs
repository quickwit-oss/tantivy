use std::any::Any;

use fnv::FnvHashMap;
use stacker::{Addr, ArenaHashMap, MemoryArena};

use crate::indexer::path_to_unordered_id::PathToUnorderedId;

/// IndexingContext contains all of the transient memory arenas
/// required for building the inverted index.
#[doc(hidden)]
pub struct IndexingContext {
    /// The term index is an adhoc hashmap,
    /// itself backed by a dedicated memory arena.
    pub(crate) term_index: ArenaHashMap,
    /// Arena is a memory arena that stores posting lists / term frequencies / positions.
    pub(crate) arena: MemoryArena,
    pub(crate) path_to_unordered_id: PathToUnorderedId,
    /// Optional codec-specific payload attached to a term, keyed by the value
    /// `Addr` of the term's recorder in `term_index`.
    ///
    /// Hidden contract: keying on `Addr` is sound because a term's recorder
    /// address never changes once allocated (the arena only appends, and
    /// `subscribe` updates the recorder in place). The payload is therefore
    /// looked up by `Addr` at serialization time and fed to the codec's
    /// postings serializer at the beginning of the term.
    pub(crate) codec_term_payloads: FnvHashMap<Addr, Box<dyn Any + Send>>,
}

impl IndexingContext {
    /// Create a new IndexingContext given the size of the term hash map.
    pub(crate) fn new(table_size: usize) -> IndexingContext {
        let term_index = ArenaHashMap::with_capacity(table_size);
        IndexingContext {
            arena: MemoryArena::default(),
            term_index,
            path_to_unordered_id: PathToUnorderedId::default(),
            codec_term_payloads: FnvHashMap::default(),
        }
    }

    /// Returns the memory usage for the inverted index memory arenas, in bytes.
    pub(crate) fn mem_usage(&self) -> usize {
        self.term_index.mem_usage() + self.arena.mem_usage()
    }
}
