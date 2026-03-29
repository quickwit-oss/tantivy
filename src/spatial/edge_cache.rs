//! Cached geometry set access over one or more edge readers.
//!
//! Wraps EdgeReaders with an LRU cache keyed by (segment, head_position). The query path uses
//! segment 0 with a single reader. The merge path uses real segment indices with one reader per
//! source segment and a shared eviction budget.

use std::collections::HashMap;

use lru::LruCache;

use super::cell_index::GeometryId;
use super::edge_reader::EdgeReader;
use super::geometry_set::{EdgeSet, GeometrySet};
use super::surface::Surface;

struct CachedSet {
    head_position: u32,
    set: GeometrySet,
}

/// Cached geometry set access. Keyed by (segment, position) so the query path and merge path use
/// the same type. The query path passes segment 0. The merge path passes the real segment index.
pub struct EdgeCache<'a, S: Surface> {
    readers: Vec<EdgeReader<'a, S>>,
    index: HashMap<(usize, u32), (usize, u32)>,
    sets: LruCache<(usize, u32), CachedSet>,
    cached_vertices: usize,
    max_vertices: usize,
}

impl<'a, S: Surface> EdgeCache<'a, S> {
    /// Create a cache over the given readers. The max_vertices budget is shared across all
    /// segments. Eviction removes the least recently used set regardless of which segment it came
    /// from.
    pub fn new(readers: Vec<EdgeReader<'a, S>>, max_vertices: usize) -> Self {
        EdgeCache {
            readers,
            index: HashMap::new(),
            sets: LruCache::unbounded(),
            cached_vertices: 0,
            max_vertices,
        }
    }

    /// Total number of geometry entries in the given segment.
    pub fn geometry_count(&self, segment: usize) -> u32 {
        self.readers[segment].geometry_count()
    }

    /// Resolve a geometry to its doc_id without decoding vertices or touching the cache.
    pub fn doc_id_for(&self, id: GeometryId) -> u32 {
        self.readers[id.0 as usize].doc_id_for(id.1)
    }

    /// Retrieve the full geometry set containing the given geometry. Returns (head_position, set).
    /// Cached. The returned reference is valid until the next call that triggers eviction.
    pub fn get_geometry_set(&mut self, id: GeometryId) -> (u32, &GeometrySet) {
        let segment = id.0 as usize;
        let position = id.1;
        let key = (segment, position);
        if let Some(&head_key) = self.index.get(&key) {
            let cached = self.sets.get(&head_key).unwrap();
            return (cached.head_position, &cached.set);
        }

        let (head_position, set) = self.readers[segment].read_geometry_set(position);
        let head_key = (segment, head_position);

        let member_count = set.members.len() as u32;
        let vertex_count: usize = set.members.iter().map(|m| m.vertices.len()).sum();
        self.cached_vertices += vertex_count;

        for i in 0..member_count {
            self.index.insert((segment, head_position + i), head_key);
        }

        self.sets.put(head_key, CachedSet { head_position, set });

        while self.cached_vertices > self.max_vertices && self.sets.len() > 1 {
            if let Some((evicted_key, evicted_set)) = self.sets.pop_lru() {
                let evicted_verts: usize = evicted_set
                    .set
                    .members
                    .iter()
                    .map(|m| m.vertices.len())
                    .sum();
                self.cached_vertices -= evicted_verts;
                for i in 0..evicted_set.set.members.len() as u32 {
                    self.index.remove(&(evicted_key.0, evicted_key.1 + i));
                }
            }
        }

        let cached = self.sets.get(&head_key).unwrap();
        (cached.head_position, &cached.set)
    }

    /// Retrieve one member's edges and the document's doc_id.
    pub fn get_edge_set(&mut self, id: GeometryId) -> (u32, &EdgeSet) {
        let (head, set) = self.get_geometry_set(id);
        let member_idx = (id.1 - head) as usize;
        (set.doc_id, &set.members[member_idx])
    }
}
