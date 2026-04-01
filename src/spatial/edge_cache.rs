//! Cached geometry set access over one or more edge readers.
//!
//! Wraps EdgeReaders with an LRU cache keyed by (segment, head_position). The query path uses
//! segment 0 with a single reader. The merge path uses real segment indices with one reader per
//! source segment and a shared eviction budget.
//!
//! The cache uses interior mutability so callers take &EdgeCache. Returns Rc<GeometrySet> so the
//! data survives eviction. The vertex budget is advisory.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use lru::LruCache;

use super::cell_index::GeometryId;
use super::edge_reader::EdgeReader;
use super::geometry_set::{EdgeSet, GeometrySet};
use super::surface::Surface;

struct CachedSet {
    head_position: u32,
    set: Rc<GeometrySet>,
}

struct CacheInner<'a, S: Surface> {
    readers: Vec<EdgeReader<'a, S>>,
    index: HashMap<(usize, u32), (usize, u32)>,
    sets: LruCache<(usize, u32), CachedSet>,
    cached_vertices: usize,
    max_vertices: usize,
}

/// A geometry set entry returned from the cache. Holds an Rc to the set and knows its member
/// index within the set. The Rc keeps the data alive even if the cache evicts the entry.
pub struct GeometryEntry {
    set: Rc<GeometrySet>,
    head: u32,
    member_idx: usize,
}

impl GeometryEntry {
    /// The vertices for this member.
    pub fn vertices(&self) -> &Vec<[f64; 3]> {
        &self.set.members[self.member_idx].vertices
    }

    /// The full edge set for this member.
    pub fn edge_set(&self) -> &EdgeSet {
        &self.set.members[self.member_idx]
    }

    /// The document id.
    pub fn doc_id(&self) -> u32 {
        self.set.doc_id
    }

    /// The head position of the set this member belongs to.
    pub fn head(&self) -> u32 {
        self.head
    }

    /// The number of members in the geometry set.
    pub fn member_count(&self) -> u32 {
        self.set.members.len() as u32
    }

    /// The full geometry set.
    pub fn geometry_set(&self) -> &GeometrySet {
        &self.set
    }

    /// The two endpoints of the given edge. For point geometries (single vertex), returns the
    /// vertex twice.
    pub fn edge(&self, index: u16) -> ([f64; 3], [f64; 3]) {
        let vertices = self.vertices();
        let i = index as usize;
        let v0 = vertices[i];
        let v1 = if i + 1 < vertices.len() {
            vertices[i + 1]
        } else {
            debug_assert_eq!(
                i, 0,
                "edge index past vertex count is only valid for points"
            );
            v0
        };
        (v0, v1)
    }
}

/// Cached geometry set access. Keyed by (segment, position) so the query path and merge path use
/// the same type. The query path passes segment 0. The merge path passes the real segment index.
/// Uses interior mutability so callers take &EdgeCache.
pub struct EdgeCache<'a, S: Surface> {
    inner: RefCell<CacheInner<'a, S>>,
}

impl<'a, S: Surface> EdgeCache<'a, S> {
    /// Create a cache over the given readers. The max_vertices budget is shared across all
    /// segments. Eviction removes the least recently used set regardless of which segment it came
    /// from.
    pub fn new(readers: Vec<EdgeReader<'a, S>>, max_vertices: usize) -> Self {
        EdgeCache {
            inner: RefCell::new(CacheInner {
                readers,
                index: HashMap::new(),
                sets: LruCache::unbounded(),
                cached_vertices: 0,
                max_vertices,
            }),
        }
    }

    /// Total number of geometry entries in the given segment.
    pub fn geometry_count(&self, segment: usize) -> u32 {
        self.inner.borrow().readers[segment].geometry_count()
    }

    /// Resolve a geometry to its doc_id without decoding vertices or touching the cache.
    pub fn doc_id_for(&self, id: GeometryId) -> u32 {
        self.inner.borrow().readers[id.0 as usize].doc_id_for(id.1)
    }

    /// Retrieve the geometry entry for the given geometry id. The returned entry holds an Rc to
    /// the geometry set and knows its member index. The Rc keeps the data alive regardless of
    /// cache eviction.
    pub fn get(&self, id: GeometryId) -> GeometryEntry {
        let mut inner = self.inner.borrow_mut();
        let segment = id.0 as usize;
        let position = id.1;
        let key = (segment, position);

        let head_key = if let Some(&hk) = inner.index.get(&key) {
            hk
        } else {
            let (head_position, set) = inner.readers[segment].read_geometry_set(position);
            let head_key = (segment, head_position);

            let member_count = set.members.len() as u32;
            let vertex_count: usize = set.members.iter().map(|m| m.vertices.len()).sum();
            inner.cached_vertices += vertex_count;

            for i in 0..member_count {
                inner.index.insert((segment, head_position + i), head_key);
            }

            inner.sets.put(
                head_key,
                CachedSet {
                    head_position,
                    set: Rc::new(set),
                },
            );

            while inner.cached_vertices > inner.max_vertices && inner.sets.len() > 1 {
                if let Some((evicted_key, evicted_set)) = inner.sets.pop_lru() {
                    let evicted_verts: usize = evicted_set
                        .set
                        .members
                        .iter()
                        .map(|m| m.vertices.len())
                        .sum();
                    inner.cached_vertices -= evicted_verts;
                    for i in 0..evicted_set.set.members.len() as u32 {
                        inner.index.remove(&(evicted_key.0, evicted_key.1 + i));
                    }
                }
            }

            head_key
        };

        let cached = inner.sets.get(&head_key).unwrap();
        let head = cached.head_position;
        let member_idx = (position - head) as usize;
        GeometryEntry {
            set: Rc::clone(&cached.set),
            head,
            member_idx,
        }
    }
}
