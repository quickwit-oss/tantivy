//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 16-byte footer
//! locates the skip list directory. The directory provides random access by geometry position.
//! Between skip list entries, a forward walk bridges the gap.
//!
//! Geometry entry format per 004_edge_index.md:
//!   flags (u8): bit 0 = closed, bit 1 = contains_origin, bit 2 = has_holes, bit 3 = is_head
//!   len (u32): byte length of data section
//!   Head: [flags] [len] [doc_id] [ring boundaries?] [vertex data]
//!   Member: [flags] [len] [ring boundaries?] [vertex data]

use std::collections::HashMap;

use lru::LruCache;

use super::geometry_set::{EdgeSet, GeometrySet};

struct EntryHeader {
    data_len: u32,
    closed: bool,
    contains_hilbert_start: bool,
    has_holes: bool,
    is_head: bool,
}

struct CachedSet {
    head_position: u32,
    set: GeometrySet,
}

/// Reads geometry entries from a serialized edge index segment.
pub struct EdgeReader<'a> {
    data: &'a [u8],
    geometry_count: u32,
    skip_interval: u32,
    dir_offset: u64,
    index: HashMap<u32, u32>,
    sets: LruCache<u32, CachedSet>,
    cached_vertices: usize,
    max_vertices: usize,
}

impl<'a> EdgeReader<'a> {
    /// Construct a reader from the raw bytes of an edge index segment. Reads the 16-byte footer
    /// to locate the skip list directory. `max_vertices` bounds the decode cache; eviction kicks
    /// in when the total cached vertex count exceeds this.
    pub fn open(data: &'a [u8], max_vertices: usize) -> Self {
        let n = data.len();
        let dir_offset = u64::from_le_bytes(data[n - 8..n].try_into().unwrap());
        let skip_interval = u32::from_le_bytes(data[n - 12..n - 8].try_into().unwrap());
        let geometry_count = u32::from_le_bytes(data[n - 16..n - 12].try_into().unwrap());

        EdgeReader {
            data,
            geometry_count,
            skip_interval,
            dir_offset,
            index: HashMap::new(),
            sets: LruCache::unbounded(),
            cached_vertices: 0,
            max_vertices,
        }
    }

    /// Total number of geometry entries in this segment.
    pub fn geometry_count(&self) -> u32 {
        self.geometry_count
    }

    /// Resolve a geometry position to its doc_id without decoding vertices or caching. Walks
    /// forward from the nearest skip list entry, noting doc_ids from head entries. If the target
    /// is reached without a head, backs up one skip list entry and retries.
    pub fn doc_id_for(&self, position: u32) -> u32 {
        let mut skip_index = (position / self.skip_interval) as usize;

        loop {
            let dir_entry = self.dir_offset as usize + skip_index * 8;
            let mut current =
                u64::from_le_bytes(self.data[dir_entry..dir_entry + 8].try_into().unwrap());
            let start_pos = skip_index as u32 * self.skip_interval;
            let mut doc_id: Option<u32> = None;

            for i in 0..=(position - start_pos) {
                let h = self.read_header(current);
                if h.is_head {
                    doc_id = Some(u32::from_le_bytes(
                        self.data[current as usize + 5..current as usize + 9]
                            .try_into()
                            .unwrap(),
                    ));
                }
                if start_pos + i == position {
                    if let Some(id) = doc_id {
                        return id;
                    }
                    break;
                }
                let advance = if h.is_head { 9 } else { 5 } + h.data_len as u64;
                current += advance;
            }

            // Head was before the skip list entry. Retry from the previous entry.
            debug_assert!(skip_index > 0, "doc_id_for: no head found");
            skip_index -= 1;
        }
    }

    /// Retrieve one member's edges and the document's doc_id.
    pub fn get_edge_set(&mut self, position: u32) -> (u32, &EdgeSet) {
        let (head, set) = self.get_geometry_set(position);
        let member_idx = (position - head) as usize;
        (set.doc_id, &set.members[member_idx])
    }

    /// Retrieve the full geometry set containing the given position. Returns the head position
    /// and the set.
    pub fn get_geometry_set(&mut self, position: u32) -> (u32, &GeometrySet) {
        debug_assert!(position < self.geometry_count);

        if let Some(&head) = self.index.get(&position) {
            let cached = self.sets.get(&head).unwrap();
            return (cached.head_position, &cached.set);
        }

        // Walk forward from the skip list to find the head and decode the set.
        let (head_position, head_offset, doc_id) = self.find_head(position);

        // Decode members by walking forward from the head until the next head or end.
        let mut members: Vec<EdgeSet> = Vec::new();
        let mut cur = head_offset;
        let mut pos = head_position;
        loop {
            if pos > position && members.is_empty() {
                break;
            }
            let h = self.read_header(cur);
            if !h.is_head && !members.is_empty() && pos > position {
                // Reached a member beyond the target and we have the set.
                // But we need to keep going until the next head to get all members.
            }
            if h.is_head && !members.is_empty() {
                break; // Next set starts here.
            }
            let data_start = if h.is_head { cur + 9 } else { cur + 5 };

            let (hole_starts, vertex_start) = if h.has_holes {
                self.decode_ring_boundaries(data_start as usize, h.data_len as usize)
            } else {
                (Vec::new(), data_start as usize)
            };

            let vertex_end = data_start as usize + h.data_len as usize;
            let vertices = self.decode_vertices(&self.data[vertex_start..vertex_end]);

            let mut offsets = vec![0usize];
            for &hole_start in &hole_starts {
                offsets.push(hole_start);
            }
            offsets.push(vertices.len());

            members.push(EdgeSet {
                vertices,
                closed: h.closed,
                contains_hilbert_start: h.contains_hilbert_start,
                ring_offsets: offsets,
            });

            cur = data_start + h.data_len as u64;
            pos += 1;

            if pos >= self.geometry_count {
                break;
            }
        }

        let member_count = members.len() as u32;
        let vertex_count: usize = members.iter().map(|m| m.vertices.len()).sum();
        self.cached_vertices += vertex_count;

        for i in 0..member_count {
            self.index.insert(head_position + i, head_position);
        }

        self.sets.put(
            head_position,
            CachedSet {
                head_position,
                set: GeometrySet { members, doc_id },
            },
        );

        while self.cached_vertices > self.max_vertices && self.sets.len() > 1 {
            if let Some((evicted_id, evicted_set)) = self.sets.pop_lru() {
                let evicted_verts: usize = evicted_set
                    .set
                    .members
                    .iter()
                    .map(|m| m.vertices.len())
                    .sum();
                self.cached_vertices -= evicted_verts;
                for i in 0..evicted_set.set.members.len() as u32 {
                    self.index.remove(&(evicted_id + i));
                }
            }
        }

        let cached = self.sets.get(&head_position).unwrap();
        (cached.head_position, &cached.set)
    }

    /// Walk forward from the skip list to find the head entry for the given position.
    /// Returns (head_position, head_byte_offset, doc_id).
    fn find_head(&self, position: u32) -> (u32, u64, u32) {
        let mut skip_index = (position / self.skip_interval) as usize;

        loop {
            let dir_entry = self.dir_offset as usize + skip_index * 8;
            let mut current =
                u64::from_le_bytes(self.data[dir_entry..dir_entry + 8].try_into().unwrap());
            let start_pos = skip_index as u32 * self.skip_interval;

            let mut head_position: Option<u32> = None;
            let mut head_offset: Option<u64> = None;
            let mut doc_id: Option<u32> = None;

            let end = std::cmp::min(position + 1, self.geometry_count);
            for i in 0..(end - start_pos) {
                let h = self.read_header(current);
                if h.is_head {
                    head_position = Some(start_pos + i);
                    head_offset = Some(current);
                    doc_id = Some(u32::from_le_bytes(
                        self.data[current as usize + 5..current as usize + 9]
                            .try_into()
                            .unwrap(),
                    ));
                }
                let advance = if h.is_head { 9 } else { 5 } + h.data_len as u64;
                current += advance;
            }

            if let (Some(hp), Some(ho), Some(id)) = (head_position, head_offset, doc_id) {
                return (hp, ho, id);
            }

            debug_assert!(skip_index > 0, "find_head: no head found");
            skip_index -= 1;
        }
    }

    fn read_header(&self, offset: u64) -> EntryHeader {
        let o = offset as usize;
        let flags = self.data[o];
        let data_len = u32::from_le_bytes(self.data[o + 1..o + 5].try_into().unwrap());
        EntryHeader {
            data_len,
            closed: flags & 0x01 != 0,
            contains_hilbert_start: flags & 0x02 != 0,
            has_holes: flags & 0x04 != 0,
            is_head: flags & 0x08 != 0,
        }
    }

    fn decode_ring_boundaries(&self, data_start: usize, data_len: usize) -> (Vec<usize>, usize) {
        let mut hole_starts = Vec::new();
        let mut pos = data_start;
        let data_end = data_start + data_len;
        loop {
            debug_assert!(pos + 4 <= data_end, "ring boundary overruns data section");
            let entry = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
            let vertex_index = (entry & 0x7FFFFFFF) as usize;
            let continuation = entry & 0x80000000 != 0;
            hole_starts.push(vertex_index);
            pos += 4;
            if !continuation {
                break;
            }
        }
        (hole_starts, pos)
    }

    fn decode_vertices(&self, data: &[u8]) -> Vec<[f64; 3]> {
        let mut vertices: Vec<[f64; 3]> = data
            .chunks_exact(24)
            .map(|chunk| {
                [
                    f64::from_le_bytes(chunk[0..8].try_into().unwrap()),
                    f64::from_le_bytes(chunk[8..16].try_into().unwrap()),
                    f64::from_le_bytes(chunk[16..24].try_into().unwrap()),
                ]
            })
            .collect();
        if vertices.len() == 1 {
            vertices.push(vertices[0]);
        }
        vertices
    }
}
