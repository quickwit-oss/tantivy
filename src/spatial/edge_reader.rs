//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 16-byte footer at
//! the end locates the skip list directory, and the directory provides random access by geometry
//! position. Between skip list entries, a forward walk bridges the gap using entry len fields.
//!
//! Geometry entry format per 004_edge_index.md:
//!   len (u32): bit 31 = closed, bit 30 = contains_hilbert_start, bits 0-29 = data byte length
//!   set (u32): bit 31 = has_holes, bit 30 = is_member, bits 0-29 = count or back distance
//!   Head/standalone: [len] [set] [doc_id] [ring boundaries?] [vertex data]
//!   Member:          [len] [set] [ring boundaries?] [vertex data]

use std::collections::HashMap;

use lru::LruCache;

use super::geometry_set::{EdgeSet, GeometrySet};

/// A geometry entry's header as stored on disk.
struct EntryHeader {
    /// Byte length of data section (bits 0-29 of len word). Includes ring boundary entries
    /// and vertex data.
    data_len: u32,
    /// Whether this geometry is closed (has interior). Bit 31 of len word.
    closed: bool,
    /// Whether this geometry's interior contains the Hilbert curve start point. Bit 30 of len word.
    contains_hilbert_start: bool,
    /// Whether ring boundary entries precede vertex data. Bit 31 of set word.
    has_holes: bool,
    /// Whether this is a member entry (not head or standalone). Bit 30 of set word.
    is_member: bool,
    /// Bits 0-29 of set word: member count (head/standalone) or byte distance back (member).
    set_value: u32,
}

/// Cached geometry set with the head position for member index resolution.
struct CachedSet {
    /// Position of the head entry for this set.
    head_position: u32,
    /// The decoded geometry set.
    set: GeometrySet,
}

/// Reads geometry entries from a serialized edge index segment. Constructed from a byte slice,
/// reads the footer to locate the skip list directory, and provides random access to vertices and
/// doc_ids by geometry position. Caches decoded geometry sets in an LRU evicted by total vertex
/// count so large polygons cost proportionally more than small ones.
pub struct EdgeReader<'a> {
    /// The raw bytes of the edge index segment, typically mmap'd.
    data: &'a [u8],
    /// Total number of geometry entries in this segment.
    geometry_count: u32,
    /// Every Nth geometry's offset is recorded in the skip list directory.
    skip_interval: u32,
    /// Byte offset where the skip list directory begins.
    dir_offset: u64,
    /// Maps any member position to the head's position for set lookup.
    index: HashMap<u32, u32>,
    /// LRU cache of decoded geometry sets, keyed by head position.
    sets: LruCache<u32, CachedSet>,
    /// Running total of vertices across all cached sets, for eviction decisions.
    cached_vertices: usize,
    /// Eviction kicks in when `cached_vertices` exceeds this bound.
    max_vertices: usize,
}

impl<'a> EdgeReader<'a> {
    /// Construct a reader from the raw bytes of an edge index segment. Reads the 16-byte footer to
    /// locate the skip list directory. `max_vertices` bounds the decode cache; eviction kicks in
    /// when the total cached vertex count exceeds this.
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

    /// Resolve a geometry position to its doc_id without decoding vertices or caching. Reads only
    /// the skip list, entry headers, and the doc_id field. Useful for checking a terms bitset
    /// before committing to a full geometry decode.
    pub fn doc_id_for(&self, position: u32) -> u32 {
        // Resolve byte offset via skip list + forward walk.
        let skip_index = (position / self.skip_interval) as usize;
        let remainder = position % self.skip_interval;

        let dir_entry = self.dir_offset as usize + skip_index * 8;
        let mut current =
            u64::from_le_bytes(self.data[dir_entry..dir_entry + 8].try_into().unwrap());

        if remainder > 0 {
            let mut steps = 0u32;
            while steps < remainder {
                let h = self.read_header(current);
                let advance = if h.is_member { 8 } else { 12 } + h.data_len as u64;
                current += advance;
                steps += 1;
            }
        }

        // Now at the target entry. Follow back pointers to the head if it's a member.
        let header = self.read_header(current);
        let head_offset = if header.is_member {
            let mut cur = current - header.set_value as u64;
            loop {
                let h = self.read_header(cur);
                if !h.is_member {
                    break cur;
                }
                cur -= h.set_value as u64;
            }
        } else {
            current
        };

        // Read the doc_id from the head entry: 4 bytes at head_offset + 8 (after len and set).
        u32::from_le_bytes(
            self.data[head_offset as usize + 8..head_offset as usize + 12]
                .try_into()
                .unwrap(),
        )
    }

    /// Retrieve one member's edges and the document's doc_id.
    pub fn get_edge_set(&mut self, position: u32) -> (u32, &EdgeSet) {
        let (head, set) = self.get_geometry_set(position);
        let member_idx = (position - head) as usize;
        (set.doc_id, &set.members[member_idx])
    }

    /// Retrieve the full geometry set containing the given position. Returns the head position
    /// and the set. Used by the merge path which needs all members.
    pub fn get_geometry_set(&mut self, position: u32) -> (u32, &GeometrySet) {
        debug_assert!(position < self.geometry_count);

        if let Some(&head) = self.index.get(&position) {
            let cached = self.sets.get(&head).unwrap();
            return (cached.head_position, &cached.set);
        }

        // Resolve the byte offset via skip list directory and forward walk.
        let skip_index = (position / self.skip_interval) as usize;
        let remainder = position % self.skip_interval;

        let dir_entry = self.dir_offset as usize + skip_index * 8;
        let mut current =
            u64::from_le_bytes(self.data[dir_entry..dir_entry + 8].try_into().unwrap());

        if remainder > 0 {
            let mut steps = 0u32;
            while steps < remainder {
                let h = self.read_header(current);
                let advance = if h.is_member { 8 } else { 12 } + h.data_len as u64;
                current += advance;
                steps += 1;
            }
        }

        // Resolve the set head and member count.
        let offset = current;
        let header = self.read_header(offset);

        let (head_position, head_offset, count) = if header.is_member {
            let mut cur = offset - header.set_value as u64;
            let mut hops = 1u32;
            loop {
                let h = self.read_header(cur);
                if !h.is_member {
                    break (position - hops, cur, h.set_value);
                }
                cur -= h.set_value as u64;
                hops += 1;
            }
        } else {
            (position, offset, header.set_value)
        };

        // Read the inline doc_id from the head entry (at head_offset + 8).
        let doc_id = u32::from_le_bytes(
            self.data[head_offset as usize + 8..head_offset as usize + 12]
                .try_into()
                .unwrap(),
        );

        // Decode all members.
        let mut members: Vec<EdgeSet> = Vec::with_capacity(count as usize);
        let mut cur = head_offset;
        for _ in 0..count {
            let h = self.read_header(cur);
            let data_start = if h.is_member { cur + 8 } else { cur + 12 };

            // Decode ring boundaries if has_holes, then vertex data.
            let (hole_starts, vertex_start) = if h.has_holes {
                self.decode_ring_boundaries(data_start as usize, h.data_len as usize)
            } else {
                (Vec::new(), data_start as usize)
            };

            let vertex_end = data_start as usize + h.data_len as usize;
            let vertices = self.decode_vertices(&self.data[vertex_start..vertex_end]);

            // Build the full ring_offsets vec: always [0, ..., vertex_count].
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
        }

        // Cache the set and evict if over budget.
        let vertex_count: usize = members.iter().map(|m| m.vertices.len()).sum();
        self.cached_vertices += vertex_count;

        for i in 0..count {
            self.index.insert(head_position + i, head_position);
        }

        self.sets.put(
            head_position,
            CachedSet {
                head_position,
                set: GeometrySet {
                    members,
                    doc_id,
                },
            },
        );

        while self.cached_vertices > self.max_vertices && self.sets.len() > 1 {
            if let Some((evicted_id, evicted_set)) = self.sets.pop_lru() {
                let evicted_verts: usize =
                    evicted_set.set.members.iter().map(|m| m.vertices.len()).sum();
                self.cached_vertices -= evicted_verts;
                for i in 0..evicted_set.set.members.len() as u32 {
                    self.index.remove(&(evicted_id + i));
                }
            }
        }

        let cached = self.sets.get(&head_position).unwrap();
        (cached.head_position, &cached.set)
    }

    fn read_header(&self, offset: u64) -> EntryHeader {
        let o = offset as usize;
        let raw_len = u32::from_le_bytes(self.data[o..o + 4].try_into().unwrap());
        let raw_set = u32::from_le_bytes(self.data[o + 4..o + 8].try_into().unwrap());
        EntryHeader {
            data_len: raw_len & 0x3FFFFFFF,
            closed: raw_len & 0x80000000 != 0,
            contains_hilbert_start: raw_len & 0x40000000 != 0,
            has_holes: raw_set & 0x80000000 != 0,
            is_member: raw_set & 0x40000000 != 0,
            set_value: raw_set & 0x3FFFFFFF,
        }
    }

    /// Decode ring boundary entries from the data section. Returns the hole ring start vertex
    /// indices and the byte offset where vertex data begins.
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

    // 24 bytes per vertex, 3 * f64.
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
