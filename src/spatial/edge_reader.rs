//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 16-byte footer at
//! the end locates the skip list directory, and the directory provides random access by geometry
//! position. Between skip list entries, a forward walk bridges the gap using entry len fields.
//!
//! The high bit of the len field is a closed flag indicating whether the geometry has interior
//! (polygon). The actual byte length is len & 0x7FFFFFFF. Head and standalone entries (set >= 0)
//! carry an inline doc_id after the set indicator. Member entries (set < 0) do not.

use std::collections::HashMap;

use lru::LruCache;

/// A geometry entry's header as stored on disk.
struct EntryHeader {
    /// Byte length of vertex data (high bit masked off).
    len: u32,
    /// Set indicator: 0 = standalone, positive = head with member count, negative = back pointer.
    set: i32,
    /// Whether this geometry is closed (has interior).
    closed: bool,
}

/// Decoded geometry set: all members' vertices and the doc_id. The head position identifies the
/// set -- member positions are consecutive starting from the head.
pub struct GeometrySet {
    /// The head position -- first member of the set in the edge index.
    pub geometry_id: u32,
    /// Decoded vertices for each member. Member at position P has its vertices at
    /// index `P - geometry_id`.
    pub vertices: Vec<Vec<[f64; 3]>>,
    /// Whether each member is closed (has interior), parallel to `vertices`.
    pub closed: Vec<bool>,
    /// The document this set belongs to.
    pub doc_id: u32,
}

/// Reads geometry entries from a serialized edge index segment. Constructed from a byte slice,
/// reads the footer to locate the skip list directory, and provides random access to vertices and
/// doc_ids by geometry position. Caches decoded vertices in an LRU evicted by total vertex count
/// so large polygons cost proportionally more than small ones.
pub struct EdgeReader<'a> {
    /// The raw bytes of the edge index segment, typically mmap'd.
    data: &'a [u8],
    /// Total number of geometry entries in this segment.
    geometry_count: u32,
    /// Every Nth geometry's offset is recorded in the skip list directory.
    skip_interval: u32,
    /// Byte offset where the skip list directory begins.
    dir_offset: u64,
    /// Maps any member position to the head's geometry_id for set lookup.
    index: HashMap<u32, u32>,
    /// LRU cache of decoded geometry sets, keyed by geometry_id (head position).
    sets: LruCache<u32, GeometrySet>,
    /// Running total of vertices across all cached sets, for eviction decisions.
    cached_vertices: usize,
    /// Eviction kicks in when `cached_vertices` exceeds this bound.
    max_vertices: usize,
}

impl<'a> EdgeReader<'a> {
    /// Construct a reader from the raw bytes of an edge index segment. Reads the 16-byte footer to
    /// locate the skip list directory. `max_vertices` bounds the decode cache -- eviction kicks in
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

    /// Retrieve the geometry set containing the given position. On a cache miss, resolves the
    /// byte offset via the skip list directory, resolves the set head via back pointers, decodes
    /// all members, reads the inline doc_id, and caches the entire set. The caller's member is
    /// at index `position - set.geometry_id` in the returned `vertices` vec.
    pub fn get(&mut self, position: u32) -> &GeometrySet {
        debug_assert!(position < self.geometry_count);

        if let Some(&geometry_id) = self.index.get(&position) {
            return self.sets.get(&geometry_id).unwrap();
        }

        // Resolve the byte offset via skip list directory and forward walk.
        let skip_index = (position / self.skip_interval) as usize;
        let remainder = position % self.skip_interval;

        let dir_entry = self.dir_offset as usize + skip_index * 8;
        let mut current =
            u64::from_le_bytes(self.data[dir_entry..dir_entry + 8].try_into().unwrap());

        if remainder > 0 {
            // Forward walk from the skip list entry point. Every position is
            // a geometry entry. Read len (mask high bit) and set to determine
            // the entry size: head/standalone (set >= 0) has an inline doc_id
            // adding 4 bytes.
            let mut steps = 0u32;
            while steps < remainder {
                let h = self.read_header(current);
                let advance = if h.set >= 0 { 12 } else { 8 } + h.len as u64;
                current += advance;
                steps += 1;
            }
        }

        // Resolve the set head and member count.
        let offset = current;
        let header = self.read_header(offset);

        let (head_position, head_offset, count) = if header.set == 0 {
            (position, offset, 1)
        } else if header.set > 0 {
            (position, offset, header.set as u32)
        } else {
            let mut cur = offset - (-header.set) as u64;
            let mut hops = 1u32;
            loop {
                let h = self.read_header(cur);
                if h.set > 0 {
                    break (position - hops, cur, h.set as u32);
                } else if h.set == 0 {
                    break (position - hops, cur, 1);
                }
                cur -= (-h.set) as u64;
                hops += 1;
            }
        };

        // Read the inline doc_id from the head entry (at head_offset + 8).
        let doc_id = u32::from_le_bytes(
            self.data[head_offset as usize + 8..head_offset as usize + 12]
                .try_into()
                .unwrap(),
        );

        // Decode all members.
        let mut all_vertices: Vec<Vec<[f64; 3]>> = Vec::with_capacity(count as usize);
        let mut all_closed: Vec<bool> = Vec::with_capacity(count as usize);
        let mut cur = head_offset;
        for _ in 0..count {
            let h = self.read_header(cur);
            let data_offset = if h.set >= 0 { cur + 12 } else { cur + 8 };
            let start = data_offset as usize;
            let end = start + h.len as usize;
            all_vertices.push(self.decode_vertices(&self.data[start..end]));
            all_closed.push(h.closed);
            cur = data_offset + h.len as u64;
        }

        // Cache the set and evict if over budget.
        let vertex_count: usize = all_vertices.iter().map(|v| v.len()).sum();
        self.cached_vertices += vertex_count;

        for i in 0..count {
            self.index.insert(head_position + i, head_position);
        }

        self.sets.put(
            head_position,
            GeometrySet {
                geometry_id: head_position,
                vertices: all_vertices,
                closed: all_closed,
                doc_id,
            },
        );

        while self.cached_vertices > self.max_vertices && self.sets.len() > 1 {
            if let Some((evicted_id, evicted_set)) = self.sets.pop_lru() {
                let evicted_verts: usize = evicted_set.vertices.iter().map(|v| v.len()).sum();
                self.cached_vertices -= evicted_verts;
                for i in 0..evicted_set.vertices.len() as u32 {
                    self.index.remove(&(evicted_id + i));
                }
            }
        }

        self.sets.get(&head_position).unwrap()
    }

    fn read_header(&self, offset: u64) -> EntryHeader {
        let o = offset as usize;
        let raw_len = u32::from_le_bytes(self.data[o..o + 4].try_into().unwrap());
        EntryHeader {
            len: raw_len & 0x7FFFFFFF,
            set: i32::from_le_bytes(self.data[o + 4..o + 8].try_into().unwrap()),
            closed: raw_len & 0x80000000 != 0,
        }
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
