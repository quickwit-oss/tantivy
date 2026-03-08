//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 16-byte footer at
//! the end locates the skip list directory, and the directory provides random access by geometry
//! position. Between skip list entries, a forward walk bridges the gap using entry len fields.
//!
//! Doc_id footers (4 bytes each) sit between sets in the byte stream. The high bit distinguishes
//! them from entry headers: the first 4 bytes of an entry are the byte length (always positive),
//! while a doc_id footer has the high bit set (negative as i32). The forward walk reads one i32
//! and knows immediately whether it is an entry or a footer -- no set state tracking needed.

use std::collections::HashMap;

use lru::LruCache;

/// A geometry entry's header as stored on disk: u32 byte length of vertex data followed by i32
/// set indicator.
struct EntryHeader {
    len: u32,
    set: i32,
}

/// Decoded geometry set: all members' vertices and the doc_id. The head position identifies the
/// set -- member positions are consecutive starting from the head.
pub struct GeometrySet {
    /// The head position -- first member of the set in the edge index.
    pub geometry_id: u32,
    /// Decoded vertices for each member. Member at position P has its vertices at
    /// index `P - geometry_id`.
    pub vertices: Vec<Vec<[f64; 3]>>,
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
    /// all members, reads the doc_id footer, and caches the entire set. The caller's member is
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
            // Forward walk from the skip list entry point. The first 4 bytes
            // at each position distinguish entries from doc_id footers: a
            // positive i32 is a byte length (entry header), a negative i32
            // is a doc_id footer with the high bit set.
            let mut steps = 0u32;
            while steps < remainder {
                let first = i32::from_le_bytes(
                    self.data[current as usize..current as usize + 4]
                        .try_into()
                        .unwrap(),
                );
                if first < 0 {
                    current += 4;
                } else {
                    current += 8 + first as u64;
                    steps += 1;
                }
            }
            // Skip any doc_id footer between the last entry we passed and
            // the target entry. This happens at set boundaries where the
            // previous document's footer sits between two entries.
            while i32::from_le_bytes(
                self.data[current as usize..current as usize + 4]
                    .try_into()
                    .unwrap(),
            ) < 0
            {
                current += 4;
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

        // Decode all members and read the doc_id footer.
        let mut all_vertices: Vec<Vec<[f64; 3]>> = Vec::with_capacity(count as usize);
        let mut cur = head_offset;
        for _ in 0..count {
            let h = self.read_header(cur);
            let start = (cur + 8) as usize;
            let end = start + h.len as usize;
            all_vertices.push(self.decode_vertices(&self.data[start..end]));
            cur += 8 + h.len as u64;
        }

        let doc_id = u32::from_le_bytes(
            self.data[cur as usize..cur as usize + 4]
                .try_into()
                .unwrap(),
        ) & 0x7FFFFFFF;

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
        EntryHeader {
            len: u32::from_le_bytes(self.data[o..o + 4].try_into().unwrap()),
            set: i32::from_le_bytes(self.data[o + 4..o + 8].try_into().unwrap()),
        }
    }

    // 24 bytes per vertex, 3 * f64.
    fn decode_vertices(&self, data: &[u8]) -> Vec<[f64; 3]> {
        data.chunks_exact(24)
            .map(|chunk| {
                [
                    f64::from_le_bytes(chunk[0..8].try_into().unwrap()),
                    f64::from_le_bytes(chunk[8..16].try_into().unwrap()),
                    f64::from_le_bytes(chunk[16..24].try_into().unwrap()),
                ]
            })
            .collect()
    }
}
