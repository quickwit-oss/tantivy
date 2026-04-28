//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 16-byte footer
//! locates the skip list directory. The directory provides random access by geometry position.
//! Between skip list entries, a forward walk bridges the gap.
//!
//! Geometry entry format per 004_edge_index.md:
//!   flags (u8): bit 0 = closed, bit 1 = contains_hilbert_start, bit 2 = has_holes, bit 3 = is_head
//!   len (u32): byte length of data section
//!   Head: [flags] [len] [doc_id] [ring boundaries?] [vertex data]
//!   Member: [flags] [len] [ring boundaries?] [vertex data]

use std::marker::PhantomData;

use super::geometry_set::{EdgeSet, GeometrySet};
use super::surface::Surface;

/// A located geometry entry. Holds a reference to the raw vertex data in the mmap'd slice.
/// Reads edges directly without allocation.
pub struct LocatedEntry<'a> {
    /// The document id for this geometry.
    pub doc_id: u32,
    /// Whether this geometry is a closed polygon.
    pub closed: bool,
    /// Raw vertex bytes in the mmap'd slice.
    pub vertex_data: &'a [u8],
    /// Number of vertices.
    pub vertex_count: usize,
}

impl<'a> LocatedEntry<'a> {
    /// Construct a located entry from its parts.
    pub fn new(doc_id: u32, closed: bool, vertex_data: &'a [u8], vertex_count: usize) -> Self {
        LocatedEntry {
            doc_id,
            closed,
            vertex_data,
            vertex_count,
        }
    }

    /// Number of vertices in this geometry member.
    pub fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    /// Read the vertex at the given index directly from the mmap'd data.
    pub fn vertex(&self, index: usize) -> [f64; 3] {
        let offset = index * 24;
        let mut point = [0.0f64; 3];
        for i in 0..3 {
            point[i] = f64::from_le_bytes(
                self.vertex_data[offset + i * 8..offset + (i + 1) * 8]
                    .try_into()
                    .unwrap(),
            );
        }
        point
    }

    /// Read the first vertex.
    pub fn first_vertex(&self) -> [f64; 3] {
        self.vertex(0)
    }

    /// Read the two endpoints of edge at the given index.
    pub fn edge(&self, index: u32) -> ([f64; 3], [f64; 3]) {
        let i = index as usize;
        let v0 = self.vertex(i);
        let v1 = if i + 1 < self.vertex_count {
            self.vertex(i + 1)
        } else {
            assert_eq!(
                i, 0,
                "edge index past vertex count is only valid for points"
            );
            v0
        };
        (v0, v1)
    }
}

struct EntryHeader {
    data_len: u32,
    closed: bool,
    contains_hilbert_start: bool,
    has_holes: bool,
    is_head: bool,
}

/// Reads geometry entries from a serialized edge index segment. Pure deserialization with no
/// mutable state. The Surface type parameter determines how many f64 coordinates are read per
/// vertex.
pub struct EdgeReader<'a, S: Surface> {
    data: &'a [u8],
    geometry_count: u32,
    skip_interval: u32,
    dir_offset: u64,
    _surface: PhantomData<S>,
}

impl<'a, S: Surface> EdgeReader<'a, S> {
    /// Construct a reader from the raw bytes of an edge index segment.
    pub fn open(data: &'a [u8]) -> Self {
        if data.len() < 16 {
            return EdgeReader {
                data,
                geometry_count: 0,
                skip_interval: 1,
                dir_offset: 0,
                _surface: PhantomData,
            };
        }
        let n = data.len();
        let dir_offset = u64::from_le_bytes(data[n - 8..n].try_into().unwrap());
        let skip_interval = u32::from_le_bytes(data[n - 12..n - 8].try_into().unwrap());
        let geometry_count = u32::from_le_bytes(data[n - 16..n - 12].try_into().unwrap());

        EdgeReader {
            data,
            geometry_count,
            skip_interval,
            dir_offset,
            _surface: PhantomData,
        }
    }

    /// The raw data slice backing this reader.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Total number of geometry entries in this segment.
    pub fn geometry_count(&self) -> u32 {
        self.geometry_count
    }

    /// Resolve a geometry position to its doc_id without decoding vertices.
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

            assert!(skip_index > 0, "doc_id_for: no head found");
            skip_index -= 1;
        }
    }

    /// Read the full geometry set containing the given position. Returns (head_position, set). No
    /// caching. Every call decodes from the byte stream.
    pub fn read_geometry_set(&self, position: u32) -> (u32, GeometrySet) {
        assert!(position < self.geometry_count);

        let (head_position, head_offset, doc_id) = self.find_head(position);

        let mut members: Vec<EdgeSet> = Vec::new();
        let mut cur = head_offset;
        let mut pos = head_position;
        loop {
            let h = self.read_header(cur);
            if h.is_head && !members.is_empty() {
                break;
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

        (head_position, GeometrySet { members, doc_id })
    }

    /// Locate a geometry entry and return its header, doc_id, and the byte offset where
    /// vertex data begins. Does not decode vertices.
    pub fn locate_entry(&self, position: u32) -> LocatedEntry<'_> {
        assert!(position < self.geometry_count);
        let (_, _, doc_id) = self.find_head(position);

        // Walk from the skip list entry to the target position.
        let skip_index = (position / self.skip_interval) as usize;
        let dir_entry = self.dir_offset as usize + skip_index * 8;
        let mut current =
            u64::from_le_bytes(self.data[dir_entry..dir_entry + 8].try_into().unwrap());
        let start_pos = skip_index as u32 * self.skip_interval;

        for _ in 0..(position - start_pos) {
            let h = self.read_header(current);
            let advance = if h.is_head { 9 } else { 5 } + h.data_len as u64;
            current += advance;
        }

        let h = self.read_header(current);
        let data_start = if h.is_head {
            current as usize + 9
        } else {
            current as usize + 5
        };

        let vertex_start = if h.has_holes {
            self.skip_ring_boundaries(data_start, h.data_len as usize)
        } else {
            data_start
        };

        let vertex_end = data_start + h.data_len as usize;
        let vertex_count = (vertex_end - vertex_start) / (S::DIMENSIONS * 8);

        LocatedEntry {
            doc_id,
            closed: h.closed,
            vertex_data: &self.data[vertex_start..vertex_end],
            vertex_count,
        }
    }

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

            assert!(skip_index > 0, "find_head: no head found");
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

    // Skip past ring boundary entries without allocating. Returns the byte offset where
    // vertex data begins.
    fn skip_ring_boundaries(&self, data_start: usize, data_len: usize) -> usize {
        let data_end = data_start + data_len;
        let mut pos = data_start;
        loop {
            assert!(pos + 4 <= data_end, "ring boundary overruns data section");
            let entry = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
            let continuation = entry & 0x80000000 != 0;
            pos += 4;
            if !continuation {
                break;
            }
        }
        pos
    }

    fn decode_ring_boundaries(&self, data_start: usize, data_len: usize) -> (Vec<usize>, usize) {
        let mut hole_starts = Vec::new();
        let mut pos = data_start;
        let data_end = data_start + data_len;
        loop {
            assert!(pos + 4 <= data_end, "ring boundary overruns data section");
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
        let vertex_size = S::DIMENSIONS * 8;
        data.chunks_exact(vertex_size)
            .map(|chunk| {
                let mut point = [0.0f64; 3];
                for i in 0..S::DIMENSIONS {
                    point[i] = f64::from_le_bytes(chunk[i * 8..(i + 1) * 8].try_into().unwrap());
                }
                point
            })
            .collect()
    }
}
