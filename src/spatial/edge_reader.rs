//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 48-byte footer
//! locates separate member-offset and document-ID arrays indexed by geometry position.
//!
//! Each member has a fixed header containing its value head, counts and flags, followed by
//! internal ring offsets and vertex coordinates. Document lookup does not read this payload.

use std::io;
use std::marker::PhantomData;

use super::edge_writer::{EDGE_FOOTER_BYTES, EDGE_MAGIC, EDGE_MEMBER_HEADER_BYTES, EDGE_REVISION};
use super::geometry_set::{EdgeSet, GeometryKind, GeometrySet};
use super::surface::Surface;

/// Iterates value heads from a document's mapped directory range.
pub struct ValueHeads<'a> {
    data: &'a [u8],
}

impl Iterator for ValueHeads<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }
        let head = u32::from_le_bytes(self.data[..4].try_into().unwrap());
        self.data = &self.data[4..];
        Some(head)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.data.len() / 4;
        (len, Some(len))
    }
}

impl ExactSizeIterator for ValueHeads<'_> {}

/// A located geometry entry. Holds a reference to the raw vertex data in the mmap'd slice.
/// Reads edges directly without allocation.
pub struct LocatedEntry<'a, S: Surface> {
    /// The document id for this geometry.
    pub doc_id: u32,
    /// The first member position of the owning value.
    pub head: u32,
    /// Number of members in the owning value.
    pub member_count: u32,
    /// The declared geometry kind.
    pub kind: GeometryKind,
    /// Whether this geometry is a closed polygon.
    pub closed: bool,
    /// Whether the Hilbert start lies inside this member.
    pub contains_hilbert_start: bool,
    /// Encoded internal ring boundaries.
    ring_data: &'a [u8],
    /// Raw vertex bytes in the mmap'd slice.
    pub vertex_data: &'a [u8],
    /// Number of vertices.
    pub vertex_count: usize,
    _surface: PhantomData<S>,
}

impl<'a, S: Surface> LocatedEntry<'a, S> {
    /// Number of vertices in this geometry member.
    pub fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    /// Read the vertex at the given index directly from the mmap'd data.
    pub fn vertex(&self, index: usize) -> S::Point {
        let offset = index * S::DIMENSIONS * 8;
        S::point_from_le_bytes(&self.vertex_data[offset..offset + S::DIMENSIONS * 8])
    }

    /// Read the two endpoints of edge at the given index.
    pub fn edge(&self, index: u32) -> (S::Point, S::Point) {
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

    /// Decode this member into its owned representation.
    pub fn to_edge_set(&self) -> EdgeSet<S> {
        let mut ring_offsets = vec![0];
        for entry in self.ring_data.chunks_exact(4) {
            ring_offsets.push(u32::from_le_bytes(entry.try_into().unwrap()) as usize);
        }
        ring_offsets.push(self.vertex_count);
        EdgeSet {
            kind: self.kind,
            vertices: (0..self.vertex_count)
                .map(|index| self.vertex(index))
                .collect(),
            closed: self.closed,
            contains_hilbert_start: self.contains_hilbert_start,
            ring_offsets,
        }
    }

    /// Test polygon containment using this member's stored rings.
    pub fn contains_point(&self, point: &S::Point) -> bool {
        if !self.closed {
            return false;
        }
        let mut start = 0;
        let mut ring_index = 0;
        let mut material = false;
        for end in self
            .ring_data
            .chunks_exact(4)
            .map(|entry| u32::from_le_bytes(entry.try_into().unwrap()) as usize)
            .chain(std::iter::once(self.vertex_count))
        {
            let len = end - start;
            let origin_inside = S::origin_inside_source(len, |index| self.vertex(start + index));
            let inside = S::contains_point_source(
                point,
                len,
                |index| self.vertex(start + index),
                origin_inside,
            );
            if S::RING_ROLES {
                material = if ring_index == 0 {
                    inside
                } else {
                    material && !inside
                };
            } else {
                material ^= inside;
            }
            start = end;
            ring_index += 1;
        }
        material
    }
}

/// Reads geometry entries from a serialized edge index segment. Pure deserialization with no
/// mutable state. The Surface type parameter determines how many f64 coordinates are read per
/// vertex.
pub struct EdgeReader<'a, S: Surface> {
    data: &'a [u8],
    geometry_count: u32,
    offsets_offset: u64,
    doc_ids_offset: u64,
    doc_count: u32,
    value_count: u32,
    doc_dir_offset: u64,
    _surface: PhantomData<S>,
}

impl<'a, S: Surface> EdgeReader<'a, S> {
    /// Construct a reader from the raw bytes of an edge index segment.
    pub fn open(data: &'a [u8]) -> io::Result<Self> {
        if data.is_empty() {
            return Ok(EdgeReader {
                data,
                geometry_count: 0,
                offsets_offset: 0,
                doc_ids_offset: 0,
                doc_count: 0,
                value_count: 0,
                doc_dir_offset: 0,
                _surface: PhantomData,
            });
        }
        let n = data.len();
        if n < EDGE_FOOTER_BYTES || &data[n - 8..n - 4] != EDGE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unrecognized spatial edge format; rebuild the spatial index",
            ));
        }
        let revision = u32::from_le_bytes(data[n - 4..n].try_into().unwrap());
        if revision != EDGE_REVISION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "spatial edge revision {revision} is unsupported (expected {EDGE_REVISION}); \
                     rebuild the spatial index"
                ),
            ));
        }
        let footer = n - EDGE_FOOTER_BYTES;
        let geometry_count = u32::from_le_bytes(data[footer..footer + 4].try_into().unwrap());
        let header_bytes = u32::from_le_bytes(data[footer + 4..footer + 8].try_into().unwrap());
        let offsets_offset = u64::from_le_bytes(data[footer + 8..footer + 16].try_into().unwrap());
        let doc_ids_offset = u64::from_le_bytes(data[footer + 16..footer + 24].try_into().unwrap());
        let doc_count = u32::from_le_bytes(data[footer + 24..footer + 28].try_into().unwrap());
        let value_count = u32::from_le_bytes(data[footer + 28..footer + 32].try_into().unwrap());
        let doc_dir_offset = u64::from_le_bytes(data[footer + 32..footer + 40].try_into().unwrap());
        let offsets_end = u64::from(geometry_count)
            .checked_mul(8)
            .and_then(|len| offsets_offset.checked_add(len));
        let doc_ids_end = u64::from(geometry_count)
            .checked_mul(4)
            .and_then(|len| doc_ids_offset.checked_add(len));
        let doc_dir_end = u64::from(doc_count)
            .checked_add(1)
            .and_then(|count| count.checked_add(u64::from(value_count)))
            .and_then(|count| count.checked_mul(4))
            .and_then(|len| doc_dir_offset.checked_add(len));
        if header_bytes as usize != EDGE_MEMBER_HEADER_BYTES
            || offsets_end != Some(doc_ids_offset)
            || doc_ids_end != Some(doc_dir_offset)
            || doc_dir_end != Some(footer as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid spatial edge directory; rebuild the spatial index",
            ));
        }

        let starts = doc_dir_offset as usize;
        let final_start = starts + doc_count as usize * 4;
        if u32::from_le_bytes(data[starts..starts + 4].try_into().unwrap()) != 0
            || u32::from_le_bytes(data[final_start..final_start + 4].try_into().unwrap())
                != value_count
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid spatial document directory; rebuild the spatial index",
            ));
        }

        Ok(EdgeReader {
            data,
            geometry_count,
            offsets_offset,
            doc_ids_offset,
            doc_count,
            value_count,
            doc_dir_offset,
            _surface: PhantomData,
        })
    }

    /// Total number of geometry entries in this segment.
    pub fn geometry_count(&self) -> u32 {
        self.geometry_count
    }

    /// Return the value heads owned by a document.
    pub fn value_heads(&self, doc_id: u32) -> io::Result<ValueHeads<'a>> {
        if doc_id >= self.doc_count {
            return Ok(ValueHeads {
                data: &self.data[..0],
            });
        }
        let starts = self.doc_dir_offset as usize;
        let start = self.read_u32(starts + doc_id as usize * 4) as usize;
        let end = self.read_u32(starts + (doc_id as usize + 1) * 4) as usize;
        if start > end || end > self.value_count as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid spatial document range; rebuild the spatial index",
            ));
        }
        let heads = starts + (self.doc_count as usize + 1) * 4;
        Ok(ValueHeads {
            data: &self.data[heads + start * 4..heads + end * 4],
        })
    }

    /// Resolve a geometry position to its doc_id without decoding vertices.
    pub fn doc_id_for(&self, position: u32) -> u32 {
        assert!(position < self.geometry_count);
        self.read_u32(self.doc_ids_offset as usize + position as usize * 4)
    }

    /// Read the full geometry set containing the given position. Returns (head_position, set). No
    /// caching. Every call decodes from the byte stream.
    pub fn read_geometry_set(&self, position: u32) -> (u32, GeometrySet<S>) {
        assert!(position < self.geometry_count);

        let entry = self.locate_entry(position);
        let head_position = entry.head;
        let doc_id = entry.doc_id;
        let member_count = entry.member_count;

        let mut members: Vec<EdgeSet<S>> = Vec::new();
        for pos in head_position..head_position + member_count {
            members.push(self.locate_entry(pos).to_edge_set());
        }

        (head_position, GeometrySet { members, doc_id })
    }

    /// Locate a geometry member's header and coordinate bytes without decoding vertices.
    pub fn locate_entry(&self, position: u32) -> LocatedEntry<'a, S> {
        assert!(position < self.geometry_count);
        let offset_start = self.offsets_offset as usize + position as usize * 8;
        let header_start = u64::from_le_bytes(
            self.data[offset_start..offset_start + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let data_start = header_start + EDGE_MEMBER_HEADER_BYTES;
        assert!(data_start <= self.offsets_offset as usize);
        let head = self.read_u32(header_start);
        let member_count = self.read_u32(header_start + 4);
        let vertex_count = self.read_u32(header_start + 8) as usize;
        let ring_count = self.read_u32(header_start + 12);
        let flags = self.read_u32(header_start + 16);
        let ring_bytes = ring_count.saturating_sub(1) as usize * 4;
        let vertex_start = data_start + ring_bytes;
        let vertex_end = vertex_start + vertex_count * S::DIMENSIONS * 8;
        assert!(vertex_end <= self.offsets_offset as usize);

        LocatedEntry {
            doc_id: self.doc_id_for(position),
            head,
            member_count,
            kind: if flags & 0x01 != 0 {
                GeometryKind::Polygon
            } else if flags & 0x04 != 0 {
                GeometryKind::LineString
            } else {
                GeometryKind::Point
            },
            closed: flags & 0x01 != 0,
            contains_hilbert_start: flags & 0x02 != 0,
            ring_data: &self.data[data_start..vertex_start],
            vertex_data: &self.data[vertex_start..vertex_end],
            vertex_count,
            _surface: PhantomData,
        }
    }

    fn read_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }
}
