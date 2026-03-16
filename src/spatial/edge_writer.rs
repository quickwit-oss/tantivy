//! Edge index serialization.
//!
//! The edge index stores flattened polygon vertices so the reader can retrieve them at query time
//! for crossing tests. During build, `CellIndex` has the vertices in hand and `FaceEdge` copies
//! them -- the writer exists purely for persistence, never for build-time lookup.
//!
//! Vertices stream through the `CountingWriter` immediately on insert because merge cannot
//! accumulate an entire segment's vertex data in memory (12 GB at 10M geometries with 50 vertices
//! each). The skip list directory records every Nth geometry offset to keep the in-memory vec
//! small -- 5 MB at skip 16 instead of 80 MB for a full directory.

use std::io::Write;

use common::CountingWriter;

use crate::directory::WritePtr;
use crate::spatial::geometry_set::GeometrySet;

/// Streams geometry entries to disk. Vertices write through immediately. `offsets` records every
/// Nth geometry's start position for the skip list directory, where N is the skip interval.
pub struct EdgeWriter<'a> {
    write: &'a mut CountingWriter<WritePtr>,
    offsets: Vec<u64>,
    geometry_count: u32,
    skip_interval: u32,
}

impl<'a> EdgeWriter<'a> {
    /// Creates a new writer backed by the given output.
    pub fn new(write: &'a mut CountingWriter<WritePtr>, skip_interval: u32) -> Self {
        EdgeWriter {
            write,
            offsets: Vec::new(),
            geometry_count: 0,
            skip_interval,
        }
    }

    /// Write all geometries for one document from a smashed GeometrySet. Geometry IDs are arrival
    /// order — the writer counts. Set indicators, ring boundaries, and flags are derived from the
    /// GeometrySet fields. Doc_id is inline after the set field on the head entry.
    pub fn insert(&mut self, set: &GeometrySet) {
        let count = set.members.len();
        let mut prev_pos: u64 = 0;

        for (member_idx, member) in set.members.iter().enumerate() {
            let vertices = &member.vertices;
            let closed = member.closed;
            let contains_hilbert_start = member.contains_hilbert_start;
            let ring_offsets = &member.ring_offsets;

            let pos = self.write.written_bytes();

            if self.geometry_count % self.skip_interval == 0 {
                self.offsets.push(pos);
            }
            self.geometry_count += 1;

            // Ring boundary entries: one u32 per hole ring, written before vertex data.
            // Only present when there are holes (more than one ring, i.e. offsets len > 2).
            let has_holes = ring_offsets.len() > 2;
            let ring_boundary_bytes = if has_holes {
                (ring_offsets.len() - 2) * 4
            } else {
                0
            };

            // 24 bytes per vertex, 3 * f64.
            let vertex_bytes = vertices.len() * 24;
            let data_bytes = ring_boundary_bytes + vertex_bytes;
            let len_word = (data_bytes as u32)
                | if closed { 0x80000000 } else { 0 }
                | if contains_hilbert_start { 0x40000000 } else { 0 };

            // set field: bit 31 = has_holes, bit 30 = is_member, bits 0-29 = count or distance.
            let is_member = count > 1 && member_idx > 0;
            let set_word: u32 = if count == 1 {
                if has_holes { 0x80000000 | 1 } else { 1 }
            } else if member_idx == 0 {
                let c = count as u32;
                if has_holes { 0x80000000 | c } else { c }
            } else {
                let delta = pos - prev_pos;
                debug_assert!(delta <= 0x3FFFFFFF, "back pointer exceeds 30-bit range");
                let d = delta as u32;
                0x40000000 | if has_holes { 0x80000000 | d } else { d }
            };

            self.write.write_all(&len_word.to_le_bytes()).unwrap();
            self.write.write_all(&set_word.to_le_bytes()).unwrap();
            if !is_member {
                self.write.write_all(&set.doc_id.to_le_bytes()).unwrap();
            }

            // Write ring boundary entries: hole ring start indices with continuation flag.
            if has_holes {
                let hole_count = ring_offsets.len() - 2;
                for (h, &offset) in ring_offsets[1..ring_offsets.len() - 1].iter().enumerate() {
                    let continuation = if h < hole_count - 1 { 0x80000000u32 } else { 0 };
                    let entry = continuation | (offset as u32);
                    self.write.write_all(&entry.to_le_bytes()).unwrap();
                }
            }

            for v in vertices {
                for &coord in v {
                    self.write.write_all(&coord.to_le_bytes()).unwrap();
                }
            }

            prev_pos = pos;
        }
    }

    /// Write the skip list directory and footer.
    pub fn finish(&mut self) {
        let dir_offset = self.write.written_bytes();

        for &offset in &self.offsets {
            self.write.write_all(&offset.to_le_bytes()).unwrap();
        }

        self.write
            .write_all(&self.geometry_count.to_le_bytes())
            .unwrap();
        self.write
            .write_all(&self.skip_interval.to_le_bytes())
            .unwrap();
        self.write.write_all(&dir_offset.to_le_bytes()).unwrap();
    }
}
