//! Edge index serialization.
//!
//! Stores flattened polygon vertices for query-time crossing tests. Vertices stream through
//! immediately on insert because merge cannot accumulate an entire segment's vertex data in
//! memory. The skip list directory records every Nth geometry offset.

use std::io::Write;
use std::marker::PhantomData;

use common::CountingWriter;

use crate::directory::WritePtr;
use crate::spatial::geometry_set::GeometrySet;
use crate::spatial::surface::Surface;

/// Streams geometry entries to disk. The Surface type parameter determines how many f64
/// coordinates are written per vertex.
pub struct EdgeWriter<'a, S: Surface> {
    write: &'a mut CountingWriter<WritePtr>,
    offsets: Vec<u64>,
    geometry_count: u32,
    skip_interval: u32,
    base: u64,
    _surface: PhantomData<S>,
}

impl<'a, S: Surface> EdgeWriter<'a, S> {
    /// Creates a new writer backed by the given output.
    pub fn new(write: &'a mut CountingWriter<WritePtr>, skip_interval: u32) -> Self {
        let base = write.written_bytes();
        EdgeWriter {
            write,
            offsets: Vec::new(),
            geometry_count: 0,
            skip_interval,
            base,
            _surface: PhantomData,
        }
    }

    /// Write all geometries for one document from a smashed GeometrySet.
    pub fn insert(&mut self, set: &GeometrySet) {
        for (member_idx, member) in set.members.iter().enumerate() {
            let pos = self.write.written_bytes() - self.base;

            if self.geometry_count % self.skip_interval == 0 {
                self.offsets.push(pos);
            }
            self.geometry_count += 1;

            let is_head = member_idx == 0;
            let has_holes = member.ring_offsets.len() > 2;

            let flags: u8 = (if member.closed { 0x01 } else { 0 })
                | (if member.contains_hilbert_start {
                    0x02
                } else {
                    0
                })
                | (if has_holes { 0x04 } else { 0 })
                | (if is_head { 0x08 } else { 0 });

            let ring_boundary_bytes = if has_holes {
                (member.ring_offsets.len() - 2) * 4
            } else {
                0
            };
            let vertex_bytes = member.vertices.len() * S::DIMENSIONS * 8;
            let data_bytes = (ring_boundary_bytes + vertex_bytes) as u32;

            self.write.write_all(&[flags]).unwrap();
            self.write.write_all(&data_bytes.to_le_bytes()).unwrap();
            if is_head {
                self.write.write_all(&set.doc_id.to_le_bytes()).unwrap();
            }

            if has_holes {
                let hole_count = member.ring_offsets.len() - 2;
                for (h, &offset) in member.ring_offsets[1..member.ring_offsets.len() - 1]
                    .iter()
                    .enumerate()
                {
                    let continuation = if h < hole_count - 1 { 0x80000000u32 } else { 0 };
                    let entry = continuation | (offset as u32);
                    self.write.write_all(&entry.to_le_bytes()).unwrap();
                }
            }

            for v in &member.vertices {
                for &coord in &v[..S::DIMENSIONS] {
                    self.write.write_all(&coord.to_le_bytes()).unwrap();
                }
            }
        }
    }

    /// Write the skip list directory and footer.
    pub fn finish(&mut self) {
        let dir_offset = self.write.written_bytes() - self.base;

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
