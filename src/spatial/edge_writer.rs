//! Edge index serialization.
//!
//! Stores flattened polygon vertices for query-time crossing tests. Vertices stream through
//! immediately on insert because merge cannot accumulate an entire segment's vertex data in
//! memory. The skip list directory records every Nth geometry offset, with the position of the
//! head of the set that geometry belongs to.

use std::io::{self, Write};
use std::marker::PhantomData;

use common::CountingWriter;

use crate::directory::WritePtr;
use crate::spatial::geometry_set::GeometrySet;
use crate::spatial::surface::Surface;

/// Streams geometry entries to disk. The Surface type parameter determines how many f64
/// coordinates are written per vertex.
pub struct EdgeWriter<'a, S: Surface> {
    write: &'a mut CountingWriter<WritePtr>,
    /// Every Nth geometry's offset, and the position of its set's head.
    directory: Vec<(u64, u32)>,
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
            directory: Vec::new(),
            geometry_count: 0,
            skip_interval,
            base,
            _surface: PhantomData,
        }
    }

    /// Write all geometries for one document from a smashed GeometrySet.
    pub fn insert(&mut self, set: &GeometrySet) -> io::Result<()> {
        self.insert_for_doc(set, set.doc_id)
    }

    /// Write all geometries of a set under the given doc id rather than the set's own, as a merge
    /// does when it renumbers documents.
    pub fn insert_for_doc(&mut self, set: &GeometrySet, doc_id: u32) -> io::Result<()> {
        let head_position = self.geometry_count;
        for (member_idx, member) in set.members.iter().enumerate() {
            let pos = self.write.written_bytes() - self.base;

            if self.geometry_count % self.skip_interval == 0 {
                self.directory.push((pos, head_position));
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
            let data_bytes = u32::try_from(ring_boundary_bytes + vertex_bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "A geometry of {} vertices is too large",
                        member.vertices.len()
                    ),
                )
            })?;

            self.write.write_all(&[flags])?;
            self.write.write_all(&data_bytes.to_le_bytes())?;
            if is_head {
                self.write.write_all(&doc_id.to_le_bytes())?;
            }

            if has_holes {
                let hole_count = member.ring_offsets.len() - 2;
                for (h, &offset) in member.ring_offsets[1..member.ring_offsets.len() - 1]
                    .iter()
                    .enumerate()
                {
                    let continuation = if h < hole_count - 1 { 0x80000000u32 } else { 0 };
                    let entry = continuation | (offset as u32);
                    self.write.write_all(&entry.to_le_bytes())?;
                }
            }

            for v in &member.vertices {
                for &coord in &v[..S::DIMENSIONS] {
                    self.write.write_all(&coord.to_le_bytes())?;
                }
            }
        }
        Ok(())
    }

    /// Write the skip list directory and footer.
    pub fn finish(&mut self) -> io::Result<()> {
        let dir_offset = self.write.written_bytes() - self.base;

        for &(offset, head_position) in &self.directory {
            self.write.write_all(&offset.to_le_bytes())?;
            self.write.write_all(&head_position.to_le_bytes())?;
        }

        self.write.write_all(&self.geometry_count.to_le_bytes())?;
        self.write.write_all(&self.skip_interval.to_le_bytes())?;
        self.write.write_all(&dir_offset.to_le_bytes())?;
        Ok(())
    }
}
