//! Edge index serialization.
//!
//! Stores flattened polygon vertices for query-time crossing tests. Vertices stream through
//! immediately on insert because merge cannot accumulate an entire segment's vertex data in
//! memory. Separate offset and document arrays provide direct access by geometry ID.

use std::io::Write;
use std::marker::PhantomData;

use common::CountingWriter;

use crate::spatial::geometry_set::{GeometryKind, GeometrySet};
use crate::spatial::surface::Surface;

/// Streams geometry entries to disk. The Surface type parameter determines how many f64
/// coordinates are written per vertex.
pub struct EdgeWriter<'a, S: Surface, W: Write> {
    write: &'a mut CountingWriter<W>,
    offsets: Vec<u64>,
    doc_ids: Vec<u32>,
    value_heads: Vec<u32>,
    geometry_count: u32,
    base: u64,
    _surface: PhantomData<S>,
}

pub(crate) const EDGE_MEMBER_HEADER_BYTES: usize = 20;
pub(crate) const EDGE_MAGIC: &[u8; 4] = b"TEDG";
pub(crate) const EDGE_REVISION: u32 = 1;
pub(crate) const EDGE_FOOTER_BYTES: usize = 48;

impl<'a, S: Surface, W: Write> EdgeWriter<'a, S, W> {
    /// Creates a new writer backed by the given output.
    pub fn new(write: &'a mut CountingWriter<W>) -> Self {
        let base = write.written_bytes();
        EdgeWriter {
            write,
            offsets: Vec::new(),
            doc_ids: Vec::new(),
            value_heads: Vec::new(),
            geometry_count: 0,
            base,
            _surface: PhantomData,
        }
    }

    /// Write all geometries for one document from a smashed GeometrySet.
    pub fn insert(&mut self, set: &GeometrySet<S>) {
        let head = self.geometry_count;
        let member_count = set.members.len() as u32;
        if member_count > 0 {
            self.value_heads.push(head);
        }
        for member in &set.members {
            let pos = self.write.written_bytes() - self.base;
            let flags: u32 = (if member.closed { 0x01 } else { 0 })
                | (if member.contains_hilbert_start {
                    0x02
                } else {
                    0
                })
                | (if member.kind == GeometryKind::LineString {
                    0x04
                } else {
                    0
                });
            self.offsets.push(pos);
            self.doc_ids.push(set.doc_id);
            self.geometry_count += 1;

            for field in [
                head,
                member_count,
                member.vertices.len() as u32,
                member.ring_offsets.len().saturating_sub(1) as u32,
                flags,
            ] {
                self.write.write_all(&field.to_le_bytes()).unwrap();
            }

            for &offset in member
                .ring_offsets
                .iter()
                .skip(1)
                .take(member.ring_offsets.len().saturating_sub(2))
            {
                self.write
                    .write_all(&(offset as u32).to_le_bytes())
                    .unwrap();
            }

            for v in &member.vertices {
                for &coord in v.as_ref() {
                    self.write.write_all(&coord.to_le_bytes()).unwrap();
                }
            }
        }
    }

    /// Number of geometry entries written so far.
    pub fn geometry_count(&self) -> u32 {
        self.geometry_count
    }

    /// Write the member and document directories and footer.
    pub fn finish(&mut self) {
        let offsets_offset = self.write.written_bytes() - self.base;
        // Release offsets before allocating the document grouping buffers.
        for offset in std::mem::take(&mut self.offsets) {
            self.write.write_all(&offset.to_le_bytes()).unwrap();
        }
        let doc_ids_offset = self.write.written_bytes() - self.base;
        for doc_id in &self.doc_ids {
            self.write.write_all(&doc_id.to_le_bytes()).unwrap();
        }

        let doc_dir_offset = self.write.written_bytes() - self.base;
        let doc_count = self
            .value_heads
            .iter()
            .map(|&head| self.doc_ids[head as usize] + 1)
            .max()
            .unwrap_or(0);
        let mut starts = vec![0u32; doc_count as usize + 1];
        for &head in &self.value_heads {
            starts[self.doc_ids[head as usize] as usize + 1] += 1;
        }
        for index in 1..starts.len() {
            starts[index] += starts[index - 1];
        }
        // Persist the starts before reusing them as scatter cursors.
        for start in &starts {
            self.write.write_all(&start.to_le_bytes()).unwrap();
        }
        let value_count = self.value_heads.len() as u32;
        let mut heads = vec![0u32; self.value_heads.len()];
        for head in std::mem::take(&mut self.value_heads) {
            let slot = &mut starts[self.doc_ids[head as usize] as usize];
            heads[*slot as usize] = head;
            *slot += 1;
        }
        drop(starts);
        drop(std::mem::take(&mut self.doc_ids));
        for head in heads {
            self.write.write_all(&head.to_le_bytes()).unwrap();
        }

        self.write
            .write_all(&self.geometry_count.to_le_bytes())
            .unwrap();
        self.write
            .write_all(&(EDGE_MEMBER_HEADER_BYTES as u32).to_le_bytes())
            .unwrap();
        self.write.write_all(&offsets_offset.to_le_bytes()).unwrap();
        self.write.write_all(&doc_ids_offset.to_le_bytes()).unwrap();
        self.write.write_all(&doc_count.to_le_bytes()).unwrap();
        self.write.write_all(&value_count.to_le_bytes()).unwrap();
        self.write.write_all(&doc_dir_offset.to_le_bytes()).unwrap();
        self.write.write_all(EDGE_MAGIC).unwrap();
        self.write.write_all(&EDGE_REVISION.to_le_bytes()).unwrap();
    }
}
