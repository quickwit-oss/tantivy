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

    /// Write all geometries for one document. Each tuple carries (geometry_id, vertices, closed).
    /// Set indicators are derived from the slice length. Doc_id is inline after the set indicator
    /// on the head entry.
    pub fn insert(&mut self, doc_id: u32, geometries: &[(u32, &[[f64; 3]], bool)]) {
        let count = geometries.len();
        let mut prev_pos: u64 = 0;

        for (i, &(_geometry_id, vertices, closed)) in geometries.iter().enumerate() {
            let pos = self.write.written_bytes();

            if self.geometry_count % self.skip_interval == 0 {
                self.offsets.push(pos);
            }
            self.geometry_count += 1;

            let set_ind: i32 = if count == 1 {
                0
            } else if i == 0 {
                count as i32
            } else {
                let delta = pos - prev_pos;
                debug_assert!(delta <= i32::MAX as u64, "back pointer exceeds i32 range");
                -(delta as i32)
            };

            // 24 bytes per vertex, 3 * f64.
            let vertex_bytes = vertices.len() * 24;
            let len_word = (vertex_bytes as u32) | if closed { 0x80000000 } else { 0 };

            self.write.write_all(&len_word.to_le_bytes()).unwrap();
            self.write.write_all(&set_ind.to_le_bytes()).unwrap();
            if set_ind >= 0 {
                self.write.write_all(&doc_id.to_le_bytes()).unwrap();
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
