//! Edge index deserialization.
//!
//! Reads geometry entries from a byte slice, typically mmap'd segment data. The 16-byte footer
//! locates the skip list directory. The directory provides random access by geometry position,
//! and records for each entry the head of the set it belongs to, so that finding a head never
//! walks more than two skip intervals. Between skip list entries, a forward walk bridges the gap.
//!
//! Geometry entry format per 004_edge_index.md:
//!   flags (u8): bit 0 = closed, bit 1 = contains_hilbert_start, bit 2 = has_holes, bit 3 = is_head
//!   len (u32): byte length of data section
//!   Head: [flags] [len] [doc_id] [ring boundaries?] [vertex data]
//!   Member: [flags] [len] [ring boundaries?] [vertex data]

use std::marker::PhantomData;

use super::geometry_set::{EdgeSet, GeometrySet};
use super::surface::Surface;
use crate::error::DataCorruption;

/// Bytes in one skip list directory entry: the offset (u64) of a geometry entry and the position
/// (u32) of the head of its set.
const DIRECTORY_ENTRY_LEN: usize = std::mem::size_of::<u64>() + std::mem::size_of::<u32>();

/// Bytes in the footer: the geometry count (u32), the skip interval (u32) and the directory
/// offset (u64).
const FOOTER_LEN: usize = 2 * std::mem::size_of::<u32>() + std::mem::size_of::<u64>();

struct EntryHeader {
    data_len: u32,
    closed: bool,
    contains_hilbert_start: bool,
    has_holes: bool,
    is_head: bool,
}

impl EntryHeader {
    /// Bytes before the data section: the flags, the data length and, for a head, the doc id.
    fn header_len(&self) -> u64 {
        if self.is_head {
            9
        } else {
            5
        }
    }

    /// Bytes in the whole entry.
    fn entry_len(&self) -> u64 {
        self.header_len() + self.data_len as u64
    }
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
    /// Construct a reader from the raw bytes of an edge index segment. Fails when the footer does
    /// not locate a skip list directory that ends where the footer begins.
    pub fn open(data: &'a [u8]) -> crate::Result<Self> {
        let corrupt =
            || DataCorruption::comment_only(format!("Corrupt {}-byte edge index", data.len()));
        let (body, footer) = data.split_last_chunk::<FOOTER_LEN>().ok_or_else(corrupt)?;
        let geometry_count = u32::from_le_bytes(footer[0..4].try_into().unwrap());
        let skip_interval = u32::from_le_bytes(footer[4..8].try_into().unwrap());
        let dir_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        if skip_interval == 0 {
            return Err(corrupt().into());
        }
        let dir_len = geometry_count.div_ceil(skip_interval) as u64 * DIRECTORY_ENTRY_LEN as u64;
        if dir_offset.checked_add(dir_len) != Some(body.len() as u64) {
            return Err(corrupt().into());
        }

        Ok(EdgeReader {
            data,
            geometry_count,
            skip_interval,
            dir_offset,
            _surface: PhantomData,
        })
    }

    /// A reader over no geometries, for a segment that has no geometry for the field.
    pub fn empty() -> Self {
        EdgeReader {
            data: &[],
            geometry_count: 0,
            skip_interval: 1,
            dir_offset: 0,
            _surface: PhantomData,
        }
    }

    /// Total number of geometry entries in this segment.
    pub fn geometry_count(&self) -> u32 {
        self.geometry_count
    }

    /// Resolve a geometry position to its doc_id without decoding vertices.
    pub fn doc_id_for(&self, position: u32) -> u32 {
        let (_, head_offset) = self.find_head(position);
        self.read_doc_id(head_offset)
    }

    /// Read the full geometry set containing the given position. Returns (head_position, set). No
    /// caching. Every call decodes from the byte stream.
    pub fn read_geometry_set(&self, position: u32) -> (u32, GeometrySet) {
        debug_assert!(position < self.geometry_count);

        let (head_position, head_offset) = self.find_head(position);
        let doc_id = self.read_doc_id(head_offset);

        let mut members: Vec<EdgeSet> = Vec::new();
        let mut cur = head_offset;
        let mut pos = head_position;
        loop {
            let h = self.read_header(cur);
            if h.is_head && !members.is_empty() {
                break;
            }
            let data_start = cur + h.header_len();

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

    /// The position and offset of the head of the set containing the given position: the last
    /// head at or before it in its skip block, or else the head the directory records for the
    /// block, which starts inside that head's set.
    fn find_head(&self, position: u32) -> (u32, u64) {
        let skip_index = position / self.skip_interval;
        let (mut offset, block_head) = self.read_dir_entry(skip_index);
        let mut head = None;
        for pos in skip_index * self.skip_interval..=position {
            let h = self.read_header(offset);
            if h.is_head {
                head = Some((pos, offset));
            }
            offset += h.entry_len();
        }
        head.unwrap_or_else(|| (block_head, self.offset_of(block_head)))
    }

    /// The offset of the entry at the given position.
    fn offset_of(&self, position: u32) -> u64 {
        let skip_index = position / self.skip_interval;
        let (mut offset, _) = self.read_dir_entry(skip_index);
        for _ in skip_index * self.skip_interval..position {
            offset += self.read_header(offset).entry_len();
        }
        offset
    }

    /// The offset of the given skip block's first entry and the position of its set's head.
    fn read_dir_entry(&self, skip_index: u32) -> (u64, u32) {
        let at = self.dir_offset as usize + skip_index as usize * DIRECTORY_ENTRY_LEN;
        let offset = u64::from_le_bytes(self.data[at..at + 8].try_into().unwrap());
        let head_position = u32::from_le_bytes(self.data[at + 8..at + 12].try_into().unwrap());
        (offset, head_position)
    }

    /// The doc id of the head entry at the given offset.
    fn read_doc_id(&self, head_offset: u64) -> u32 {
        let at = head_offset as usize + 5;
        u32::from_le_bytes(self.data[at..at + 4].try_into().unwrap())
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use common::{CountingWriter, TerminatingWrite};

    use super::*;
    use crate::directory::{Directory, RamDirectory};
    use crate::spatial::edge_writer::EdgeWriter;
    use crate::spatial::geometry::Geometry;
    use crate::spatial::geometry_set::to_geometry_set;
    use crate::spatial::plane::Plane;
    use crate::spatial::sphere::Sphere;

    fn written(sets: &[GeometrySet]) -> crate::Result<Vec<u8>> {
        let directory = RamDirectory::create();
        let path = Path::new("edges");
        let mut write = CountingWriter::wrap(directory.open_write(path)?);
        let mut edge_writer = EdgeWriter::<Sphere>::new(&mut write, 2);
        for set in sets {
            edge_writer.insert(set)?;
        }
        edge_writer.finish()?;
        write.finish().terminate()?;
        Ok(directory.open_read(path)?.read_bytes()?.to_vec())
    }

    fn point(doc_id: u32) -> GeometrySet {
        let point = Geometry::<Plane>::Point([doc_id as f64, 0.0]).project::<Sphere>();
        to_geometry_set(&point, doc_id)
    }

    #[test]
    fn test_open_reads_a_written_index() -> crate::Result<()> {
        let data = written(&[point(3), point(5), point(8)])?;
        let reader = EdgeReader::<Sphere>::open(&data)?;
        assert_eq!(reader.geometry_count(), 3);
        assert_eq!(reader.doc_id_for(2), 8);
        let footer_only = written(&[])?;
        assert_eq!(footer_only.len(), FOOTER_LEN);
        assert_eq!(
            EdgeReader::<Sphere>::open(&footer_only)?.geometry_count(),
            0
        );
        Ok(())
    }

    fn points(doc_id: u32, count: usize) -> GeometrySet {
        let points = (0..count).map(|i| [i as f64, doc_id as f64]).collect();
        to_geometry_set(
            &Geometry::<Plane>::MultiPoint(points).project::<Sphere>(),
            doc_id,
        )
    }

    #[test]
    fn test_find_the_set_of_every_position() -> crate::Result<()> {
        // With a skip interval of 2, the long set spans several skip blocks.
        let sets = [points(3, 1), points(5, 9), points(8, 2), points(9, 1)];
        let data = written(&sets)?;
        let reader = EdgeReader::<Sphere>::open(&data)?;
        let mut position = 0;
        for set in &sets {
            let head = position;
            for member in &set.members {
                assert_eq!(
                    reader.doc_id_for(position),
                    set.doc_id,
                    "position {position}"
                );
                let (head_position, read) = reader.read_geometry_set(position);
                assert_eq!(head_position, head);
                assert_eq!(read.doc_id, set.doc_id);
                assert_eq!(read.members.len(), set.members.len());
                assert_eq!(
                    read.members[(position - head) as usize].vertices,
                    member.vertices
                );
                position += 1;
            }
        }
        assert_eq!(reader.geometry_count(), position);
        Ok(())
    }

    #[test]
    fn test_open_rejects_a_corrupt_index() -> crate::Result<()> {
        let data = written(&[point(3), point(5), point(8)])?;
        for len in 0..FOOTER_LEN {
            assert!(
                EdgeReader::<Sphere>::open(&data[..len]).is_err(),
                "{len} bytes"
            );
        }
        assert!(EdgeReader::<Sphere>::open(&data[1..]).is_err());
        let mut no_skip_interval = data.clone();
        let interval_at = data.len() - FOOTER_LEN + 4;
        no_skip_interval[interval_at..interval_at + 4].copy_from_slice(&0u32.to_le_bytes());
        assert!(EdgeReader::<Sphere>::open(&no_skip_interval).is_err());
        assert_eq!(EdgeReader::<Sphere>::empty().geometry_count(), 0);
        Ok(())
    }
}
