//! Cell index deserialization.
//!
//! Reads a serialized cell index from a byte slice. The footer at the end locates the dictionary,
//! and the dictionary provides random access by cell_id. The iterator walks cells in sorted order
//! for merge.

use common::read_u32_vint_no_advance;

use crate::error::DataCorruption;
use crate::spatial::cell_index::{
    CellRelation, ClippedShape, IndexCell, DIRECTORY_ENTRY_LEN, FOOTER_LEN,
};
use crate::spatial::s2cell_id::S2CellId;

/// Reads a serialized cell index from a byte slice.
pub struct CellIndexReader<'a> {
    data: &'a [u8],
    cell_count: u32,
    dir_offset: u64,
}

impl<'a> CellIndexReader<'a> {
    /// Opens a cell index reader from the raw bytes of a serialized cell index. Fails when the
    /// footer does not locate a directory that ends where the footer begins.
    pub fn open(data: &'a [u8]) -> crate::Result<Self> {
        let corrupt =
            || DataCorruption::comment_only(format!("Corrupt {}-byte cell index", data.len()));
        let (body, footer) = data.split_last_chunk::<FOOTER_LEN>().ok_or_else(corrupt)?;
        let (cell_count, dir_offset) = footer.split_at(std::mem::size_of::<u32>());
        let cell_count = u32::from_le_bytes(cell_count.try_into().unwrap());
        let dir_offset = u64::from_le_bytes(dir_offset.try_into().unwrap());
        let dir_len = cell_count as u64 * DIRECTORY_ENTRY_LEN as u64;
        if dir_offset.checked_add(dir_len) != Some(body.len() as u64) {
            return Err(corrupt().into());
        }
        Ok(CellIndexReader {
            data,
            cell_count,
            dir_offset,
        })
    }

    /// A reader over no cells, for a segment that has no geometry for the field.
    pub fn empty() -> Self {
        CellIndexReader {
            data: &[],
            cell_count: 0,
            dir_offset: 0,
        }
    }

    /// Number of cells in the index.
    pub fn cell_count(&self) -> u32 {
        self.cell_count
    }

    /// Finds the cell containing the given cell_id using the S2 range containment test.
    pub fn find(&self, target: S2CellId) -> Option<IndexCell> {
        let mut lo = 0u32;
        let mut hi = self.cell_count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (cell_id, _) = self.read_dir_entry(mid);
            let cell_id = S2CellId(cell_id);

            if target < cell_id.range_min() {
                hi = mid;
            } else if target > cell_id.range_max() {
                lo = mid + 1;
            } else {
                let (_, offset) = self.read_dir_entry(mid);
                return Some(self.read_cell(offset as usize));
            }
        }

        None
    }

    /// Returns an iterator over all cells in sorted order.
    pub fn iter(&'a self) -> CellIndexIter<'a> {
        CellIndexIter {
            reader: self,
            pos: 0,
            end_at: None,
        }
    }

    /// Returns an iterator over all index cells whose ranges overlap the given target cell's
    /// range. This is the candidate collection step: for a covering cell, find all index cells
    /// that might contain relevant geometry.
    ///
    /// Binary search the dictionary for the first cell whose range_max is at or past the target's
    /// range_min, then yield cells forward until past the target's range_max.
    pub fn scan_range(&'a self, target: S2CellId) -> CellIndexIter<'a> {
        CellIndexIter {
            reader: self,
            pos: self.first_ending_at_or_after(target.range_min()),
            end_at: Some(target.range_max()),
        }
    }

    /// Locates a target cell relative to the index cells from the dictionary alone, without
    /// decoding any cell.
    pub fn locate(&self, target: S2CellId) -> CellRelation {
        let pos = self.first_ending_at_or_after(target.range_min());
        if pos >= self.cell_count {
            return CellRelation::Disjoint;
        }
        let (cell_id, _) = self.read_dir_entry(pos);
        let cell_id = S2CellId(cell_id);
        if cell_id.contains(target) {
            CellRelation::Indexed(pos as usize)
        } else if target.contains(cell_id) {
            CellRelation::Subdivided(pos as usize)
        } else {
            CellRelation::Disjoint
        }
    }

    /// Decodes the cell at the given position in cell order.
    pub fn cell(&self, position: usize) -> IndexCell {
        let (_, offset) = self.read_dir_entry(position as u32);
        self.read_cell(offset as usize)
    }

    /// Binary search the dictionary for the position of the first cell whose range_max is at or
    /// past the given cell id.
    fn first_ending_at_or_after(&self, target_min: S2CellId) -> u32 {
        let mut lo = 0u32;
        let mut hi = self.cell_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (cell_id, _) = self.read_dir_entry(mid);
            if S2CellId(cell_id).range_max() < target_min {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn read_dir_entry(&self, index: u32) -> (u64, u64) {
        let base = self.dir_offset as usize + index as usize * DIRECTORY_ENTRY_LEN;
        let cell_id = u64::from_le_bytes(self.data[base..base + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(self.data[base + 8..base + 16].try_into().unwrap());
        (cell_id, offset)
    }

    fn read_cell(&self, offset: usize) -> IndexCell {
        let mut pos = offset;

        let cell_id = u64::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let read_vint = |pos: &mut usize| {
            let (value, len) = read_u32_vint_no_advance(&self.data[*pos..]);
            *pos += len;
            value as usize
        };

        let shape_count = read_vint(&mut pos);

        let mut shapes = Vec::with_capacity(shape_count);
        for _ in 0..shape_count {
            let geometry_id = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            let contains_center = self.data[pos] != 0;
            pos += 1;

            let edge_count = read_vint(&mut pos);
            let edge_indices = (0..edge_count)
                .map(|_| read_vint(&mut pos) as u32)
                .collect();

            shapes.push(ClippedShape {
                geometry_id: (0, geometry_id),
                contains_center,
                edge_indices,
            });
        }

        IndexCell {
            cell_id: S2CellId(cell_id),
            shapes,
        }
    }
}

/// Iterates cells in sorted order, optionally bounded by a range_max.
pub struct CellIndexIter<'a> {
    reader: &'a CellIndexReader<'a>,
    pos: u32,
    /// When set, stop yielding cells whose range_min exceeds this value.
    end_at: Option<S2CellId>,
}

impl<'a> Iterator for CellIndexIter<'a> {
    type Item = IndexCell;

    fn next(&mut self) -> Option<IndexCell> {
        if self.pos >= self.reader.cell_count {
            return None;
        }
        let (cell_id, offset) = self.reader.read_dir_entry(self.pos);
        if let Some(max) = self.end_at {
            if S2CellId(cell_id).range_min() > max {
                return None;
            }
        }
        self.pos += 1;
        Some(self.reader.read_cell(offset as usize))
    }
}

#[cfg(test)]
mod tests {
    use common::CountingWriter;

    use super::*;
    use crate::spatial::cell_index::CellIndex;

    fn written(cell_index: &CellIndex) -> Vec<u8> {
        let mut write = CountingWriter::wrap(Vec::new());
        cell_index.write(&mut write).unwrap();
        write.finish()
    }

    fn cell(face: i32, geometry_id: u32) -> IndexCell {
        let mut cell = IndexCell::new(S2CellId::from_face(face));
        cell.add_shape(ClippedShape::new((0, geometry_id), true));
        cell
    }

    #[test]
    fn test_empty_reads_no_cells() {
        let reader = CellIndexReader::empty();
        assert_eq!(reader.cell_count(), 0);
        assert!(reader.iter().next().is_none());
        assert!(reader.find(S2CellId::from_face(0)).is_none());
        assert_eq!(
            reader.locate(S2CellId::from_face(0)),
            CellRelation::Disjoint
        );
    }

    #[test]
    fn test_open_reads_a_written_index() -> crate::Result<()> {
        let data = written(&CellIndex {
            cells: vec![cell(1, 7), cell(4, 9)],
        });
        let reader = CellIndexReader::open(&data)?;
        assert_eq!(reader.cell_count(), 2);
        let cells: Vec<IndexCell> = reader.iter().collect();
        assert_eq!(cells, vec![cell(1, 7), cell(4, 9)]);
        assert_eq!(reader.cell(1), cell(4, 9));

        let footer_only = written(&CellIndex::new());
        assert_eq!(footer_only.len(), FOOTER_LEN);
        assert_eq!(CellIndexReader::open(&footer_only)?.cell_count(), 0);
        Ok(())
    }

    #[test]
    fn test_counts_and_edge_ids_beyond_a_u16_round_trip() -> crate::Result<()> {
        let mut crowded = IndexCell::new(S2CellId::from_face(2));
        for geometry_id in 0..70_000 {
            crowded.add_shape(ClippedShape::new((0, geometry_id), geometry_id % 2 == 0));
        }
        let mut long = ClippedShape::new((0, 3), false);
        long.edge_indices = (0..70_000).chain([u32::MAX]).collect();
        crowded.add_shape(long);
        let data = written(&CellIndex {
            cells: vec![cell(1, 7), crowded.clone(), cell(4, 9)],
        });
        let reader = CellIndexReader::open(&data)?;
        let cells: Vec<IndexCell> = reader.iter().collect();
        assert_eq!(cells, vec![cell(1, 7), crowded, cell(4, 9)]);
        Ok(())
    }

    #[test]
    fn test_open_rejects_a_corrupt_index() {
        let data = written(&CellIndex {
            cells: vec![cell(1, 7)],
        });
        for len in 0..FOOTER_LEN {
            assert!(CellIndexReader::open(&data[..len]).is_err(), "{len} bytes");
        }
        assert!(CellIndexReader::open(&data[1..]).is_err());
        let mut longer = data.clone();
        longer.insert(0, 0);
        assert!(CellIndexReader::open(&longer).is_err());
    }

    #[test]
    fn test_locate_finds_containing_and_contained_cells() -> crate::Result<()> {
        let face_1 = S2CellId::from_face(1);
        let child = face_1.child(2);
        let data = written(&CellIndex {
            cells: vec![IndexCell::new(child)],
        });
        let reader = CellIndexReader::open(&data)?;
        assert_eq!(reader.locate(face_1), CellRelation::Subdivided(0));
        assert_eq!(reader.locate(child), CellRelation::Indexed(0));
        assert_eq!(reader.locate(child.child(3)), CellRelation::Indexed(0));
        assert_eq!(reader.locate(face_1.child(1)), CellRelation::Disjoint);
        assert_eq!(
            reader.locate(S2CellId::from_face(0)),
            CellRelation::Disjoint
        );
        Ok(())
    }
}
