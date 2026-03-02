//! Cell index deserialization.
//!
//! Reads a serialized cell index from a byte slice. The footer at the end locates the dictionary,
//! and the dictionary provides random access by cell_id. The iterator walks cells in sorted order
//! for merge.

use crate::spatial::cell_index::{ClippedShape, IndexCell};
use crate::spatial::s2cell_id::S2CellId;

/// Reads a serialized cell index from a byte slice.
pub struct CellIndexReader<'a> {
    data: &'a [u8],
    cell_count: u32,
    dir_offset: u64,
}

impl<'a> CellIndexReader<'a> {
    /// Opens a cell index reader from the raw bytes of a serialized cell index.
    pub fn open(data: &'a [u8]) -> Self {
        let n = data.len();
        let dir_offset = u64::from_le_bytes(data[n - 8..n].try_into().unwrap());
        let cell_count = u32::from_le_bytes(data[n - 12..n - 8].try_into().unwrap());
        CellIndexReader {
            data,
            cell_count,
            dir_offset,
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
        // Find the first dictionary entry whose range_max >= target.range_min().
        let target_min = target.range_min();
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

        CellIndexIter {
            reader: self,
            pos: lo,
            end_at: Some(target.range_max()),
        }
    }

    fn read_dir_entry(&self, index: u32) -> (u64, u64) {
        let base = self.dir_offset as usize + index as usize * 16;
        let cell_id = u64::from_le_bytes(self.data[base..base + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(self.data[base + 8..base + 16].try_into().unwrap());
        (cell_id, offset)
    }

    fn read_cell(&self, offset: usize) -> IndexCell {
        let mut pos = offset;

        let cell_id = u64::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let shape_count = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut shapes = Vec::with_capacity(shape_count);
        for _ in 0..shape_count {
            let geometry_id = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            let contains_center = self.data[pos] != 0;
            pos += 1;

            let edge_count =
                u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            let mut edge_indices = Vec::with_capacity(edge_count);
            for _ in 0..edge_count {
                let edge_id = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap());
                pos += 2;
                edge_indices.push(edge_id);
            }

            shapes.push(ClippedShape {
                geometry_id,
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
