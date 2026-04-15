//! Cell index deserialization.
//!
//! Reads a serialized cell index from a byte slice. The footer at the end locates the dictionary,
//! and the dictionary provides random access by cell_id. The cursor provides seekable access for
//! the branch-and-bound traversal. The iterator walks cells in sorted order for merge.

use crate::spatial::clipped_shape::ClippedShape;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::shape_index::ShapeCell;

/// The relationship between a target cell and the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellRelation {
    /// The target is contained by an index cell (including equality).
    Indexed,
    /// The target contains one or more index cells.
    Subdivided,
    /// The target does not overlap any index cell.
    Disjoint,
}

/// Reads a serialized cell index from a byte slice.
pub struct CellIndexReader<'a> {
    data: &'a [u8],
    cell_count: u32,
    dir_offset: u64,
}

impl<'a> CellIndexReader<'a> {
    /// Opens a cell index reader from the raw bytes of a serialized cell index.
    pub fn open(data: &'a [u8]) -> Self {
        if data.len() < 12 {
            return CellIndexReader {
                data,
                cell_count: 0,
                dir_offset: 0,
            };
        }
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
    pub fn find(&self, target: S2CellId) -> Option<ShapeCell> {
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

    /// Returns a cursor positioned at the beginning of the cell index.
    pub fn cursor(&'a self) -> CellIndexCursor<'a> {
        CellIndexCursor {
            reader: self,
            pos: 0,
        }
    }

    fn read_dir_entry(&self, index: u32) -> (u64, u64) {
        let base = self.dir_offset as usize + index as usize * 16;
        let cell_id = u64::from_le_bytes(self.data[base..base + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(self.data[base + 8..base + 16].try_into().unwrap());
        (cell_id, offset)
    }

    fn read_cell(&self, offset: usize) -> ShapeCell {
        let mut pos = offset;

        let cell_id = u64::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let shape_count = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut shapes = Vec::with_capacity(shape_count);
        for _ in 0..shape_count {
            let geometry_id = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            let contains_center = self.data[pos] != 0;
            pos += 1;

            let edge_count =
                u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let mut edge_indices = Vec::with_capacity(edge_count);
            for _ in 0..edge_count {
                let edge_index = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                edge_indices.push(edge_index);
            }

            shapes.push(ClippedShape {
                geometry_id: (0, geometry_id),
                contains_center,
                edge_indices,
            });
        }

        ShapeCell {
            cell_id: S2CellId(cell_id),
            shapes,
        }
    }
}

/// Seekable cursor over the cell index dictionary. Provides positioned access without
/// deserialization and can produce an iterator to walk forward from the current position.
pub struct CellIndexCursor<'a> {
    reader: &'a CellIndexReader<'a>,
    pos: u32,
}

impl<'a> CellIndexCursor<'a> {
    /// Returns true if the cursor is past the last cell.
    pub fn done(&self) -> bool {
        self.pos >= self.reader.cell_count
    }

    /// Returns the cell id at the current position without deserializing the cell contents.
    /// Returns None if the cursor is done.
    pub fn id(&self) -> Option<S2CellId> {
        if self.done() {
            return None;
        }
        let (cell_id, _) = self.reader.read_dir_entry(self.pos);
        Some(S2CellId(cell_id))
    }

    /// Reads and deserializes the cell at the current position. Panics if the cursor is done.
    pub fn cell(&self) -> ShapeCell {
        assert!(!self.done());
        let (_, offset) = self.reader.read_dir_entry(self.pos);
        self.reader.read_cell(offset as usize)
    }

    /// Binary search forward to the first cell whose range_max is at or past the given target.
    pub fn seek(&mut self, target: S2CellId) {
        let mut lo = self.pos;
        let mut hi = self.reader.cell_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (cell_id, _) = self.reader.read_dir_entry(mid);
            if S2CellId(cell_id).range_max() < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.pos = lo;
    }

    /// Locate a target cell relative to the index.
    ///
    /// If the target is contained by some index cell (including equality), positions the cursor
    /// at that cell and returns Indexed. If the target contains one or more index cells, positions
    /// the cursor at the first such cell and returns Subdivided. Otherwise returns Disjoint.
    ///
    /// S2CellIterator::LocateImpl in s2cell_iterator.h.
    pub fn locate(&mut self, target: S2CellId) -> CellRelation {
        self.seek(target.range_min());
        if !self.done() {
            let id = self.id().unwrap();
            // The target is contained by the cell we landed on.
            if id >= target && id.range_min() <= target {
                return CellRelation::Indexed;
            }
            // The cell we landed on is contained by the target.
            if id <= target.range_max() {
                return CellRelation::Subdivided;
            }
        }
        // Check the previous cell. If it contains the target then it's indexed.
        if self.prev() && self.id().unwrap().range_max() >= target {
            return CellRelation::Indexed;
        }
        CellRelation::Disjoint
    }

    /// Move to the previous cell. Returns true if the move succeeded, false if already at the
    /// beginning.
    pub fn prev(&mut self) -> bool {
        if self.pos == 0 {
            return false;
        }
        self.pos -= 1;
        true
    }

    /// Returns an iterator that walks forward from the current position.
    pub fn iter_from_here(&self) -> CellIndexIter<'a> {
        CellIndexIter {
            reader: self.reader,
            pos: self.pos,
            end_at: None,
        }
    }

    /// Returns an iterator that walks forward from the current position, stopping when a cell's
    /// range_min exceeds the given bound.
    pub fn iter_until(&self, range_max: S2CellId) -> CellIndexIter<'a> {
        CellIndexIter {
            reader: self.reader,
            pos: self.pos,
            end_at: Some(range_max),
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
    type Item = ShapeCell;

    fn next(&mut self) -> Option<ShapeCell> {
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
