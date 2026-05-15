//! The spatial index produced by the clipper and consumed by queries.

use std::io::Write;

use common::CountingWriter;

use super::clipped_shape::{ClippedShape, GeometryId};
use crate::spatial::s2cell_id::S2CellId;

/// Stores the index contents for a particular S2CellId.
///
/// It consists of a set of clipped shapes.
#[derive(Clone, Debug, PartialEq)]
pub struct ShapeCell {
    /// Cell id.
    pub cell_id: S2CellId,
    /// Shapes stored in the cell.
    pub shapes: Vec<ClippedShape>,
}

impl ShapeCell {
    /// Creates a new empty ShapeCell for the given cell ID.
    pub fn new(cell_id: S2CellId) -> Self {
        Self {
            cell_id,
            shapes: Vec::new(),
        }
    }

    /// Adds a clipped shape to this cell.
    pub fn add_shape(&mut self, shape: ClippedShape) {
        self.shapes.push(shape);
    }

    /// Returns the clipped shape corresponding to the given geometry ID, or None if the geometry
    /// does not intersect this cell.
    pub fn find_shape(&self, geometry_id: GeometryId) -> Option<&ClippedShape> {
        self.shapes.iter().find(|s| s.geometry_id == geometry_id)
    }

    /// Returns the number of clipped shapes in this cell.
    #[inline]
    pub fn num_shapes(&self) -> usize {
        self.shapes.len()
    }

    /// Returns true if this cell contains no clipped shapes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }
}

/// A spatial index mapping S2CellIds to the shapes that intersect each cell.
#[derive(Clone, Debug, Default)]
pub struct ShapeIndex {
    /// Cells in the index.
    pub cells: Vec<ShapeCell>,
}

impl ShapeIndex {
    /// Creates a new empty ShapeIndex.
    pub fn new() -> Self {
        Self { cells: Vec::new() }
    }

    /// Returns true if the index contains no cells.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Returns the number of cells in the index.
    #[inline]
    pub fn num_cells(&self) -> usize {
        self.cells.len()
    }

    /// Returns the index cell containing the given cell ID, or None if not found.
    pub fn find_cell(&self, target: S2CellId) -> Option<&ShapeCell> {
        let mut lo = 0;
        let mut hi = self.cells.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = &self.cells[mid];

            if target < cell.cell_id.range_min() {
                hi = mid;
            } else if target > cell.cell_id.range_max() {
                lo = mid + 1;
            } else {
                return Some(cell);
            }
        }

        None
    }

    /// Writes the cell index to a CountingWriter with a dictionary at the end.
    /// Offsets are recorded relative to the start of this write, not the global
    /// stream position, because CompositeFile shares one CountingWriter across
    /// all fields but the reader sees a per-field slice starting at zero.
    ///
    /// If doc_id_map is provided, each geometry_id is resolved to a doc_id and
    /// the doc_id file position is recorded in the directory alongside the cell
    /// offset. The doc_ids are written to a separate writer in cell order.
    pub fn write<W: Write>(&self, write: &mut CountingWriter<W>) {
        self.write_with_doc_ids::<W>(write, None, None);
    }

    pub fn write_with_doc_ids<W: Write>(
        &self,
        write: &mut CountingWriter<W>,
        mut doc_ids_write: Option<&mut CountingWriter<W>>,
        doc_id_map: Option<&[u32]>,
    ) {
        let base = write.written_bytes();
        let doc_ids_base = doc_ids_write
            .as_ref()
            .map(|w| w.written_bytes())
            .unwrap_or(0);
        let mut offsets: Vec<(u64, u64, u64)> = Vec::with_capacity(self.cells.len());

        for cell in &self.cells {
            let offset = write.written_bytes() - base;
            let doc_id_offset = doc_ids_write
                .as_ref()
                .map(|w| w.written_bytes() - doc_ids_base)
                .unwrap_or(0);

            write.write_all(&cell.cell_id.0.to_le_bytes()).unwrap();
            write
                .write_all(&(cell.shapes.len() as u32).to_le_bytes())
                .unwrap();

            for shape in &cell.shapes {
                write.write_all(&shape.geometry_id.1.to_le_bytes()).unwrap();
                write.write_all(&[shape.contains_center as u8]).unwrap();
                write
                    .write_all(&(shape.edge_indices.len() as u32).to_le_bytes())
                    .unwrap();
                for &edge_id in &shape.edge_indices {
                    write.write_all(&edge_id.to_le_bytes()).unwrap();
                }

                if let (Some(ref mut dw), Some(map)) = (&mut doc_ids_write, doc_id_map) {
                    let doc_id = map[shape.geometry_id.1 as usize];
                    dw.write_all(&doc_id.to_le_bytes()).unwrap();
                }
            }

            offsets.push((cell.cell_id.0, offset, doc_id_offset));
        }

        // Directory: (cell_id, offset, doc_id_offset) triples.
        let dir_offset = write.written_bytes() - base;
        for &(cell_id, offset, doc_id_offset) in &offsets {
            write.write_all(&cell_id.to_le_bytes()).unwrap();
            write.write_all(&offset.to_le_bytes()).unwrap();
            write.write_all(&doc_id_offset.to_le_bytes()).unwrap();
        }

        // Footer: cell count, directory offset.
        write
            .write_all(&(self.cells.len() as u32).to_le_bytes())
            .unwrap();
        write.write_all(&dir_offset.to_le_bytes()).unwrap();
    }
}
