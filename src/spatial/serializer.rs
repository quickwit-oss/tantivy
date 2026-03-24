//! Spatial field serialization.
//!
//! Writes cell index and edge index data for spatial fields. Each field's cell index and edge
//! index are written as sub-sections of the composite files for the segment's SpatialCells and
//! SpatialEdges components.
use std::io;
use std::io::Write;

use crate::directory::{CompositeWrite, WritePtr};
use crate::schema::Field;
use crate::spatial::cell_index::{BuildOptions, IndexBuilder};
use crate::spatial::edge_writer::EdgeWriter;
use crate::spatial::geometry_set::GeometrySet;

/// Default skip interval for the edge index skip list directory.
const EDGE_SKIP_INTERVAL: u32 = 16;

/// Serializes spatial field data into cell index and edge index files.
pub struct SpatialSerializer {
    cells_write: CompositeWrite,
    edges_write: CompositeWrite,
}

impl SpatialSerializer {
    /// Create the serializer from two write pointers (cells and edges).
    pub fn new(cells_write: WritePtr, edges_write: WritePtr) -> io::Result<SpatialSerializer> {
        Ok(SpatialSerializer {
            cells_write: CompositeWrite::wrap(cells_write),
            edges_write: CompositeWrite::wrap(edges_write),
        })
    }

    /// Serialize one field's spatial data. Builds a CellIndex from the smashed GeometrySets
    /// and streams edges through EdgeWriter.
    pub fn serialize_field(&mut self, field: Field, sets: &[GeometrySet]) -> io::Result<()> {
        if sets.is_empty() {
            return Ok(());
        }

        // Build the cell index from smashed sets.
        let builder = IndexBuilder::new(BuildOptions::default());
        let cell_index = builder.build_from_sets(sets);

        // Write the cell index.
        let cells_out = self.cells_write.for_field(field);
        cell_index.write(cells_out);
        cells_out.flush()?;

        // Write the edge index.
        let edges_out = self.edges_write.for_field(field);
        {
            let mut edge_writer = EdgeWriter::new(edges_out, EDGE_SKIP_INTERVAL);

            for set in sets {
                edge_writer.insert(set);
            }

            edge_writer.finish();
        }
        edges_out.flush()?;

        Ok(())
    }

    /// Split into the two underlying CompositeWrites for direct access. Used by the merger.
    pub fn into_composite_writes(self) -> (CompositeWrite, CompositeWrite) {
        (self.cells_write, self.edges_write)
    }

    /// Close both composite files.
    pub fn close(self) -> io::Result<()> {
        self.cells_write.close()?;
        self.edges_write.close()?;
        Ok(())
    }
}
