//! Spatial field serialization.
//!
//! Writes cell index and edge index data for spatial fields. Each field's cell index and edge
//! index are written as sub-sections of the composite files for the segment's SpatialCells and
//! SpatialEdges components.
use std::io;
use std::io::Write;

use crate::directory::{CompositeWrite, WritePtr};
use crate::schema::Field;
use crate::spatial::cell_index::{BuildOptions, GeometryData, IndexBuilder};
use crate::spatial::edge_writer::EdgeWriter;

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

    /// Serialize one field's spatial data. Builds a CellIndex from the accumulated documents
    /// and streams edges through EdgeWriter.
    pub fn serialize_field(
        &mut self,
        field: Field,
        documents: &[(u32, Vec<GeometryData>)],
    ) -> io::Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        // Build the cell index.
        let mut builder = IndexBuilder::new(BuildOptions::default());
        for (doc_id, geometries) in documents {
            builder.add(*doc_id, geometries.clone());
        }
        let cell_index = builder.build();

        // Write the cell index.
        let cells_out = self.cells_write.for_field(field);
        cell_index.write(cells_out);
        cells_out.flush()?;

        // Write the edge index. Iterate documents in the same order so geometry IDs align.
        let edges_out = self.edges_write.for_field(field);
        {
            let mut edge_writer = EdgeWriter::new(edges_out, EDGE_SKIP_INTERVAL);

            let mut geometry_id: u32 = 0;
            for (doc_id, geometries) in documents {
                let flattened: Vec<(Vec<[f64; 3]>, bool)> = geometries
                    .iter()
                    .map(|geo| {
                        let mut flat = Vec::new();
                        for ring in &geo.rings {
                            flat.extend_from_slice(ring);
                            // Duplicate the first vertex at the end of polygon rings so
                            // edge lookup is always position and position + 1.
                            if geo.dimension == 2 && !ring.is_empty() {
                                flat.push(ring[0]);
                            }
                        }
                        (flat, geo.dimension == 2)
                    })
                    .collect();

                let refs: Vec<(u32, &[[f64; 3]], bool)> = flattened
                    .iter()
                    .enumerate()
                    .map(|(i, (flat, closed))| (geometry_id + i as u32, flat.as_slice(), *closed))
                    .collect();

                edge_writer.insert(*doc_id, &refs);
                geometry_id += geometries.len() as u32;
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
