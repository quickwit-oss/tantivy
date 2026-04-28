//! Spatial segment readers.
//!
//! Provides per-field access to cell index and edge index data stored in a segment's
//! SpatialCells and SpatialEdges composite files.
use std::io;
use std::sync::Arc;

use common::file_slice::FileSlice;
use common::OwnedBytes;

use crate::directory::CompositeFile;
use crate::schema::{Field, Schema};
use crate::space_usage::PerFieldSpaceUsage;

/// Per-field spatial index readers for a segment.
#[derive(Clone)]
pub struct SpatialReaders {
    cells: Arc<CompositeFile>,
    edges: Arc<CompositeFile>,
}

impl SpatialReaders {
    /// Returns an empty SpatialReaders for segments with no spatial fields.
    pub fn empty() -> SpatialReaders {
        SpatialReaders {
            cells: Arc::new(CompositeFile::empty()),
            edges: Arc::new(CompositeFile::empty()),
        }
    }

    /// Opens readers from cell index and edge index file slices.
    pub fn open(cells_file: FileSlice, edges_file: FileSlice) -> crate::Result<SpatialReaders> {
        let cells = CompositeFile::open(&cells_file)?;
        let edges = CompositeFile::open(&edges_file)?;
        Ok(SpatialReaders {
            cells: Arc::new(cells),
            edges: Arc::new(edges),
        })
    }

    /// Returns the per-field spatial reader.
    pub fn get_field(&self, field: Field) -> crate::Result<Option<SpatialReader>> {
        let cells_file = self.cells.open_read(field);
        let edges_file = self.edges.open_read(field);
        match (cells_file, edges_file) {
            (Some(c), Some(e)) => {
                let reader = SpatialReader::open(c, e)?;
                Ok(Some(reader))
            }
            _ => Ok(None),
        }
    }

    /// Return a break down of the space usage per field.
    pub fn space_usage(&self, schema: &Schema) -> PerFieldSpaceUsage {
        self.cells.space_usage(schema)
    }
}

/// Per-field spatial reader holding cell index and edge index bytes.
#[derive(Clone)]
pub struct SpatialReader {
    cells_data: OwnedBytes,
    edges_data: OwnedBytes,
}

impl SpatialReader {
    /// Opens the spatial reader from cell and edge file slices.
    pub fn open(cells_file: FileSlice, edges_file: FileSlice) -> io::Result<SpatialReader> {
        let cells_data = cells_file.read_bytes()?;
        let edges_data = edges_file.read_bytes()?;
        Ok(SpatialReader {
            cells_data,
            edges_data,
        })
    }

    /// Returns the cell index bytes.
    pub fn cells_bytes(&self) -> &[u8] {
        self.cells_data.as_ref()
    }

    /// Returns the edge index bytes.
    pub fn edges_bytes(&self) -> &[u8] {
        self.edges_data.as_ref()
    }
}
