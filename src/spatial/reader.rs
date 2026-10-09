//! Spatial segment readers.
//!
//! Provides per-field access to cell index and edge index data stored in a segment's
//! SpatialCells and SpatialEdges composite files.
use std::io;
use std::sync::Arc;

use common::file_slice::FileSlice;
use common::OwnedBytes;

use crate::directory::CompositeFile;
use crate::error::DataCorruption;
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

    /// Returns the per-field spatial reader, or None if the segment has no geometry for the
    /// field. A field must have both a cell index and an edge index, or neither.
    pub fn get_field(&self, field: Field) -> crate::Result<Option<SpatialReader>> {
        let cells_file = self.cells.open_read(field);
        let edges_file = self.edges.open_read(field);
        match (cells_file, edges_file) {
            (Some(c), Some(e)) => {
                let reader = SpatialReader::open(c, e)?;
                Ok(Some(reader))
            }
            (None, None) => Ok(None),
            (Some(_), None) | (None, Some(_)) => Err(DataCorruption::comment_only(format!(
                "Spatial field {field:?} has only one of a cell index and an edge index"
            ))
            .into()),
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;

    use super::*;
    use crate::directory::{CompositeWrite, Directory, RamDirectory};

    /// A composite file with the given fields, each holding one byte.
    fn composite(
        directory: &RamDirectory,
        name: &str,
        fields: &[Field],
    ) -> crate::Result<FileSlice> {
        let path = Path::new(name);
        let mut write = CompositeWrite::wrap(directory.open_write(path)?);
        for &field in fields {
            write.for_field(field).write_all(&[0])?;
        }
        write.close()?;
        Ok(directory.open_read(path)?)
    }

    #[test]
    fn test_get_field_rejects_a_field_missing_from_one_index() -> crate::Result<()> {
        let directory = RamDirectory::create();
        let (both, cells_only, edges_only, neither) = (
            Field::from_field_id(0),
            Field::from_field_id(1),
            Field::from_field_id(2),
            Field::from_field_id(3),
        );
        let readers = SpatialReaders::open(
            composite(&directory, "cells", &[both, cells_only])?,
            composite(&directory, "edges", &[both, edges_only])?,
        )?;
        assert!(readers.get_field(both)?.is_some());
        assert!(readers.get_field(neither)?.is_none());
        assert!(matches!(
            readers.get_field(cells_only),
            Err(crate::TantivyError::DataCorruption(_))
        ));
        assert!(matches!(
            readers.get_field(edges_only),
            Err(crate::TantivyError::DataCorruption(_))
        ));
        Ok(())
    }
}
