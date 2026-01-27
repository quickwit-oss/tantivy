//! HUSH
use std::io;
use std::sync::Arc;

use common::file_slice::FileSlice;
use common::OwnedBytes;

use crate::directory::CompositeFile;
use crate::schema::Field;
use crate::space_usage::PerFieldSpaceUsage;

/// Per-field spatial index readers for a segment.
///
/// Provides access to the spatial index data stored in a segment's composite file. Each field with
/// spatial indexing enabled has its own SpatialReader.
#[derive(Clone)]
pub struct SpatialReaders {
    data: Arc<CompositeFile>,
}

impl SpatialReaders {
    /// Returns an empty SpatialReaders for segments with no spatial fields.
    pub fn empty() -> SpatialReaders {
        SpatialReaders {
            data: Arc::new(CompositeFile::empty()),
        }
    }

    /// Creates a field norm reader.
    pub fn open(file: FileSlice) -> crate::Result<SpatialReaders> {
        let data = CompositeFile::open(&file)?;
        Ok(SpatialReaders {
            data: Arc::new(data),
        })
    }

    /// Returns the FieldNormReader for a specific field.
    pub fn get_field(&self, field: Field) -> crate::Result<Option<SpatialReader>> {
        if let Some(file) = self.data.open_read(field) {
            let spatial_reader = SpatialReader::open(file)?;
            Ok(Some(spatial_reader))
        } else {
            Ok(None)
        }
    }

    /// Return a break down of the space usage per field.
    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        self.data.space_usage()
    }

    /// Returns a handle to inner file
    pub fn get_inner_file(&self) -> Arc<CompositeFile> {
        self.data.clone()
    }
}

/// HUSH
#[derive(Clone)]
pub struct SpatialReader {
    data: OwnedBytes,
}

impl SpatialReader {
    /// Opens the spatial reader from a `FileSlice`. Returns `None` if the file is empty (no
    /// spatial fields indexed.)
    pub fn open(spatial_file: FileSlice) -> io::Result<SpatialReader> {
        let data = spatial_file.read_bytes()?;
        Ok(SpatialReader { data })
    }
    /// HUSH
    pub fn get_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}
