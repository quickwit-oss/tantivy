use std::path::PathBuf;

use crate::{schema::Field, space_usage::PerFieldSpaceUsage, DocId};

use qdrant_segment::{
    entry::entry_point::SegmentEntry,
    segment_constructor::{build_segment, load_segment},
    types::WithPayload,
};

/// Manager of the VectorReader of all the segments. For the moment only create VectorReader's it
/// doesn't store any reference to them.
pub struct VectorReaders {
    segment_path: PathBuf,
}

impl VectorReaders {
    /// Creates a new VectorReaders container in the segment path.
    pub fn new(path: PathBuf) -> VectorReaders {
        trace!("New VectorReaders created! {:?}.", path);
        VectorReaders { segment_path: path }
    }

    /// Creates a VectorReader initialized for this field. It opens a VectorReader in the path of
    /// the segment and field.
    pub fn open_read(&self, field: Field) -> VectorReader {
        let path = field.field_id().to_string();
        VectorReader::new(self.segment_path.join(path))
    }

    /// Computes the storage needed to index this field.
    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        todo!();
    }
}

type ScoreType = f32;

/// VectorReader for a segment and field.
pub struct VectorReader {
    segment: qdrant_segment::segment::Segment,
}

impl VectorReader {
    /// Creates a VectorReader on this path. Usually this method is call from the VectorReaders
    /// container of the segment reader.
    pub fn new(path: PathBuf) -> VectorReader {
        trace!("New vector reader created! {:?}.", path);

        let segment = load_segment(path.as_path()).unwrap();

        VectorReader { segment }
    }

    /// Search documents with similarity to this vector.
    pub fn search(&self, vector: &Vec<f32>, limit: usize) -> Vec<(DocId, ScoreType)> {
        let res = self
            .segment
            .search(&vector, &WithPayload::default(), None, limit, None)
            .unwrap();

        res.iter().map(|x | {
            (x.id as DocId, x.score as ScoreType)
        }).collect()
    }
}
