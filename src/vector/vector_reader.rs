use std::{collections::HashMap, path::PathBuf, sync::{Arc, RwLock}};

use crate::{schema::Field, space_usage::PerFieldSpaceUsage, DocId};

use nuclia_vectors::{
    entry::entry_point::{SegmentEntry, OperationResult},
    segment_constructor::{build_segment, load_segment},
    types::WithPayload,
};
use tempfile::TempDir;

/// Manager of the VectorReader of all the segments. For the moment only create VectorReader's it
/// doesn't store any reference to them.
pub struct VectorReaders {
    segment_path: PathBuf,
    reader_map: HashMap<Field, VectorReader>
}

impl VectorReaders {
    /// Creates a new VectorReaders container in the segment path.
    pub fn new(path: &PathBuf) -> VectorReaders {
        trace!("New VectorReaders created! {:?}.", path);
        VectorReaders { 
            segment_path: path.clone(),
            reader_map: HashMap::new()
        }
    }

    /// Creates a VectorReader initialized for this field. It opens a VectorReader in the path of
    /// the segment and field.
    pub fn open_read(&mut self, field: Field) -> &VectorReader {
        let path = field.field_id().to_string();
        
        let vector_reader = self.reader_map
            .entry(field)
            .or_insert(VectorReader::new(self.segment_path.join(path)));

        vector_reader
        
    }

    /// Computes the storage needed to index this field.
    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        todo!();
    }
}

type ScoreType = f32;

/// VectorReader for a segment and field.
pub struct VectorReader {
    segment: Arc<RwLock<nuclia_vectors::segment::Segment>>,
}

unsafe impl Send for VectorReader {}
unsafe impl Sync for VectorReader {}

impl VectorReader {
    /// Creates a VectorReader on this path. Usually this method is call from the VectorReaders
    /// container of the segment reader.
    pub fn new(path: PathBuf) -> VectorReader {
        trace!("New vector reader created! {:?}.", path);

        let segment = load_segment(path.as_path(), true).unwrap();

        VectorReader {segment: Arc::new(RwLock::new(segment)) }
    }

    /// Search documents with similarity to this vector.
    pub fn search(&self, vector: &Vec<f32>, limit: usize) -> Vec<(DocId, ScoreType)> {
        let res = self
            .segment
            .read()
            .unwrap()
            .search(&vector, &WithPayload::default(), limit, None)
            .unwrap();

        res.iter().map(|x | {
            (x.id as DocId, x.score as ScoreType)
        }).collect()
    }

    pub fn vector(&self, doc_id: DocId) -> OperationResult<Vec<f32>> {
        self.segment.read().unwrap().vector(doc_id as u64)
    }
}
