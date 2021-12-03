use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use nuclia_vectors::{
    entry::entry_point::SegmentEntry,
    segment_constructor::{build_segment, load_segment},
    types::{Distance, Indexes, SegmentConfig, HnswConfig, StorageType},
};

use crate::{DocId, TantivyError, schema::{Field, Schema}};

/// VectorWriter for segment.
/// Internally contains a Writer for each field. Lazy Initialization, a writer is created when a
/// search access its field.
pub struct VectorWriters {
    segment_path: PathBuf,
    writers: HashMap<Field, VectorWriter>,
    segment_config: SegmentConfig,
}

impl VectorWriters {
    pub fn new(path: &PathBuf) -> VectorWriters {
        trace!("Create VectorWriter for segment");

        //let hsnw_config = HnswConfig::default();

        let config = SegmentConfig {
            vector_size: 3,
            distance: Distance::Dot,
            index: Indexes::Plain{},
            payload_index: None,
            storage_type: StorageType::Drive,
        };

        VectorWriters {
            segment_path: path.clone(),
            segment_config: config,
            writers: HashMap::new(),
        }
    }

    pub fn record(&mut self, doc_id: DocId, field: Field, vector: &Vec<f32>) -> crate::Result<bool> {
        trace!("record {} - {:?} - {:?}", doc_id, field, vector);
        match self.writers.get_mut(&field) {
            Some(writer) => {
                writer.record(doc_id, vector)
            }
            None => {
                let mut writer = VectorWriter::new(&self.segment_path, field, &self.segment_config);
                let result = writer.record(doc_id, vector);
                self.writers.insert(field, writer);

                result
            },
        }
        
        
    }
}

pub struct VectorWriter {
    segment: nuclia_vectors::segment::Segment,
}

impl VectorWriter {
    fn new(segment_path: &PathBuf, field: Field, config: &SegmentConfig) -> VectorWriter {
        
        let field_path = field.field_id().to_string();
        let path = segment_path.join(field_path);

        trace!("New VectorWriter for field: {:?} at {}", field, path.to_str().unwrap());

        let segment = match load_segment(path.as_path(), false) {
            Ok(segment) => segment,
            Err(e) => match build_segment(path.as_path(), &config, false) {
                Ok(segment) => segment,
                Err(e) => {
                    panic!("Error loading VectorWriter: {}", e);
                }
            },
        };

        VectorWriter { segment }
    }

    fn record(&mut self, doc_id: DocId, vector: &Vec<f32>) -> crate::Result<bool> {
        trace!("record => {} - {:?} - {:?}", doc_id, vector, self.segment.current_path);
        match self.segment.upsert_point(1, doc_id as u64, vector) {
            Ok(b) => Ok(b),
            Err(e) => Err(TantivyError::InvalidArgument(e.to_string())),
        }
    }
}
