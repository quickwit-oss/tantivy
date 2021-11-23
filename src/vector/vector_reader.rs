use std::path::PathBuf;

use crate::{DocId, schema::Field, space_usage::PerFieldSpaceUsage};


pub struct VectorReaders {
    segment_path: PathBuf
}

impl VectorReaders {
    pub fn new(path: PathBuf) -> VectorReaders {
        trace!("New VectorReaders created! {:?}.", path);
        VectorReaders{ segment_path: path }
    }

    pub fn open_read(&self, field: Field) -> VectorReader {
        let path = field.field_id().to_string();
        VectorReader::new(self.segment_path.join(path))

    }

    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        todo!();
    }
}



pub struct VectorReader {
    
}

impl VectorReader {
    pub fn new(path: PathBuf) -> VectorReader {
        trace!("New vector reader created! {:?}.", path);
        VectorReader{}
    }

    pub fn search(&self, vector: Vec<f32>, limit: usize) -> Vec<DocId>{
        todo!();
    }

}