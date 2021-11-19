use std::path::Path;

use crate::schema::{Field, Schema};


pub struct VectorWriter {

}

impl VectorWriter {

    pub fn from_schema(schema: &Schema) -> VectorWriter {
        trace!("Create VectorWriter for schema: {:?}", schema);

        VectorWriter{}
    }

    pub fn record(&self, doc_id: u32, field: Field, vector: &Vec<f32>) {
        trace!("record {} - {:?} - {:?}", doc_id, field, vector);
    }
}