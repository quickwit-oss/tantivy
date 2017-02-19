use std::sync::Arc;
use DocId;

#[derive(Clone)]
pub enum DocToOpstampMapping {
    WithMap(Arc<Vec<u64>>),
    None
}

impl From<Vec<u64>> for DocToOpstampMapping {
    fn from(opstamps: Vec<u64>) -> DocToOpstampMapping {
        DocToOpstampMapping::WithMap(Arc::new(opstamps))
    }
}

impl DocToOpstampMapping { 
    // TODO Unit test
    pub fn compute_doc_limit(&self, opstamp: u64) -> DocId {
        match *self {
            DocToOpstampMapping::WithMap(ref doc_opstamps) => {
                match doc_opstamps.binary_search(&opstamp) {
                    Ok(doc_id) => doc_id as DocId,
                    Err(doc_id) => doc_id as DocId,
                }
            }
            DocToOpstampMapping::None => DocId::max_value(),
        }
    }
}

