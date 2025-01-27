use crate::index::SegmentId;
use crate::query::Weight;
use crate::schema::document::Document;
use crate::schema::{TantivyDocument, Term};
use crate::{DocId, Opstamp};

pub enum DeleteOperation {
    ByWeight {
        opstamp: Opstamp,
        target: Box<dyn Weight>,
    },
    ByAddress {
        opstamp: Opstamp,
        segment_id: SegmentId,
        doc_id: DocId,
    },
}

impl DeleteOperation {
    pub fn opstamp(&self) -> Opstamp {
        match self {
            DeleteOperation::ByWeight { opstamp, .. } => *opstamp,
            DeleteOperation::ByAddress { opstamp, .. } => *opstamp,
        }
    }
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation<D: Document = TantivyDocument> {
    pub opstamp: Opstamp,
    pub document: D,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation<D: Document = TantivyDocument> {
    /// Add operation
    Add(D),
    /// Delete operation
    Delete(Term),

    /// Delete a document by its address
    DeleteByAddress(SegmentId, DocId),
}
