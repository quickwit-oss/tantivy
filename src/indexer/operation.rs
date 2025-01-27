use crate::index::SegmentId;
use crate::query::Weight;
use crate::schema::document::Document;
use crate::schema::{TantivyDocument, Term};
use crate::{DocId, Opstamp};

/// Timestamped Delete operation.
pub enum DeleteOperation {
    ByWeight {
        /// Operation stamp.
        /// It is used to check whether the delete operation
        /// applies to an added document operation.
        opstamp: Opstamp,
        /// Weight is used to define the set of documents to be deleted.
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
    /// Operation stamp.
    pub opstamp: Opstamp,
    /// Document to be added.
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
