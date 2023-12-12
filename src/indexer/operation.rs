use crate::query::Weight;
use crate::schema::document::Document;
use crate::schema::{TantivyDocument, Term};
use crate::Opstamp;

/// Timestamped Delete operation.
pub struct DeleteOperation {
    pub opstamp: Opstamp,
    pub target: Box<dyn Weight>,
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
}
