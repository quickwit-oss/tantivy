use crate::schema::Document;
use crate::schema::Term;
use crate::Opstamp;

/// Timestamped Delete operation.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct DeleteOperation {
    pub opstamp: Opstamp,
    pub term: Term,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation {
    pub opstamp: Opstamp,
    pub document: Document,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation {
    /// Add operation
    Add(Document),
    /// Delete operation
    Delete(Term),
}
