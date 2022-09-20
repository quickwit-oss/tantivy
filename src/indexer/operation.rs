use crate::query::Weight;
use crate::schema::{Document, Term};
use crate::Opstamp;

/// Timestamped Delete operation.
pub struct DeleteOperation {
    pub opstamp: Opstamp,
    pub target: DeleteTarget,
}

/// Target of a Delete operation
pub enum DeleteTarget {
    Term(Term),
    Query(Box<dyn Weight>),
}

impl Default for DeleteOperation {
    fn default() -> Self {
        DeleteOperation {
            opstamp: 0,
            target: DeleteTarget::Term(Term::new().into()),
        }
    }
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
