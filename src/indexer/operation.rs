use itertools::Either;

use crate::query::Weight;
use crate::schema::{Document, Term};
use crate::Opstamp;

/// Timestamped Delete operation.
//#[derive(Clone, Debug)]
pub struct DeleteOperation {
    pub opstamp: Opstamp,
    pub term: Either<Term, Box<dyn Weight>>,
}

impl DeleteOperation {
    pub fn new(opstamp: Opstamp, term: Term) -> Self {
        DeleteOperation {
            opstamp,
            term: Either::Left(term),
        }
    }

    pub fn new_for_query(opstamp: Opstamp, weight: Box<dyn Weight>) -> Self {
        DeleteOperation {
            opstamp,
            term: Either::Right(weight),
        }
    }
}

impl Default for DeleteOperation {
    fn default() -> Self {
        DeleteOperation::new(0, Term::new())
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
