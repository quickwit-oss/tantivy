use schema::Document;
use schema::Term;
use Opstamp;

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
    Add(Document),
    Delete(Term),
}
