use schema::Document;
use schema::Term;

/// Timestamped Delete operation.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct DeleteOperation {
    pub opstamp: u64,
    pub term: Term,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation {
    pub opstamp: u64,
    pub document: Document,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation {
    Add(Document),
    Delete(Term),
}
