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


pub enum AddOperations {
    Single(AddOperation),
    Multiple(Vec<AddOperation>),
}

impl AddOperations {
    pub fn first_opstamp(&self) -> u64 {
        match *self {
            AddOperations::Single(ref op) => op.opstamp,
            AddOperations::Multiple(ref ops) => ops[0].opstamp,
        }
    }
}

impl From<AddOperation> for AddOperations {
    fn from(op: AddOperation) -> AddOperations {
        AddOperations::Single(op)
    }
}

impl From<Vec<AddOperation>> for AddOperations {
    fn from(ops: Vec<AddOperation>) -> AddOperations {
        AddOperations::Multiple(ops)
    }
}

impl IntoIterator for AddOperations {
    type Item = AddOperation;

    type IntoIter = Box<Iterator<Item = AddOperation>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            AddOperations::Single(op) => Box::new(Some(op).into_iter()),
            AddOperations::Multiple(ops) => Box::new(ops.into_iter()),
        }
    }
}
