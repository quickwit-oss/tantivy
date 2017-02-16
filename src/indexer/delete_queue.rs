use super::operation::DeleteOperation;
use std::sync::{Arc, RwLock};
use std::mem;

/// This implementation assumes that we
/// have a lot more write operation than read operations.

#[derive(Default)]
struct InnerDeleteQueue {
    ro_chunks: ReadOnlyDeletes,
    last_chunk: Vec<DeleteOperation>,
}

impl InnerDeleteQueue {
    pub fn push(&mut self, delete_operation: DeleteOperation) {
        self.last_chunk.push(delete_operation);
    }

    pub fn snapshot(&mut self,) -> ReadOnlyDeletes {
        if self.last_chunk.len() > 0 {
            let new_operations = vec!();
            let new_ro_chunk = mem::replace(&mut self.last_chunk, new_operations);
            self.ro_chunks.push(new_ro_chunk)
        }
        self.ro_chunks.clone()
    }

    pub fn clear(&mut self) {
        self.ro_chunks.clear();
        self.last_chunk.clear();
    }
}

#[derive(Default, Clone)]
pub struct ReadOnlyDeletes(Vec<Arc<Vec<DeleteOperation>>>);

impl ReadOnlyDeletes {
    fn push(&mut self, operations: Vec<DeleteOperation>) {
        self.0.push(Arc::new(operations));
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item=&'a DeleteOperation> {
        self.0
            .iter()
            .flat_map(|chunk| chunk.iter())
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

#[derive(Clone, Default)]
pub struct DeleteQueue(Arc<RwLock<InnerDeleteQueue>>);

impl DeleteQueue {
    pub fn push(&self, delete_operation: DeleteOperation) {
        self.0.write().unwrap().push(delete_operation);
    }

    pub fn snapshot(&self) -> ReadOnlyDeletes {
        self.0.write().unwrap().snapshot()
    }

    pub fn clear(&self) {
        self.0.write().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {

    use super::{DeleteQueue, DeleteOperation};
    use schema::{Term, Field};

    #[test]
    fn test_deletequeue() {
        let delete_queue = DeleteQueue::default();
        
        let make_op = |i: usize| {
            let field = Field(1u8);
            DeleteOperation {
                opstamp: i as u64,
                term: Term::from_field_u32(field, i as u32)
            }
        };

        delete_queue.push(make_op(1));
        delete_queue.push(make_op(2));

        let snapshot = delete_queue.snapshot();
        {
            let mut operations_it = snapshot.iter();
            assert_eq!(operations_it.next().unwrap().opstamp, 1);
            assert_eq!(operations_it.next().unwrap().opstamp, 2);
            assert!(operations_it.next().is_none());
        }
        {   // iterating does not consume results.
            let mut operations_it = snapshot.iter();
            assert_eq!(operations_it.next().unwrap().opstamp, 1);
            assert_eq!(operations_it.next().unwrap().opstamp, 2);
            assert!(operations_it.next().is_none());
        }
        // operations does not own a lock on the queue.
        delete_queue.push(make_op(3));
        let snapshot2 = delete_queue.snapshot();
        {
            // operations is not affected by
            // the push that occurs after.
            let mut operations_it = snapshot.iter();
            let mut operations2_it = snapshot2.iter();
            assert_eq!(operations_it.next().unwrap().opstamp, 1);
            assert_eq!(operations2_it.next().unwrap().opstamp, 1);
            assert_eq!(operations_it.next().unwrap().opstamp, 2);
            assert_eq!(operations2_it.next().unwrap().opstamp, 2);
            assert!(operations_it.next().is_none());
            assert_eq!(operations2_it.next().unwrap().opstamp, 3);
            assert!(operations2_it.next().is_none());
        }
    }
}