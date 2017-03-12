use super::operation::DeleteOperation;
use std::sync::{Arc, RwLock};

/// This implementation assumes that we
/// have a lot more write operation than read operations.


type InnerDeleteQueue = Arc<RwLock<Vec<DeleteOperation>>>;

// TODO very inefficient.
// fix this once the refactoring/bugfix is done
#[derive(Clone)]
pub struct DeleteCursor {
    cursor: usize,
    operations: InnerDeleteQueue,
}

impl DeleteCursor {

    pub fn skip_to(&mut self, target_opstamp: u64) {
        while let Some(operation) = self.peek() {
            if operation.opstamp >= target_opstamp {
                break;
            }
            self.advance()
        }
    }

    pub fn advance(&mut self) {
        let read = self.operations.read().unwrap();
        if self.cursor < read.len()  {
            self.cursor += 1;
        }
    }

    pub fn peek(&self,) -> Option<DeleteOperation> {
        let read = self.operations.read().unwrap();
        if self.cursor >= read.len() {
            None
        }
        else {
            let operation = read[self.cursor].clone();
            Some(operation)
        }
    }
}

// TODO remove copy
impl Iterator for DeleteCursor {
    
    type Item=DeleteOperation;
    
    fn next(&mut self) -> Option<DeleteOperation >{
        let read = self.operations.read().unwrap();
        if self.cursor >= read.len() {
            None
        }
        else {
            let operation = read[self.cursor].clone();
            self.cursor += 1;
            Some(operation)
        }
    }
}


#[derive(Clone, Default)]
pub struct DeleteQueue(InnerDeleteQueue);

impl DeleteQueue {

    pub fn new() -> DeleteQueue {
        DeleteQueue::default()
    }

    pub fn push(&self, delete_operation: DeleteOperation) {
        self.0.write().unwrap().push(delete_operation);
    }

    pub fn cursor(&self) -> DeleteCursor {
        DeleteCursor {
            cursor: 0,
            operations: self.0.clone(),
        }
    }
}


#[cfg(test)]
mod tests {

    use super::{DeleteQueue, DeleteOperation};
    use schema::{Term, Field};

    #[test]
    fn test_deletequeue() {
        let delete_queue = DeleteQueue::new();
        
        let make_op = |i: usize| {
            let field = Field(1u8);
            DeleteOperation {
                opstamp: i as u64,
                term: Term::from_field_u32(field, i as u32)
            }
        };

        delete_queue.push(make_op(1));
        delete_queue.push(make_op(2));

        let snapshot = delete_queue.cursor();
        {
            let mut operations_it = snapshot.clone();
            assert_eq!(operations_it.next().unwrap().opstamp, 1);
            assert_eq!(operations_it.next().unwrap().opstamp, 2);
            assert!(operations_it.next().is_none());
        }
        {   
            let mut operations_it = snapshot.clone();
            assert_eq!(operations_it.next().unwrap().opstamp, 1);
            assert_eq!(operations_it.next().unwrap().opstamp, 2);
            assert!(operations_it.next().is_none());
        }
        
        // // operations does not own a lock on the queue.
        // delete_queue.push(make_op(3));
        // let snapshot2 = delete_queue.snapshot();
        // {
        //     // operations is not affected by
        //     // the push that occurs after.
        //     let mut operations_it = snapshot.iter();
        //     let mut operations2_it = snapshot2.iter();
        //     assert_eq!(operations_it.next().unwrap().opstamp, 1);
        //     assert_eq!(operations2_it.next().unwrap().opstamp, 1);
        //     assert_eq!(operations_it.next().unwrap().opstamp, 2);
        //     assert_eq!(operations2_it.next().unwrap().opstamp, 2);
        //     assert!(operations_it.next().is_none());
        //     assert_eq!(operations2_it.next().unwrap().opstamp, 3);
        //     assert!(operations2_it.next().is_none());
        // }
    }
}