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

    pub fn operations(&mut self,) -> ReadOnlyDeletes {
        if self.last_chunk.len() > 0 {
            let new_operations = vec!();
            let new_ro_chunk = mem::replace(&mut self.last_chunk, new_operations);
            self.ro_chunks.push(new_ro_chunk)
        }
        self.ro_chunks.clone()
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
}

#[derive(Clone, Default)]
pub struct DeleteQueue(Arc<RwLock<InnerDeleteQueue>>);

impl DeleteQueue {
    pub fn push(&self, delete_operation: DeleteOperation) {
        self.0.write().unwrap().push(delete_operation);
    }

    pub fn operations(&self) -> ReadOnlyDeletes {
        self.0.write().unwrap().operations()
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
        
        // TODO unit tests

        // let mut delete_cursor_3 = delete_queue.cursor();
        // let mut delete_cursor_3_b = delete_cursor_3.clone();
        
        // assert!(delete_cursor_3.next().is_none());
        // assert!(delete_cursor_3.peek().is_none());
        
        // delete_queue.push_op(make_op(3));
        // delete_queue.push_op(make_op(4));
                
        // assert_eq!(delete_cursor_3_b.peek(), Some(make_op(3)));
        // let mut delete_cursor_3_c = delete_cursor_3_b.clone();
        
        // assert_eq!(delete_cursor_3_b.next(), Some(make_op(3)));
        // let mut delete_cursor_4 = delete_cursor_3_b.clone();
        
        // assert_eq!(delete_cursor_3_b.peek(), Some(make_op(4)));
        // assert_eq!(delete_cursor_3_b.next(), Some(make_op(4)));
        
        // assert_eq!(delete_cursor_3_c.next(), Some(make_op(3)));
        
        // assert!(delete_cursor_3_b.next().is_none());
        // assert_eq!(delete_cursor_3_c.next(), Some(make_op(4)));
        // assert!(delete_cursor_3_c.next().is_none());
        
        // assert_eq!(delete_cursor_3.peek(), Some(make_op(3)));
        // assert_eq!(delete_cursor_3.next(), Some(make_op(3)));
        // assert!(delete_cursor_3_b.next().is_none());
        
        // assert_eq!(delete_cursor_4.next(), Some(make_op(4)));
        // assert!(delete_cursor_4.next().is_none());
        
        
    }
}