use super::operation::DeleteOperation;

// TODO remove clone
#[derive(Clone)]
pub struct DeleteQueue {
    delete_operations: Vec<DeleteOperation>,
}

impl DeleteQueue {

    pub fn new() -> DeleteQueue {
        DeleteQueue {
            delete_operations: vec!(),
        }
    }
    
    pub fn push_op(&mut self, delete_operation: DeleteOperation) {
        self.delete_operations.push(delete_operation);
    }

    pub fn operations(&self,) -> impl Iterator<Item=DeleteOperation> {
        // TODO fix iterator
        self.delete_operations.clone().into_iter()
    }
}



#[cfg(test)]
mod tests {

    use super::{DeleteQueue, DeleteOperation};
    use schema::{Term, Field};

    #[test]
    fn test_deletequeue() {
        let mut delete_queue = DeleteQueue::new();
        
        let make_op = |i: usize| {
            let field = Field(1u8);
            DeleteOperation {
                opstamp: i as u64,
                term: Term::from_field_u32(field, i as u32)
            }
        };

        delete_queue.push_op(make_op(1));
        delete_queue.push_op(make_op(2));
        
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