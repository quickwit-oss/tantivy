use schema::Term;
use std::sync::{Arc, RwLock};
use super::operation::DeleteOperation;

const BLOCK_SIZE: usize = 128;


/// DeleteQueue are implemented as an unrolled linked list.
/// Block implements a block of this unrolled linked list.
struct Block {
    operations: Vec<DeleteOperation>,
    next: Option<SharedBlock>,
}

impl Default for Block {
    fn default() -> Block {
        Block {
            operations: Vec::with_capacity(BLOCK_SIZE),
            next: None
        }
    }
}

/// A shared block wraps a block
#[derive(Clone)]
struct SharedBlock(Arc<RwLock<Block>>);

impl SharedBlock {
    // Happens a new element to the block and return 
    // what the new head is.
    fn enqueue(&self, delete_operation: DeleteOperation) -> Option<SharedBlock> {
        let mut writable_block = self.0.write().expect("Panicked while enqueueing in the delete queue.");
        if writable_block.operations.len() >= BLOCK_SIZE {
            let next_block = SharedBlock::default();
            next_block.enqueue(delete_operation);
            writable_block.next = Some(next_block.clone());
            Some(next_block)
        }
        else {
            writable_block.operations.push(delete_operation);
            None
        }
    }

    fn next_block(&self) -> Option<SharedBlock> {
        self.0
            .read()
            .unwrap()
            .next
            .clone()
    }
    
    fn cursor(&self,) -> DeleteQueueCursor {
        let len = self.0
            .read()
            .expect("Panicked while reading a block in the delete queue.")
            .operations
            .len();
        DeleteQueueCursor {
            block: self.clone(),
            pos: len,
        }
    }
}

impl Default for SharedBlock {
    fn default() -> SharedBlock {
        SharedBlock(Arc::default())
    }
}

impl Default for DeleteQueue {
    fn default() -> DeleteQueue {
        DeleteQueue {
            writing_head: SharedBlock::default(),
        }
    }
}

#[derive(Clone)]
pub struct DeleteQueueCursor {
    block: SharedBlock,
    pos: usize,
}

impl DeleteQueueCursor {
    
    pub fn peek(&mut self) -> Option<DeleteOperation> {
        if self.pos >= BLOCK_SIZE {
            self.pos = 0;
            match self.block.next_block() {
                Some(next_block) => {
                    self.block = next_block;
                    self.pos = 0;
                }
                None => {
                    // there is no next block.
                    return None;
                }
            }
        }
        let readable_block = self.block.0
            .read()
            .unwrap();
        if self.pos >= readable_block.operations.len() {
            None
        }
        else {
            Some(readable_block.operations[self.pos].clone())
        }
    }
    
    /// Returns a delete operation if an operation is available,
    /// None if the queue is empty.
    ///
    /// (We are voluntarily not using the `Iterator` trait
    /// as a call to `consume` may return None once, and return
    /// `Some(...)` ulteriorily. While this is officially
    /// compatible with the `Iterator` specification, we judge
    /// this confusing.)
    pub fn consume(&mut self) -> Option<DeleteOperation> {
        let delete_position = self.peek();
        if delete_position.is_some() {
            self.pos += 1;
        }
        delete_position
    }
}

// ----------------------------------------

pub struct DeleteQueue {
    writing_head: SharedBlock,  
}

impl DeleteQueue {
    
    pub fn cursor(&self) -> DeleteQueueCursor {
        self.writing_head.cursor()
    }
    
    pub fn push_op(&mut self, delete_operation: DeleteOperation) {
        if let Some(new_head) = self.writing_head.enqueue(delete_operation) {
            self.writing_head = new_head;
        }
    }
    pub fn push(&mut self, opstamp: u64, term: Term) {
        let delete_operation = DeleteOperation {
            opstamp: opstamp,
            term: term,
        };
        self.push_op(delete_operation);
    }
}



#[cfg(test)]
mod tests {

    use super::{DeleteQueue, DeleteOperation};
    use schema::{Term, Field};

    #[test]
    fn test_deletequeue() {
        let mut delete_queue = DeleteQueue::default();
        
        let make_op = |i: usize| {
            let field = Field(1u8);
            DeleteOperation {
                opstamp: i as u64,
                term: Term::from_field_u32(field, i as u32)
            }
        };

        delete_queue.push_op(make_op(1));
        delete_queue.push_op(make_op(2));
        
        let mut delete_cursor_3 = delete_queue.cursor();
        let mut delete_cursor_3_b = delete_cursor_3.clone();
        
        assert!(delete_cursor_3.consume().is_none());
        assert!(delete_cursor_3.peek().is_none());
        
        delete_queue.push_op(make_op(3));
        delete_queue.push_op(make_op(4));
                
        assert_eq!(delete_cursor_3_b.peek(), Some(make_op(3)));
        let mut delete_cursor_3_c = delete_cursor_3_b.clone();
        
        assert_eq!(delete_cursor_3_b.consume(), Some(make_op(3)));
        let mut delete_cursor_4 = delete_cursor_3_b.clone();
        
        assert_eq!(delete_cursor_3_b.peek(), Some(make_op(4)));
        assert_eq!(delete_cursor_3_b.consume(), Some(make_op(4)));
        
        assert_eq!(delete_cursor_3_c.consume(), Some(make_op(3)));
        
        assert!(delete_cursor_3_b.consume().is_none());
        assert_eq!(delete_cursor_3_c.consume(), Some(make_op(4)));
        assert!(delete_cursor_3_c.consume().is_none());
        
        assert_eq!(delete_cursor_3.peek(), Some(make_op(3)));
        assert_eq!(delete_cursor_3.consume(), Some(make_op(3)));
        assert!(delete_cursor_3_b.consume().is_none());
        
        assert_eq!(delete_cursor_4.consume(), Some(make_op(4)));
        assert!(delete_cursor_4.consume().is_none());
        
        
    }
}