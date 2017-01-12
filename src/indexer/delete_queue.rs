use schema::Term;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};


const BLOCK_SIZE: usize = 128;

struct DeleteOperation {
    pub opstamp: u64,
    pub term: Term,
}

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

#[derive(Clone)]
struct SharedBlock {
    inner: Arc<RwLock<Block>>,
}

impl SharedBlock {
    // Happens a new element to the block and return 
    // what the new head is.
    fn enqueue(&self, delete_operation: DeleteOperation) -> Option<SharedBlock> {
        let mut writable_block = self.inner.write().expect("Panicked while enqueueing in the delete queue.");
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
    
    fn cursor(&self,) -> DeleteQueueCursor {
        let len = self.inner
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
        SharedBlock {
            inner: Arc::default()
        }
    }
}

impl Default for DeleteQueue {
    fn default() -> DeleteQueue {
        DeleteQueue {
            head: SharedBlock::default(),
        }
    }
}

pub struct DeleteQueueCursor {
    block: SharedBlock,
    pos: usize,
}


// ----------------------------------------

pub struct DeleteQueue {
    head: SharedBlock,  
}

impl DeleteQueue {
    
    pub fn cursor(&self) -> DeleteQueueCursor {
        self.head.cursor()
    }
    
    pub fn push(&mut self, opstamp: u64, term: Term) {
        let delete_operation = DeleteOperation {
            opstamp: opstamp,
            term: term,
        };
        if let Some(new_head) = self.head.enqueue(delete_operation) {
            self.head = new_head;
        }
    }
}
