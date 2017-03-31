use super::operation::DeleteOperation;
use std::sync::{Arc, RwLock};
use std::mem;
use std::ops::DerefMut;


#[derive(Clone, Default)]
struct DeleteQueue {
    writer: Arc<RwLock<Vec<DeleteOperation>>>,
    next_block: Option<NextBlock>,
}

impl DeleteQueue {

    pub fn new() -> Arc<DeleteQueue> {
        let mut delete_queue = Arc::new(DeleteQueue::default());
        delete_queue.next_block = Some(
            NextBlock::from(delete_queue)
        );
        delete_queue
    }

    pub fn cursor(&self) -> Cursor {
        
        Cursor {
            current_block: Arc<Block>,
            pos: 0,
        }
    }

    pub fn push(&self, delete_operation: DeleteOperation) {
        let mut write_lock = self.writer
            .write()
            .expect("Failed to acquire write lock on delete queue writer")
            .push(delete_operation);
    }

    fn flush(&self) -> Option<Vec<DeleteOperation>> {
        let mut write_lock = self
            .writer
            .write()
            .expect("Failed to acquire write lock on delete queue writer");
        if write_lock.is_empty() {
            return None;
        }
        Some(mem::replace(write_lock.deref_mut(), vec!()))
    }
}

enum InnerNextBlock {
    Writer(Arc<DeleteQueue>),
    Closed(Arc<Block>),
    Terminated,
}

struct NextBlock(RwLock<InnerNextBlock>);

impl From<Arc<DeleteQueue>> for NextBlock {
    fn from(writer_arc: Arc<DeleteQueue>) -> NextBlock {
        NextBlock(RwLock::new(InnerNextBlock::Writer(writer_arc)))
    }
}

impl NextBlock {   
    pub fn next_block(&self) -> Option<Arc<Block>> {
        {
            let next_read_lock = self.0
                .read()
                .expect("Failed to acquire write lock in delete queue");
            match *next_read_lock {
                InnerNextBlock::Terminated => {
                    return None;
                }
                InnerNextBlock::Closed(ref block) => {
                    return Some(block.clone());
                }
                _ => {}
            }
        }
        let delete_operations;
        let writer_arc;
        {
            let mut next_write_lock = self.0
                .write()
                .expect("Failed to acquire write lock in delete queue");
            match *next_write_lock {
                InnerNextBlock::Terminated => {
                    return None;
                }
                InnerNextBlock::Closed(ref block) => {
                    return Some(block.clone());
                }
                InnerNextBlock::Writer(ref writer) => {
                    match writer.flush() {
                        Some(flushed_delete_operations) => {
                            delete_operations = flushed_delete_operations;
                        }
                        None => {
                            return None;
                        }
                    }
                    writer_arc = writer.clone();
                }
            }
            let next_block = Arc::new(Block {
                operations: Arc::new(delete_operations),
                next: NextBlock::from(writer_arc),
            });
            *next_write_lock.deref_mut() = InnerNextBlock::Closed(next_block.clone()); // TODO fix
            return Some(next_block)
        }
    }
}

struct Block {
    operations: Arc<Vec<DeleteOperation>>,
    next: NextBlock,
}


#[derive(Clone)]
struct Cursor {
    current_block: Arc<Block>,
    pos: usize,
}

impl Cursor {   
    fn next<'a>(&'a mut self) -> Option<&'a DeleteOperation> {
        if self.pos >= self.current_block.operations.len() {
            // we have consumed our operations entirely.
            // let's ask our writer if he has more for us.
            // self.go_next_block();
            match self.current_block.next.next_block() {
                Some(block) => {
                    self.current_block = block;
                    self.pos = 0;
                }
                None => {
                    return None;
                }
            }
        }
        let operation = &self.current_block.operations[self.pos];
        self.pos += 1;
        return Some(operation);
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