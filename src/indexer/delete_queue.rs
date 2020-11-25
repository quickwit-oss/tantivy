use super::operation::DeleteOperation;
use crate::Opstamp;
use std::mem;
use std::ops::DerefMut;
use std::sync::{Arc, RwLock, Weak};

// The DeleteQueue is similar in conceptually to a multiple
// consumer single producer broadcast channel.
//
// All consumer will receive all messages.
//
// Consumer of the delete queue are holding a `DeleteCursor`,
// which points to a specific place of the `DeleteQueue`.
//
// New consumer can be created in two ways
// - calling `delete_queue.cursor()` returns a cursor, that
//   will include all future delete operation (and some or none
//   of the past operations... The client is in charge of checking the opstamps.).
// - cloning an existing cursor returns a new cursor, that
//   is at the exact same position, and can now advance independently
//   from the original cursor.
#[derive(Default)]
struct InnerDeleteQueue {
    writer: Vec<DeleteOperation>,
    last_block: Weak<Block>,
}

#[derive(Clone)]
pub struct DeleteQueue {
    inner: Arc<RwLock<InnerDeleteQueue>>,
}

impl DeleteQueue {
    // Creates a new delete queue.
    pub fn new() -> DeleteQueue {
        DeleteQueue {
            inner: Arc::default(),
        }
    }

    fn get_last_block(&self) -> Arc<Block> {
        {
            // try get the last block with simply acquiring the read lock.
            let rlock = self.inner.read().unwrap();
            if let Some(block) = rlock.last_block.upgrade() {
                return block;
            }
        }
        // It failed. Let's double check after acquiring the write, as someone could have called
        // `get_last_block` right after we released the rlock.
        let mut wlock = self.inner.write().unwrap();
        if let Some(block) = wlock.last_block.upgrade() {
            return block;
        }
        let block = Arc::new(Block {
            operations: Arc::new([]),
            next: NextBlock::from(self.clone()),
        });
        wlock.last_block = Arc::downgrade(&block);
        block
    }

    // Creates a new cursor that makes it possible to
    // consume future delete operations.
    //
    // Past delete operations are not accessible.
    pub fn cursor(&self) -> DeleteCursor {
        let last_block = self.get_last_block();
        let operations_len = last_block.operations.len();
        DeleteCursor {
            block: last_block,
            pos: operations_len,
        }
    }

    // Appends a new delete operations.
    pub fn push(&self, delete_operation: DeleteOperation) {
        self.inner
            .write()
            .expect("Failed to acquire write lock on delete queue writer")
            .writer
            .push(delete_operation);
    }

    // DeleteQueue is a linked list of blocks of
    // delete operations.
    //
    // Writing happens by simply appending to a vec.
    // `.flush()` takes this pending delete operations vec
    // creates a new read-only block from it,
    // and appends it to the linked list.
    //
    // `.flush()` happens when, for instance,
    // a consumer reaches the last read-only operations.
    // It then ask the delete queue if there happen to
    // be some unflushed operations.
    //
    fn flush(&self) -> Option<Arc<Block>> {
        let mut self_wlock = self
            .inner
            .write()
            .expect("Failed to acquire write lock on delete queue writer");

        if self_wlock.writer.is_empty() {
            return None;
        }

        let delete_operations = mem::replace(&mut self_wlock.writer, vec![]);

        let new_block = Arc::new(Block {
            operations: Arc::from(delete_operations.into_boxed_slice()),
            next: NextBlock::from(self.clone()),
        });

        self_wlock.last_block = Arc::downgrade(&new_block);
        Some(new_block)
    }
}

enum InnerNextBlock {
    Writer(DeleteQueue),
    Closed(Arc<Block>),
}

struct NextBlock(RwLock<InnerNextBlock>);

impl From<DeleteQueue> for NextBlock {
    fn from(delete_queue: DeleteQueue) -> NextBlock {
        NextBlock(RwLock::new(InnerNextBlock::Writer(delete_queue)))
    }
}

impl NextBlock {
    fn next_block(&self) -> Option<Arc<Block>> {
        {
            let next_read_lock = self
                .0
                .read()
                .expect("Failed to acquire write lock in delete queue");
            if let InnerNextBlock::Closed(ref block) = *next_read_lock {
                return Some(Arc::clone(block));
            }
        }
        let next_block;
        {
            let mut next_write_lock = self
                .0
                .write()
                .expect("Failed to acquire write lock in delete queue");
            match *next_write_lock {
                InnerNextBlock::Closed(ref block) => {
                    return Some(Arc::clone(block));
                }
                InnerNextBlock::Writer(ref writer) => match writer.flush() {
                    Some(flushed_next_block) => {
                        next_block = flushed_next_block;
                    }
                    None => {
                        return None;
                    }
                },
            }
            *next_write_lock.deref_mut() = InnerNextBlock::Closed(Arc::clone(&next_block));
            Some(next_block)
        }
    }
}

struct Block {
    operations: Arc<[DeleteOperation]>,
    next: NextBlock,
}

#[derive(Clone)]
pub struct DeleteCursor {
    block: Arc<Block>,
    pos: usize,
}

impl DeleteCursor {
    /// Skips operations and position it so that
    /// - either all of the delete operation currently in the
    ///   queue are consume and the next get will return None.
    /// - the next get will return the first operation with an
    /// `opstamp >= target_opstamp`.
    pub fn skip_to(&mut self, target_opstamp: Opstamp) {
        // TODO Can be optimize as we work with block.
        while self.is_behind_opstamp(target_opstamp) {
            self.advance();
        }
    }

    #[cfg_attr(feature = "cargo-clippy", allow(clippy::wrong_self_convention))]
    fn is_behind_opstamp(&mut self, target_opstamp: Opstamp) -> bool {
        self.get()
            .map(|operation| operation.opstamp < target_opstamp)
            .unwrap_or(false)
    }

    /// If the current block has been entirely
    /// consumed, try to load the next one.
    ///
    /// Return `true`, if after this attempt,
    /// the cursor is on a block that has not
    /// been entirely consumed.
    /// Return `false`, if we have reached the end of the queue.
    fn load_block_if_required(&mut self) -> bool {
        if self.pos >= self.block.operations.len() {
            // we have consumed our operations entirely.
            // let's ask our writer if he has more for us.
            // self.go_next_block();
            match self.block.next.next_block() {
                Some(block) => {
                    self.block = block;
                    self.pos = 0;
                    true
                }
                None => false,
            }
        } else {
            true
        }
    }

    /// Advance to the next delete operation.
    /// Returns true iff there is such an operation.
    pub fn advance(&mut self) -> bool {
        if self.load_block_if_required() {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Get the current delete operation.
    /// Calling `.get` does not advance the cursor.
    pub fn get(&mut self) -> Option<&DeleteOperation> {
        if self.load_block_if_required() {
            Some(&self.block.operations[self.pos])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{DeleteOperation, DeleteQueue};
    use crate::schema::{Field, Term};

    #[test]
    fn test_deletequeue() {
        let delete_queue = DeleteQueue::new();

        let make_op = |i: usize| {
            let field = Field::from_field_id(1u32);
            DeleteOperation {
                opstamp: i as u64,
                term: Term::from_field_u64(field, i as u64),
            }
        };

        delete_queue.push(make_op(1));
        delete_queue.push(make_op(2));

        let snapshot = delete_queue.cursor();
        {
            let mut operations_it = snapshot.clone();
            assert_eq!(operations_it.get().unwrap().opstamp, 1);
            operations_it.advance();
            assert_eq!(operations_it.get().unwrap().opstamp, 2);
            operations_it.advance();
            assert!(operations_it.get().is_none());
            operations_it.advance();

            let mut snapshot2 = delete_queue.cursor();
            assert!(snapshot2.get().is_none());
            delete_queue.push(make_op(3));
            assert_eq!(snapshot2.get().unwrap().opstamp, 3);
            assert_eq!(operations_it.get().unwrap().opstamp, 3);
            assert_eq!(operations_it.get().unwrap().opstamp, 3);
            operations_it.advance();
            assert!(operations_it.get().is_none());
            operations_it.advance();
        }
        {
            let mut operations_it = snapshot.clone();
            assert_eq!(operations_it.get().unwrap().opstamp, 1);
            operations_it.advance();
            assert_eq!(operations_it.get().unwrap().opstamp, 2);
            operations_it.advance();
            assert_eq!(operations_it.get().unwrap().opstamp, 3);
            operations_it.advance();
            assert!(operations_it.get().is_none());
        }
    }
}
