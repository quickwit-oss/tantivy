use crossbeam::crossbeam_channel::unbounded;
use crossbeam::{Receiver, RecvError, Sender};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct GenerationItem<T> {
    generation: usize,
    item: T,
}

/// Queue to replace the Michael-Scott Queue, now deprecated in crossbeam
/// Uses the unbounded Linked-List type queue from crossbeam-channel
/// Splits the Queue into sender and receiver
struct Queue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> Queue<T> {
    fn new() -> Self {
        let (s, r) = unbounded();
        Queue {
            sender: s,
            receiver: r,
        }
    }

    /// Sender trait returns a Result type, which is ignored.
    /// The Result is not handled at the moment
    fn push(&self, elem: T) {
        self.sender.send(elem);
    }

    /// Relies on the underlying crossbeam-channel Receiver
    /// to block on empty queue
    fn pop(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }
}

/// An object pool
///
/// This is used in tantivy to create a pool of `Searcher`.
/// Object are wrapped in a `LeasedItem` wrapper and are
/// released automatically back into the pool on `Drop`.
pub struct Pool<T> {
    queue: Arc<Queue<GenerationItem<T>>>,
    freshest_generation: AtomicUsize,
    next_generation: AtomicUsize,
}

impl<T> Pool<T> {
    pub fn new() -> Pool<T> {
        let queue = Arc::new(Queue::new());
        Pool {
            queue,
            freshest_generation: AtomicUsize::default(),
            next_generation: AtomicUsize::default(),
        }
    }

    /// Publishes a new generation of `Searcher`.
    ///
    /// After publish, all new `Searcher` acquired will be
    /// of the new generation.
    pub fn publish_new_generation(&self, items: Vec<T>) {
        let next_generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
        for item in items {
            let gen_item = GenerationItem {
                item,
                generation: next_generation,
            };
            self.queue.push(gen_item);
        }
        self.advertise_generation(next_generation);
    }

    /// At the exit of this method,
    /// - freshest_generation has a value greater or equal than generation
    /// - freshest_generation has a value that has been advertised
    /// - freshest_generation has)
    fn advertise_generation(&self, generation: usize) {
        // not optimal at all but the easiest to read proof.
        loop {
            let former_generation = self.freshest_generation.load(Ordering::Acquire);
            if former_generation >= generation {
                break;
            }
            self.freshest_generation.compare_and_swap(
                former_generation,
                generation,
                Ordering::SeqCst,
            );
        }
    }

    fn generation(&self) -> usize {
        self.freshest_generation.load(Ordering::Acquire)
    }

    /// Acquires a new searcher.
    ///
    /// If no searcher is available, this methods block until
    /// a searcher is released.
    pub fn acquire(&self) -> LeasedItem<T> {
        let generation = self.generation();
        loop {
            let gen_item = self.queue.pop().unwrap();
            if gen_item.generation >= generation {
                return LeasedItem {
                    gen_item: Some(gen_item),
                    recycle_queue: Arc::clone(&self.queue),
                };
            } else {
                // this searcher is obsolete,
                // removing it from the pool.
            }
        }
    }
}

pub struct LeasedItem<T> {
    gen_item: Option<GenerationItem<T>>,
    recycle_queue: Arc<Queue<GenerationItem<T>>>,
}

impl<T> Deref for LeasedItem<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self
            .gen_item
            .as_ref()
            .expect("Unwrapping a leased item should never fail")
            .item // unwrap is safe here
    }
}

impl<T> DerefMut for LeasedItem<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self
            .gen_item
            .as_mut()
            .expect("Unwrapping a mut leased item should never fail")
            .item // unwrap is safe here
    }
}

impl<T> Drop for LeasedItem<T> {
    fn drop(&mut self) {
        if let Some(gen_item) = self.gen_item.take() {
            self.recycle_queue.push(gen_item);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Pool;
    use super::Queue;
    use std::iter;

    #[test]
    fn test_pool() {
        let items10: Vec<usize> = iter::repeat(10).take(10).collect();
        let pool = Pool::new();
        pool.publish_new_generation(items10);
        for _ in 0..20 {
            assert_eq!(*pool.acquire(), 10);
        }
        let items11: Vec<usize> = iter::repeat(11).take(10).collect();
        pool.publish_new_generation(items11);
        for _ in 0..20 {
            assert_eq!(*pool.acquire(), 11);
        }
    }

    #[test]
    fn test_queue() {
        let q = Queue::new();
        let elem = 5;
        q.push(elem);
        let res = q.pop();
        assert_eq!(res.unwrap(), elem);
    }

    #[test]
    fn test_queue_pushes_and_pops_order() {
        let elements = vec![1, 2, 3, 4];
        let q = Queue::new();
        for el in elements.iter() {
            q.push(el);
        }
        for elem in elements.iter() {
            let popped_el = q.pop().unwrap();
            assert_eq!(popped_el, elem);
        }
    }

    // #[test]
    //     fn test_pool_dont_panic_on_empty_pop() {
    //         use std::{thread, time};
    //         let pool = Pool::new();
    //         let elements_for_pool = vec![1, 2];
    //         // Clone vector, so we can calculate its length later
    //         pool.publish_new_generation(elements_for_pool.clone());

    //         let mut threads = vec![];
    //         let sleep_dur = time::Duration::from_millis(10);
    //         // spawn one more thread than there are elements in the pool
    //         for _thread_idx in 0..elements_for_pool.len() + 1 {
    //             threads.push(thread::spawn(move || {
    //                 pool.acquire();
    //                 thread::sleep(sleep_dur);
    //             }));
    //         }
    //     }
}
