use crossbeam::channel::unbounded;
use crossbeam::channel::{Receiver, RecvError, Sender};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct GenerationItem<T> {
    generation: usize,
    item: T,
}

/// Queue implementation for the Object Pool below
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
        self.sender
            .send(elem)
            .expect("Sending an item to crossbeam-queue shouldn't fail");
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
        assert!(!items.is_empty());
        let next_generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let num_items = items.len();
        for item in items {
            let gen_item = GenerationItem {
                item,
                generation: next_generation,
            };
            self.queue.push(gen_item);
        }
        self.advertise_generation(next_generation);
        // Purge possible previous searchers.
        //
        // Assuming at this point no searcher is held more than duration T by the user,
        // this guarantees that an obsolete searcher will not be uselessly held (and its associated
        // mmap) for more than duration T.
        //
        // Proof: At this point, obsolete searcher that are held by the user will be held for less
        // than T. When released, they will be dropped as their generation is detected obsolete.
        //
        // We still need to ensure that the searcher that are obsolete and in the pool get removed.
        // The queue currently contains up to 2n searchers, in any random order.
        //
        // Half of them are obsoletes. By requesting `(n+1)` fresh searchers, we ensure that all
        // searcher will be inspected.
        for _ in 0..=num_items {
            let _ = self.acquire();
        }
    }

    /// At the exit of this method,
    /// - freshest_generation has a value greater or equal than generation
    /// - freshest_generation has the last value that has been advertised
    fn advertise_generation(&self, generation: usize) {
        // not optimal at all but the easiest to read proof.
        let mut former_generation = self.freshest_generation.load(Ordering::Acquire);
        loop {
            match self.freshest_generation.compare_exchange(
                former_generation,
                generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // We successfuly updated the value.
                    return;
                }
                Err(current_generation) => {
                    // The value was updated after we did our load apparently.
                    // In theory, it is always a value greater than ours, but just to
                    // simplify the logic, we keep looping until we reach a
                    // value >= to our target value.
                    if current_generation >= generation {
                        return;
                    }
                    former_generation = current_generation;
                }
            }
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

/// A LeasedItem holds an object borrowed from a Pool.
///
/// Upon drop, the object is automatically returned
/// into the pool.
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
    use crossbeam::channel;
    use std::{iter, mem};

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
    fn test_pool_dont_panic_on_empty_pop() {
        // When the object pool is exhausted, it shouldn't panic on pop()
        use std::sync::Arc;
        use std::thread;

        // Wrap the pool in an Arc, same way as its used in `core/index.rs`
        let pool1 = Arc::new(Pool::new());
        // clone pools outside the move scope of each new thread
        let pool2 = Arc::clone(&pool1);
        let pool3 = Arc::clone(&pool1);

        let elements_for_pool = vec![1, 2];
        pool1.publish_new_generation(elements_for_pool);

        let mut threads = vec![];
        // spawn one more thread than there are elements in the pool

        let (start_1_send, start_1_recv) = channel::bounded(0);
        let (start_2_send, start_2_recv) = channel::bounded(0);
        let (start_3_send, start_3_recv) = channel::bounded(0);

        let (event_send1, event_recv) = channel::unbounded();
        let event_send2 = event_send1.clone();
        let event_send3 = event_send1.clone();

        threads.push(thread::spawn(move || {
            assert_eq!(start_1_recv.recv(), Ok("start"));
            let _leased_searcher = &pool1.acquire();
            assert!(event_send1.send("1 acquired").is_ok());
            assert_eq!(start_1_recv.recv(), Ok("stop"));
            assert!(event_send1.send("1 stopped").is_ok());
            mem::drop(_leased_searcher);
        }));

        threads.push(thread::spawn(move || {
            assert_eq!(start_2_recv.recv(), Ok("start"));
            let _leased_searcher = &pool2.acquire();
            assert!(event_send2.send("2 acquired").is_ok());
            assert_eq!(start_2_recv.recv(), Ok("stop"));
            mem::drop(_leased_searcher);
            assert!(event_send2.send("2 stopped").is_ok());
        }));

        threads.push(thread::spawn(move || {
            assert_eq!(start_3_recv.recv(), Ok("start"));
            let _leased_searcher = &pool3.acquire();
            assert!(event_send3.send("3 acquired").is_ok());
            assert_eq!(start_3_recv.recv(), Ok("stop"));
            mem::drop(_leased_searcher);
            assert!(event_send3.send("3 stopped").is_ok());
        }));

        assert!(start_1_send.send("start").is_ok());
        assert_eq!(event_recv.recv(), Ok("1 acquired"));
        assert!(start_2_send.send("start").is_ok());
        assert_eq!(event_recv.recv(), Ok("2 acquired"));
        assert!(start_3_send.send("start").is_ok());
        assert!(event_recv.try_recv().is_err());
        assert!(start_1_send.send("stop").is_ok());
        assert_eq!(event_recv.recv(), Ok("1 stopped"));
        assert_eq!(event_recv.recv(), Ok("3 acquired"));
        assert!(start_3_send.send("stop").is_ok());
        assert_eq!(event_recv.recv(), Ok("3 stopped"));
        assert!(start_2_send.send("stop").is_ok());
        assert_eq!(event_recv.recv(), Ok("2 stopped"));
    }
}
