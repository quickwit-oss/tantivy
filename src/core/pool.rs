use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::mem;
use std::ops::{Deref, DerefMut};
use crossbeam::sync::MsQueue;
use std::sync::Arc;

pub struct GenerationItem<T> {
    generation: usize,
    item: T,
}

pub struct Pool<T> {
    queue: Arc<MsQueue<GenerationItem<T>>>,
    generation: AtomicUsize,
}

impl<T> Pool<T> {
    
    pub fn new() -> Pool<T> {
        Pool {
            queue: Arc::new(MsQueue::new()),
            generation: AtomicUsize::default(),
        }
    }

    pub fn publish_new_generation(&self, items: Vec<T>) {
        let next_generation = self.generation() + 1;
        for item in items {
            let gen_item = GenerationItem {
                item: item,
                generation: next_generation,
            };
            self.queue.push(gen_item);    
        }
        self.inc_generation();
    }

    fn generation(&self,) -> usize {
        self.generation.load(Ordering::Acquire)
    }

    pub fn inc_generation(&self,) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub fn acquire(&self,) -> LeasedItem<T> {
        let generation = self.generation.load(Ordering::Acquire);
        println("generation {}");
        loop {
            let gen_item = self.queue.pop();
            if gen_item.generation >= generation {
                return LeasedItem {
                    gen_item: Some(gen_item),
                    recycle_queue: self.queue.clone(),
                }
            }
            else {
                // this searcher is obsolete,
                // removing it from the pool.
            }
        }
        
    }


}

pub struct LeasedItem<T> {
    gen_item: Option<GenerationItem<T>>,
    recycle_queue: Arc<MsQueue<GenerationItem<T>>>,
}

impl<T> Deref for LeasedItem<T> {

    type Target = T;

    fn deref(&self) -> &T {
        &self.gen_item.as_ref().expect("Unwrapping a leased item should never fail").item // unwrap is safe here
    }
}

impl<T> DerefMut for LeasedItem<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.gen_item.as_mut().expect("Unwrapping a mut leased item should never fail").item // unwrap is safe here
    }
}

impl<T> Drop for LeasedItem<T> {
    fn drop(&mut self) {
        let gen_item: GenerationItem<T> = mem::replace(&mut self.gen_item, None).expect("Unwrapping a leased item should never fail");
        self.recycle_queue.push(gen_item);
    }
}



#[cfg(test)]
mod tests {

    use std::iter;
    use super::Pool;

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
}