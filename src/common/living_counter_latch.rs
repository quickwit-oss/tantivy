use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::Arc;

pub struct LivingCounterLatch {
	counter: Arc<AtomicUsize>,
}


impl Default for LivingCounterLatch {
    fn default() -> LivingCounterLatch {
		LivingCounterLatch {
			counter: Arc::new(AtomicUsize::default()),
		}
    }
}
 
impl LivingCounterLatch {
    /// Returns true if all the living copies of the 
    /// living counter latch (apart from self) are dead.
    pub fn is_zero(&self,) -> bool {
        self.counter.load(Ordering::Acquire) == 0
    }	
}

impl Clone for LivingCounterLatch {
	fn clone(&self,) -> LivingCounterLatch {
		self.counter.fetch_add(1, Ordering::SeqCst);
		LivingCounterLatch {
			counter: self.counter.clone(),
		}
	}
}

impl Drop for LivingCounterLatch {
	fn drop(&mut self,) {
		self.counter.fetch_sub(1, Ordering::SeqCst);
	}
}


#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::atomic::{Ordering, AtomicUsize};
    use std::sync::Arc;
    use std::thread;
    use std::mem::drop;
    
    const NUM_THREADS: usize = 10;
    const COUNT_PER_THREAD: usize = 100; 
    
    #[test]
    fn test_living_counter_latch() {
        let counter = Arc::new(AtomicUsize::default());
        let living_counter_latch = LivingCounterLatch::default();
        // spawn 10 thread
        for _ in 0..NUM_THREADS {
            let living_counter_latch_clone = living_counter_latch.clone();
            let counter_clone = counter.clone();
            thread::spawn(move || {
                for _ in 0..COUNT_PER_THREAD {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }
                drop(living_counter_latch_clone);
            });
        }
        while !living_counter_latch.is_zero() {};
        assert_eq!(counter.load(Ordering::Acquire), NUM_THREADS * COUNT_PER_THREAD)
    }
}
