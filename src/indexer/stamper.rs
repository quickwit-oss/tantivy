use std::sync::Arc;
use std::sync::atomic::Ordering;


// AtomicU64 have not landed in stable.
// For the moment let's just use AtomicUsize on
// x86/64 bit platform, and a mutex on other platform.
#[cfg(target_arch = "x86_64")]
mod archicture_impl {

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    pub struct AtomicU64Ersatz(AtomicUsize);

    impl AtomicU64Ersatz {
        pub fn new(first_opstamp: u64) -> AtomicU64Ersatz {
            AtomicU64Ersatz(AtomicUsize::new(first_opstamp as usize))
        }

        pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
            self.0.fetch_add(val as usize, order) as u64
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
mod archicture_impl {

    use std::sync::atomic::Ordering;
    /// Under other architecture, we rely on a mutex.
    use std::sync::RwLock;

    #[derive(Default)]
    pub struct AtomicU64Ersatz(RwLock<u64>);

    impl AtomicU64Ersatz {
        pub fn new(first_opstamp: u64) -> AtomicU64Ersatz {
            AtomicU64Ersatz(RwLock::new(first_opstamp))
        }

        pub fn fetch_add(&self, incr: u64, _order: Ordering) -> u64 {
            let mut lock = self.0.write().unwrap();
            let previous_val = *lock;
            *lock = previous_val + incr;
            previous_val
        }
    }
}

use self::archicture_impl::AtomicU64Ersatz;

#[derive(Clone, Default)]
pub struct Stamper(Arc<AtomicU64Ersatz>);

impl Stamper {
    pub fn new(first_opstamp: u64) -> Stamper {
        Stamper(Arc::new(AtomicU64Ersatz::new(first_opstamp)))
    }

    pub fn stamp(&self) -> u64 {
        self.0.fetch_add(1u64, Ordering::SeqCst) as u64
    }
}


#[cfg(test)]
mod test {

    use super::Stamper;

    #[test]
    fn test_stamper() {
        let stamper = Stamper::new(7u64);
        assert_eq!(stamper.stamp(), 7u64);
        assert_eq!(stamper.stamp(), 8u64);

        let stamper_clone = stamper.clone();
        assert_eq!(stamper.stamp(), 9u64);

        assert_eq!(stamper.stamp(), 10u64);
        assert_eq!(stamper_clone.stamp(), 11u64);
    }
}
