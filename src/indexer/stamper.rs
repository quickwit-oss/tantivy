use crate::Opstamp;
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(not(target_arch = "arm"))]
mod atomic_impl {

    use crate::Opstamp;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    pub struct AtomicU64Wrapper(AtomicU64);

    impl AtomicU64Wrapper {
        pub fn new(first_opstamp: Opstamp) -> AtomicU64Wrapper {
            AtomicU64Wrapper(AtomicU64::new(first_opstamp as u64))
        }

        pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
            self.0.fetch_add(val as u64, order) as u64
        }

        pub fn revert(&self, val: u64, order: Ordering) -> u64 {
            self.0.store(val, order);
            val
        }
    }
}

#[cfg(target_arch = "arm")]
mod atomic_impl {

    use crate::Opstamp;
    /// Under other architecture, we rely on a mutex.
    use std::sync::atomic::Ordering;
    use std::sync::RwLock;

    #[derive(Default)]
    pub struct AtomicU64Wrapper(RwLock<u64>);

    impl AtomicU64Wrapper {
        pub fn new(first_opstamp: Opstamp) -> AtomicU64Wrapper {
            AtomicU64Wrapper(RwLock::new(first_opstamp))
        }

        pub fn fetch_add(&self, incr: u64, _order: Ordering) -> u64 {
            let mut lock = self.0.write().unwrap();
            let previous_val = *lock;
            *lock = previous_val + incr;
            previous_val
        }

        pub fn revert(&self, val: u64, _order: Ordering) -> u64 {
            let mut lock = self.0.write().unwrap();
            *lock = val;
            val
        }
    }
}

use self::atomic_impl::AtomicU64Wrapper;

/// Stamper provides Opstamps, which is just an auto-increment id to label
/// an operation.
///
/// Cloning does not "fork" the stamp generation. The stamper actually wraps an `Arc`.
#[derive(Clone, Default)]
pub struct Stamper(Arc<AtomicU64Wrapper>);

impl Stamper {
    pub fn new(first_opstamp: Opstamp) -> Stamper {
        Stamper(Arc::new(AtomicU64Wrapper::new(first_opstamp)))
    }

    pub fn stamp(&self) -> Opstamp {
        self.0.fetch_add(1u64, Ordering::SeqCst) as u64
    }

    /// Given a desired count `n`, `stamps` returns an iterator that
    /// will supply `n` number of u64 stamps.
    pub fn stamps(&self, n: u64) -> Range<Opstamp> {
        let start = self.0.fetch_add(n, Ordering::SeqCst);
        Range {
            start,
            end: start + n,
        }
    }

    /// Reverts the stamper to a given `Opstamp` value and returns it
    pub fn revert(&self, to_opstamp: Opstamp) -> Opstamp {
        self.0.revert(to_opstamp, Ordering::SeqCst)
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
        assert_eq!(stamper.stamps(3u64), (12..15));
        assert_eq!(stamper.stamp(), 15u64);
    }

    #[test]
    fn test_stamper_revert() {
        let stamper = Stamper::new(7u64);
        assert_eq!(stamper.stamp(), 7u64);
        assert_eq!(stamper.stamp(), 8u64);

        let stamper_clone = stamper.clone();
        assert_eq!(stamper_clone.stamp(), 9u64);

        stamper.revert(6);
        assert_eq!(stamper.stamp(), 6);
        assert_eq!(stamper_clone.stamp(), 7);
    }
}
