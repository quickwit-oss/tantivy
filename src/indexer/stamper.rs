use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use Opstamp;

/// Stamper provides Opstamps, which is just an auto-increment id to label
/// an operation.
///
/// Cloning does not "fork" the stamp generation. The stamper actually wraps an `Arc`.
#[derive(Clone, Default)]
pub struct Stamper(Arc<AtomicU64>);

impl Stamper {
    pub fn new(first_opstamp: Opstamp) -> Stamper {
        Stamper(Arc::new(AtomicU64::new(first_opstamp)))
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

}
