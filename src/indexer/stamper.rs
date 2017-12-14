use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct Stamper(Arc<AtomicU64>);

impl Stamper {
    pub fn new(first_opstamp: u64) -> Stamper {
        Stamper(Arc::new(AtomicU64::new(first_opstamp)))
    }

    pub fn stamp(&self) -> u64 {
        self.0.fetch_add(1u64, Ordering::SeqCst)
    }
}
