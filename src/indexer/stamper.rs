use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct Stamper(Arc<AtomicUsize>);

impl Stamper {
    pub fn new(first_opstamp: u64) -> Stamper {
        Stamper(Arc::new(AtomicUsize::new(first_opstamp as usize)))
    }

    pub fn stamp(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst) as u64
    }
}
