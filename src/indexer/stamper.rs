// AtomicU64 have not landed in stable.
// For the moment let's just use AtomicUsize on
// x86/64 bit platform, and a mutex on other platform.

#[cfg(target="x86_64")]
mod archicture_impl {

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    pub struct Stamper(Arc<AtomicU64>);

    impl Stamper {
        pub fn new(first_opstamp: u64) -> Stamper {
            Stamper(Arc::new(AtomicU64::new(first_opstamp)))
        }

        pub fn stamp(&self) -> u64 {
            self.0.fetch_add(1u64, Ordering::SeqCst) as u64
        }
    }
}


#[cfg(not(target="x86_64"))]
mod archicture_impl {

    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    pub struct Stamper(Arc<Mutex<u64>>);

    impl Stamper {
        pub fn new(first_opstamp: u64) -> Stamper {
            Stamper(Arc::new(Mutex::new(first_opstamp)))
        }

        pub fn stamp(&self) -> u64 {
            let mut guard = self.0.lock().expect("Failed to lock the stamper");
            let previous_val = *guard;
            *guard = previous_val + 1;
            previous_val
        }
    }
}

pub use self::archicture_impl::Stamper;


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