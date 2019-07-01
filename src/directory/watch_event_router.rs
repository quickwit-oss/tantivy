use std::sync::Arc;
use std::sync::RwLock;
use std::sync::Weak;

/// Type alias for callbacks registered when watching files of a `Directory`.
pub type WatchCallback = Box<dyn Fn() -> () + Sync + Send>;

/// Helper struct to implement the watch method in `Directory` implementations.
///
/// It registers callbacks (See `.subscribe(...)`) and
/// calls them upon calls to `.broadcast(...)`.
#[derive(Default)]
pub struct WatchCallbackList {
    router: RwLock<Vec<Weak<WatchCallback>>>,
}

/// Controls how long a directory should watch for a file change.
///
/// After all the clones of `WatchHandle` are dropped, the associated will not be called when a
/// file change is detected.
#[must_use = "This `WatchHandle` controls the lifetime of the watch and should therefore be used."]
#[derive(Clone)]
pub struct WatchHandle(Arc<WatchCallback>);

impl WatchCallbackList {
    /// Suscribes a new callback and returns a handle that controls the lifetime of the callback.
    pub fn subscribe(&self, watch_callback: WatchCallback) -> WatchHandle {
        let watch_callback_arc = Arc::new(watch_callback);
        let watch_callback_weak = Arc::downgrade(&watch_callback_arc);
        self.router.write().unwrap().push(watch_callback_weak);
        WatchHandle(watch_callback_arc)
    }

    fn list_callback(&self) -> Vec<Arc<WatchCallback>> {
        let mut callbacks = vec![];
        let mut router_wlock = self.router.write().unwrap();
        let mut i = 0;
        while i < router_wlock.len() {
            if let Some(watch) = router_wlock[i].upgrade() {
                callbacks.push(watch);
                i += 1;
            } else {
                router_wlock.swap_remove(i);
            }
        }
        callbacks
    }

    /// Triggers all callbacks
    pub fn broadcast(&self) {
        let callbacks = self.list_callback();
        let spawn_res = std::thread::Builder::new()
            .name("watch-callbacks".to_string())
            .spawn(move || {
                for callback in callbacks {
                    callback();
                }
            });
        if let Err(err) = spawn_res {
            error!(
                "Failed to spawn thread to call watch callbacks. Cause: {:?}",
                err
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::directory::WatchCallbackList;
    use std::mem;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    const WAIT_TIME: u64 = 20;

    #[test]
    fn test_watch_event_router_simple() {
        let watch_event_router = WatchCallbackList::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let inc_callback = Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        watch_event_router.broadcast();
        assert_eq!(0, counter.load(Ordering::SeqCst));
        let handle_a = watch_event_router.subscribe(inc_callback);
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(1, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast();
        watch_event_router.broadcast();
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(4, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(4, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_watch_event_router_multiple_callback_same_key() {
        let watch_event_router = WatchCallbackList::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let inc_callback = |inc: usize| {
            let counter_clone = counter.clone();
            Box::new(move || {
                counter_clone.fetch_add(inc, Ordering::SeqCst);
            })
        };
        let handle_a = watch_event_router.subscribe(inc_callback(1));
        let handle_a2 = watch_event_router.subscribe(inc_callback(10));
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast();
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(32, counter.load(Ordering::SeqCst));
        mem::drop(handle_a2);
        watch_event_router.broadcast();
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(32, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_watch_event_router_multiple_callback_different_key() {
        let watch_event_router = WatchCallbackList::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let inc_callback = Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        let handle_a = watch_event_router.subscribe(inc_callback);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast();
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(2, counter.load(Ordering::SeqCst));
        thread::sleep(Duration::from_millis(WAIT_TIME));
        mem::drop(handle_a);
        watch_event_router.broadcast();
        thread::sleep(Duration::from_millis(WAIT_TIME));
        assert_eq!(2, counter.load(Ordering::SeqCst));
    }

}
