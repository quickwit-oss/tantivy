use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Weak;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

pub type WatchCallback = Box<Fn()->() + 'static + Sync + Send>;


/// Helper struct to implement the watch method in `Directory` implementations.
///
/// It registers callbacks associated to a given path (See `.subscribe(...)`) and
/// calls them upon calls to `.broadcast(...)`.
///
///
#[derive(Default)]
pub struct WatchEventRouter {
    router: RwLock<HashMap<PathBuf, Vec<Weak<WatchCallback>>>>
}

#[must_use = "This `WatchHandle` controls the lifetime of the watch and should therefore be used."]
pub struct WatchHandle(Arc<WatchCallback>);

impl WatchEventRouter {

    /// Returns true if the path had no watcher before.
    pub fn subscribe(&self, path: &Path, watch_callback: WatchCallback) -> WatchHandle {
        let watch_callback_arc = Arc::new(watch_callback);
        let watch_callback_weak = Arc::downgrade(&watch_callback_arc);
        self
            .router
            .write().unwrap()
            .entry(path.to_owned())
            .or_insert_with(Vec::default)
            .push(watch_callback_weak);
        WatchHandle(watch_callback_arc)
    }

    fn list_callback(&self, path: &Path) -> Vec<Arc<WatchCallback>> {
        let mut callbacks = vec![];
        let mut router_wlock = self.router.write().unwrap();
        let mut need_to_remove: bool = false;
        if let Some(watchs) = router_wlock.get_mut(path) {
            let mut i = 0;
            while i < watchs.len() {
                if let Some(watch) = watchs[i].upgrade() {
                    callbacks.push(watch);
                    i += 1;
                } else {
                    watchs.swap_remove(i);
                }
            }
            need_to_remove = watchs.is_empty();
        }
        if need_to_remove {
            router_wlock.remove(path);
        }
        callbacks
    }

    /// Triggers all callbacks associated to the given path.
    ///
    /// This method might panick if one the callbacks panicks.
    pub fn broadcast(&self, path: &Path) {
        for callback in self.list_callback(path) {
            callback();
        }
    }

    /// For tests purpose
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.router.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use directory::WatchEventRouter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::path::Path;
    use std::mem;

    #[test]
    fn test_watch_event_router_empty() {
        assert_eq!(WatchEventRouter::default().len(), 0);
    }

    #[test]
    fn test_watch_event_router_simple() {
        let watch_event_router = WatchEventRouter::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let inc_callback =
            Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(0, counter.load(Ordering::SeqCst));
        let handle_a = watch_event_router.subscribe(Path::new("a"), inc_callback);
        assert_eq!(watch_event_router.len(), 1);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(1, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(4, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(4, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_watch_event_router_multiple_callback_same_key() {
        let watch_event_router = WatchEventRouter::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let inc_callback = |inc: usize| {
            let counter_clone = counter.clone();
            Box::new(move || {
                counter_clone.fetch_add(inc, Ordering::SeqCst);
            })
        };
        let handle_a = watch_event_router.subscribe(Path::new("a"), inc_callback(1));
        let handle_a2 = watch_event_router.subscribe(Path::new("a"), inc_callback(10));
        assert_eq!(watch_event_router.len(), 1); //< counts keys, not listeners
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(32, counter.load(Ordering::SeqCst));
        mem::drop(handle_a2);
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(32, counter.load(Ordering::SeqCst));
        assert_eq!(watch_event_router.len(), 0);
    }


    #[test]
    fn test_watch_event_router_multiple_callback_different_key() {
        let watch_event_router = WatchEventRouter::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let inc_callback = |inc: usize| {
            let counter_clone = counter.clone();
            Box::new(move || {
                counter_clone.fetch_add(inc, Ordering::SeqCst);
            })
        };
        let handle_a = watch_event_router.subscribe(Path::new("a"), inc_callback(1));
        let handle_b = watch_event_router.subscribe(Path::new("b"), inc_callback(10));
        assert_eq!(watch_event_router.len(), 2);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(2, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast(Path::new("b"));
        assert_eq!(12, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("b"));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        mem::drop(handle_b);
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("a"));
        watch_event_router.broadcast(Path::new("b"));
        watch_event_router.broadcast(Path::new("b"));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        assert_eq!(0, watch_event_router.len());
    }

}