use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Weak;
use std::path::Path;
use std::sync::Arc;

pub type WatchCallback = Box<Fn()->() + 'static + Sync + Send>;


/// Helper struct to implement the watch method in `Directory` implementations.
///
/// It registers callbacks associated to a given path (See `.suscribe(...)`) and
/// calls them upon calls to `.broadcast(...)`.
/// ```
#[derive(Default)]
pub struct WatchEventRouter {
    router: HashMap<PathBuf, Vec<Weak<WatchCallback>>>
}

pub struct WatchHandle(Arc<WatchCallback>);

impl WatchEventRouter {

    /// Returns true if the path had no watcher before.
    ///
    /// The return value is useful if you need to bind the `WatchEventRouter` to an external event
    /// like watching the filesystem in the `Mmapdirectory`.
    pub fn subscribe(&mut self, path: &Path, watch_callback: WatchCallback) -> (bool, WatchHandle) {
        let watch_callback_arc = Arc::new(watch_callback);
        let created = !self.router.contains_key(path);
        self
            .router
            .entry(path.to_owned())
            .or_insert_with(Vec::default)
            .push(Arc::downgrade(&watch_callback_arc));
        (created, WatchHandle(watch_callback_arc))
    }


    /// Triggers all callbacks associated to the given path.
    ///
    /// Returns true if there used to be some callbacks associated to this
    /// path, but we detected that they were all removed during this call to broadcast.
    ///
    /// The return value is useful for instance in the `MMapDirectory`, to know when to `unwatch`
    /// files.
    pub fn broadcast(&mut self, path: &Path) -> bool {
        let mut need_to_remove: bool = false;
        if let Some(watchs) = self.router.get_mut(path) {
            let mut i = 0;
            while i < watchs.len() {
                if let Some(watch) = watchs[i].upgrade() {
                    watch();
                    i += 1;
                } else {
                    watchs.swap_remove(i);
                }
            }
            need_to_remove = watchs.is_empty();
        }
        if need_to_remove {
            self.router.remove(path);
        }
        need_to_remove
    }

    /// For tests purpose
    pub(crate) fn len(&self) -> usize {
        self.router.len()
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
        let mut watch_event_router = WatchEventRouter::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let inc_callback =
            Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(0, counter.load(Ordering::SeqCst));
        let (added_a, handle_a) = watch_event_router.subscribe(Path::new("a"), inc_callback);
        assert_eq!(watch_event_router.len(), 1);
        assert!(added_a);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        watch_event_router.broadcast(Path::new("a"));
        assert_eq!(1, counter.load(Ordering::SeqCst));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert_eq!(4, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        assert!(watch_event_router.broadcast(Path::new("a")));
        assert_eq!(4, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_watch_event_router_multiple_callback_same_key() {
        let mut watch_event_router = WatchEventRouter::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let inc_callback = |inc: usize| {
            let counter_clone = counter.clone();
            Box::new(move || {
                counter_clone.fetch_add(inc, Ordering::SeqCst);
            })
        };
        let (added_a, handle_a) = watch_event_router.subscribe(Path::new("a"), inc_callback(1));
        let (added_a2, handle_a2) = watch_event_router.subscribe(Path::new("a"), inc_callback(10));
        assert_eq!(watch_event_router.len(), 1); //< counts keys, not listeners
        assert!(added_a);
        assert!(!added_a2);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert_eq!(32, counter.load(Ordering::SeqCst));
        mem::drop(handle_a2);
        assert!(watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert_eq!(32, counter.load(Ordering::SeqCst));
        assert_eq!(watch_event_router.len(), 0);
    }


    #[test]
    fn test_watch_event_router_multiple_callback_different_key() {
        let mut watch_event_router = WatchEventRouter::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let inc_callback = |inc: usize| {
            let counter_clone = counter.clone();
            Box::new(move || {
                counter_clone.fetch_add(inc, Ordering::SeqCst);
            })
        };
        let (added_a, handle_a) = watch_event_router.subscribe(Path::new("a"), inc_callback(1));
        let (added_b, handle_b) = watch_event_router.subscribe(Path::new("b"), inc_callback(10));
        assert_eq!(watch_event_router.len(), 2);
        assert!(added_a);
        assert!(added_b);
        assert_eq!(0, counter.load(Ordering::SeqCst));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert_eq!(2, counter.load(Ordering::SeqCst));
        assert!(!watch_event_router.broadcast(Path::new("b")));
        assert_eq!(12, counter.load(Ordering::SeqCst));
        mem::drop(handle_a);
        assert!(watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("b")));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        mem::drop(handle_b);
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert!(!watch_event_router.broadcast(Path::new("a")));
        assert!(watch_event_router.broadcast(Path::new("b")));
        assert!(!watch_event_router.broadcast(Path::new("b")));
        assert_eq!(22, counter.load(Ordering::SeqCst));
        assert_eq!(0, watch_event_router.len());
    }

}