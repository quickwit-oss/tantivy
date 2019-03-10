use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Weak;
use std::path::Path;
use std::sync::Arc;

pub type WatchCallback = Box<Fn()->() + 'static + Sync + Send>;

#[derive(Default)]
pub struct WatchEventRouter {
    router: HashMap<PathBuf, Vec<Weak<WatchCallback>>>
}

pub struct WatchHandle(Arc<WatchCallback>);

impl WatchEventRouter {
    /// Returns true if the path had no watcher before.
    pub fn suscribe(&mut self, path: &Path, watch_callback: WatchCallback) -> (bool, WatchHandle) {
        let watch_callback_arc = Arc::new(watch_callback);
        let created = self.router.contains_key(path);
        self
            .router
            .entry(path.to_owned())
            .or_insert_with(Vec::default)
            .push(Arc::downgrade(&watch_callback_arc));
        (created, WatchHandle(watch_callback_arc))
    }

    /// Returns true if there used to be some callbacks associated to this
    /// path, but they were all removed during this call to broadcast.
    pub fn broadcast(&mut self, path: &Path) -> bool {
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
            return watchs.is_empty();
        }
        false
    }
}