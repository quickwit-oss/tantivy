use crate::core::META_FILEPATH;
use crate::directory::error::OpenDirectoryError;
use crate::directory::{WatchCallback, WatchCallbackList, WatchHandle};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

const POLL_INTERVAL_MS: u64 = 1;

pub struct PollWatcher {
    watcher_router: Arc<WatchCallbackList>,
}

impl PollWatcher {
    pub fn new(path: &Path) -> Result<Self, OpenDirectoryError> {
        let watcher_router: Arc<WatchCallbackList> = Default::default();
        let watcher_router_clone = watcher_router.clone();
        let meta_path = path.to_owned().join(*META_FILEPATH);
        thread::Builder::new()
            .name("meta-file-watch-thread".to_string())
            .spawn(move || {
                let mut current_meta_time: u128 = Self::meta_last_update(&meta_path).unwrap_or(0);
                loop {
                    let new_meta_time: u128 = Self::meta_last_update(&meta_path).unwrap_or(0);
                    if new_meta_time > current_meta_time {
                        current_meta_time = new_meta_time;
                        let _ = watcher_router_clone.broadcast();
                    }
                    thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
                }
            })?;
        Ok(Self { watcher_router })
    }

    fn meta_last_update(meta_path: &Path) -> Result<u128, io::Error> {
        let meta = meta_path.metadata()?.modified()?;
        Ok(meta.duration_since(UNIX_EPOCH).unwrap().as_nanos())
    }

    pub fn watch(&mut self, watch_callback: WatchCallback) -> WatchHandle {
        self.watcher_router.subscribe(watch_callback)
    }
}

#[cfg(test)]
mod tests {}
