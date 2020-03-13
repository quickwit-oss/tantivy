use crate::core::META_FILEPATH;
use crate::directory::error::OpenDirectoryError;
use crate::directory::{WatchCallback, WatchCallbackList, WatchHandle};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, UNIX_EPOCH};
use std::fs;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};


const POLL_INTERVAL_MS: u64 = 10;

pub struct PollWatcher {
    watcher_router: Arc<WatchCallbackList>,
}

#[derive(Hash)]
struct State {
    data: String,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl PollWatcher {
    pub fn new(path: &Path) -> Result<Self, OpenDirectoryError> {
        let watcher_router: Arc<WatchCallbackList> = Default::default();
        let watcher_router_clone = watcher_router.clone();
        let meta_path = path.to_owned().join(*META_FILEPATH);
        thread::Builder::new()
            .name("meta-file-watch-thread".to_string())
            .spawn(move || {
                let mut current_hash: u64 = Self::hash(&meta_path).unwrap_or(0);
                let mut current_meta_time: u128 = Self::last_update(&meta_path).unwrap_or(0);
                loop {
                    let new_hash: u64 = Self::hash(&meta_path).unwrap_or(0);
                    let new_meta_time: u128 = Self::last_update(&meta_path).unwrap_or(0);
                    // println!("hash: {} {}\ntime: {} {}\n---", new_hash, current_hash, new_meta_time, current_meta_time);
                    if (new_hash != current_hash) || (new_meta_time != current_meta_time) {
                        // println!("update...");
                        current_hash = new_hash;
                        current_meta_time = new_meta_time;
                        let _ = watcher_router_clone.broadcast();
                    }
                    thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
                }
            })?;
        Ok(Self { watcher_router })
    }

    fn hash(path: &Path) -> Result<u64, io::Error> {
        let data = fs::read_to_string(path)?;
        Ok(calculate_hash(&State{data}))
    }

    fn last_update(path: &Path) -> Result<u128, io::Error> {
        let meta = path.metadata()?.modified()?;
        Ok(meta.duration_since(UNIX_EPOCH).unwrap().as_nanos())
    }

    pub fn watch(&mut self, watch_callback: WatchCallback) -> WatchHandle {
        self.watcher_router.subscribe(watch_callback)
    }
}

#[cfg(test)]
mod tests {}
