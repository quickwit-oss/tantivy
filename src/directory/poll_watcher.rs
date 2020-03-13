use std::sync::Arc;
use std::time::{UNIX_EPOCH};
use std::thread;
use crate::directory::{WatchCallback, WatchHandle, WatchCallbackList};
use crate::directory::error::{
    DeleteError, IOError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use std::path::{Path, PathBuf};
use crate::core::META_FILEPATH;


struct PollWatcher {
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
                let mut current_meta_time: u128 = Self::meta_last_update(&meta_path);
                loop {
                    let new_meta_time: u128 = Self::meta_last_update(&meta_path);

                    if new_meta_time > current_meta_time {
                        watcher_router_clone.broadcast();
                    }
                }
            })?;
        Ok(Self {
            watcher_router,
        })
    }

    fn meta_last_update(meta_path: &Path) -> u128 {
        let data = meta_path.metadata().unwrap();
        let modified = data.modified().unwrap();
        return modified.duration_since(UNIX_EPOCH).unwrap().as_nanos()
    }

    pub fn watch(&mut self, watch_callback: WatchCallback) -> WatchHandle {
        self.watcher_router.subscribe(watch_callback)
    }

}

#[cfg(test)]
mod tests {

    #[test]
    fn test_poll_watcher() {


        
    }

}

