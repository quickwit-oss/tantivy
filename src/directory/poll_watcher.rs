use crate::directory::{WatchCallback, WatchCallbackList, WatchHandle};
use sha2::{digest, Digest, Sha256};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

type MetaContentDigest = digest::Output<Sha256>;

const POLL_INTERVAL_MS: u64 = 500;

pub struct PollWatcher {
    watcher_router: Arc<WatchCallbackList>,
}

/// Returns a SHA-256 of the content of the given path.
fn digest_file_content(path: &Path) -> io::Result<MetaContentDigest> {
    let content = fs::read_to_string(path)?;
    Ok(Sha256::digest(content.as_bytes()))
}

struct PollWatcherState {
    digest: MetaContentDigest,
    path: PathBuf,
}

impl PollWatcherState {
    fn new(path: PathBuf) -> PollWatcherState {
        let digest: MetaContentDigest =
            digest_file_content(&path).unwrap_or_else(|_| MetaContentDigest::default());
        PollWatcherState { digest, path }
    }

    fn update_detected(&mut self) -> io::Result<bool> {
        let digest = digest_file_content(&self.path)?;
        let has_changed = digest != self.digest;
        self.digest = digest;
        Ok(has_changed)
    }
}

impl PollWatcher {
    pub fn new(meta_path: PathBuf) -> PollWatcher {
        let watcher_router: Arc<WatchCallbackList> = Default::default();
        let watcher_router_weak = Arc::downgrade(&watcher_router);
        let mut state = PollWatcherState::new(meta_path.clone());
        thread::Builder::new()
            .name("meta-file-watch-thread".to_string())
            .spawn(move || {
                // The thread will exit once the PollWatcher is dropped, as the watcher router
                // will no be upgraded.
                while let Some(watcher_router) = watcher_router_weak.upgrade() {
                    thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
                    match state.update_detected() {
                        Ok(true) => {
                            futures::executor::block_on(watcher_router.broadcast());
                        }
                        Ok(false) => {}
                        Err(err) => error!(
                            "Failed to check if the file {:?} was modified. {:?}",
                            meta_path, err
                        ),
                    }
                }
            })
            .expect("Failed to spawn Polling thread");
        PollWatcher { watcher_router }
    }

    pub fn watch(&mut self, watch_callback: WatchCallback) -> WatchHandle {
        self.watcher_router.subscribe(watch_callback)
    }
}

#[cfg(test)]
mod tests {}
