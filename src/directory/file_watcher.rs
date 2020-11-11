use crate::directory::{WatchCallback, WatchCallbackList, WatchHandle};
use crc32fast::Hasher;
use std::fs;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const POLLING_INTERVAL: Duration = Duration::from_millis(if cfg!(test) { 1 } else { 500 });

// Watches a file and executes registered callbacks when the file is modified.
pub struct FileWatcher {
    path: Arc<PathBuf>,
    callbacks: Arc<WatchCallbackList>,
    state: Arc<AtomicUsize>, // 0: new, 1: runnable, 2: terminated
}

impl FileWatcher {
    pub fn new(path: &PathBuf) -> FileWatcher {
        FileWatcher {
            path: Arc::new(path.clone()),
            callbacks: Default::default(),
            state: Default::default(),
        }
    }

    pub fn spawn(&self) {
        if self.state.compare_and_swap(0, 1, Ordering::SeqCst) > 0 {
            return;
        }

        let path = self.path.clone();
        let callbacks = self.callbacks.clone();
        let state = self.state.clone();

        thread::Builder::new()
            .name("thread-tantivy-meta-file-watcher".to_string())
            .spawn(move || {
                let mut current_checksum = None;

                while state.load(Ordering::SeqCst) == 1 {
                    if let Ok(checksum) = FileWatcher::compute_checksum(&path) {
                        // `None.unwrap_or_else(|| !checksum) != checksum` evaluates to `true`
                        if current_checksum.unwrap_or_else(|| !checksum) != checksum {
                            info!("Meta file {:?} was modified", path);
                            current_checksum = Some(checksum);
                            futures::executor::block_on(callbacks.broadcast());
                        }
                    }

                    thread::sleep(POLLING_INTERVAL);
                }
            })
            .expect("Failed to spawn meta file watcher thread");
    }

    pub fn watch(&self, callback: WatchCallback) -> WatchHandle {
        let handle = self.callbacks.subscribe(callback);
        self.spawn();
        handle
    }

    fn compute_checksum(path: &PathBuf) -> Result<u32, io::Error> {
        let reader = match fs::File::open(path) {
            Ok(f) => io::BufReader::new(f),
            Err(e) => {
                warn!("Failed to open meta file {:?}: {:?}", path, e);
                return Err(e);
            }
        };

        let mut hasher = Hasher::new();

        for line in reader.lines() {
            hasher.update(line?.as_bytes())
        }

        Ok(hasher.finalize())
    }
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.state.store(2, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_file_watcher() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let tmp_file = tmp_dir.path().join("watched.txt");

        let counter: Arc<AtomicUsize> = Default::default();
        let _handle;
        let state;
        let (tx, rx) = crossbeam::channel::unbounded();
        let timeout = Duration::from_millis(100);

        {
            let watcher = FileWatcher::new(&tmp_file);

            state = watcher.state.clone();
            assert_eq!(state.load(Ordering::SeqCst), 0);

            let counter_clone = counter.clone();

            _handle = watcher.watch(WatchCallback::new(move || {
                let val = counter_clone.fetch_add(1, Ordering::SeqCst);
                tx.send(val + 1).unwrap();
            }));

            assert_eq!(counter.load(Ordering::SeqCst), 0);
            assert_eq!(state.load(Ordering::SeqCst), 1);

            fs::write(&tmp_file, b"foo").unwrap();
            assert_eq!(rx.recv_timeout(timeout), Ok(1));

            fs::write(&tmp_file, b"foo").unwrap();
            assert!(rx.recv_timeout(timeout).is_err());

            fs::write(&tmp_file, b"bar").unwrap();
            assert_eq!(rx.recv_timeout(timeout), Ok(2));
        }

        fs::write(&tmp_file, b"qux").unwrap();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert_eq!(state.load(Ordering::SeqCst), 2);
    }
}
