use directory::error::OpenWriteError;
use Directory;
use TantivyError;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum LockType {
    /// Only one process should be able to write tantivy's index at a time.
    /// This lock file, when present, is in charge of preventing other processes to open an IndexWriter.
    ///
    /// If the process is killed and this file remains, it is safe to remove it manually.
    ///
    /// Failing to acquire this lock usually means a misuse of tantivy's API,
    /// (creating more than one instance of the `IndexWriter`), are a spurious
    /// lock file remaining after a crash. In the latter case, removing the file after
    /// checking no process running tantivy is running is safe.
    IndexWriterLock,
    /// The meta lock file is here to protect the segment files being opened by
    /// `.load_searchers()` from being garbage collected.
    /// It makes it possible for another process to safely consume
    /// our index in-writing. Ideally, we may have prefered `RWLock` semantics
    /// here, but it is difficult to achieve on Windows.
    ///
    /// Opening segment readers is a very fast process.
    /// Right now if the lock cannot be acquire on the first attempt, the logic
    /// is very simplistic. We retry after `100ms` until we effectively
    /// acquire the lock.
    /// This lock should not have much contention in normal usage.
    MetaLock
}


/// Retry the logic of acquiring locks is pretty simple.
/// We just retry `n` times after a given `duratio`, both
/// depending on the type of lock.
struct RetryPolicy {
    num_retries: usize,
    wait_in_ms: u64,
}

impl RetryPolicy {
    fn no_retry() -> RetryPolicy {
        RetryPolicy {
            num_retries: 0,
            wait_in_ms: 0,
        }
    }

    fn wait_and_retry(&mut self,) -> bool {
        if self.num_retries == 0 {
            false
        } else {
            self.num_retries -= 1;
            let wait_duration = Duration::from_millis(self.wait_in_ms);
            thread::sleep(wait_duration);
            true
        }

    }
}

impl LockType {

    fn retry_policy(&self) -> RetryPolicy {
        match *self {
            LockType::IndexWriterLock =>
                RetryPolicy::no_retry(),
            LockType::MetaLock =>
                RetryPolicy {
                    num_retries: 100,
                    wait_in_ms: 100,
                }
        }
    }

    fn try_acquire_lock(&self, directory: &mut Directory) -> Result<DirectoryLock, TantivyError> {
        let path = self.filename();
        let mut write = directory
            .open_write(path)
            .map_err(|e|
                match e {
                    OpenWriteError::FileAlreadyExists(_) =>
                        TantivyError::LockFailure(*self),
                    OpenWriteError::IOError(io_error) =>
                        TantivyError::IOError(io_error),
                })?;
        write.flush()?;
        Ok(DirectoryLock {
            directory: directory.box_clone(),
            path: path.to_owned(),
        })
    }


    /// Acquire a lock in the given directory.
    pub fn acquire_lock(&self, directory: &Directory) -> Result<DirectoryLock, TantivyError> {
        let mut box_directory = directory.box_clone();
        let mut retry_policy = self.retry_policy();
        loop {
            let lock_result = self.try_acquire_lock(&mut *box_directory);
            match lock_result {
                Ok(result) => {
                    return Ok(result);
                }
                Err(TantivyError::LockFailure(ref filepath)) => {
                    if !retry_policy.wait_and_retry() {
                        return Err(TantivyError::LockFailure(filepath.to_owned()));
                    }
                }
                Err(_) => {
                }
            }
        }
    }

    fn filename(&self) -> &Path {
        match *self {
            LockType::MetaLock => {
                Path::new(".tantivy-meta.lock")
            }
            LockType::IndexWriterLock => {
                Path::new(".tantivy-indexer.lock")
            }
        }
    }
}


/// The `DirectoryLock` is an object that represents a file lock.
/// See  [`LockType`](struct.LockType.html)
///
/// It is transparently associated to a lock file, that gets deleted
/// on `Drop.` The lock is release automatically on `Drop`.
pub struct DirectoryLock {
    directory: Box<Directory>,
    path: PathBuf,
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        if let Err(e) = self.directory.delete(&*self.path) {
            error!("Failed to remove the lock file. {:?}", e);
        }
    }
}
