use crate::directory::directory_lock::Lock;
use crate::directory::error::LockError;
use crate::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use crate::directory::WatchHandle;
use crate::directory::{FileHandle, WatchCallback};
use crate::directory::{FileSlice, WritePtr};
use std::fmt;
use std::io;
use std::io::Write;
use std::marker::Send;
use std::marker::Sync;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

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

    fn wait_and_retry(&mut self) -> bool {
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

/// The `DirectoryLock` is an object that represents a file lock.
/// See  [`LockType`](struct.LockType.html)
///
/// It is transparently associated to a lock file, that gets deleted
/// on `Drop.` The lock is released automatically on `Drop`.
pub struct DirectoryLock(Box<dyn Send + Sync + 'static>);

struct DirectoryLockGuard {
    directory: Box<dyn Directory>,
    path: PathBuf,
}

impl<T: Send + Sync + 'static> From<Box<T>> for DirectoryLock {
    fn from(underlying: Box<T>) -> Self {
        DirectoryLock(underlying)
    }
}

impl Drop for DirectoryLockGuard {
    fn drop(&mut self) {
        if let Err(e) = self.directory.delete(&*self.path) {
            error!("Failed to remove the lock file. {:?}", e);
        }
    }
}

enum TryAcquireLockError {
    FileExists,
    IOError(io::Error),
}

fn try_acquire_lock(
    filepath: &Path,
    directory: &mut dyn Directory,
) -> Result<DirectoryLock, TryAcquireLockError> {
    let mut write = directory.open_write(filepath).map_err(|e| match e {
        OpenWriteError::FileAlreadyExists(_) => TryAcquireLockError::FileExists,
        OpenWriteError::IOError { io_error, .. } => TryAcquireLockError::IOError(io_error),
    })?;
    write.flush().map_err(TryAcquireLockError::IOError)?;
    Ok(DirectoryLock::from(Box::new(DirectoryLockGuard {
        directory: directory.box_clone(),
        path: filepath.to_owned(),
    })))
}

fn retry_policy(is_blocking: bool) -> RetryPolicy {
    if is_blocking {
        RetryPolicy {
            num_retries: 100,
            wait_in_ms: 100,
        }
    } else {
        RetryPolicy::no_retry()
    }
}

/// Write-once read many (WORM) abstraction for where
/// tantivy's data should be stored.
///
/// There are currently two implementations of `Directory`
///
/// - The [`MMapDirectory`](struct.MmapDirectory.html), this
/// should be your default choice.
/// - The [`RAMDirectory`](struct.RAMDirectory.html), which
/// should be used mostly for tests.
pub trait Directory: DirectoryClone + fmt::Debug + Send + Sync + 'static {
    /// Opens a file and returns a boxed `FileHandle`.
    ///
    /// Users of `Directory` should typically call `Directory::open_read(...)`,
    /// while `Directory` implementor should implement `get_file_handle()`.
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError>;

    /// Once a virtual file is open, its data may not
    /// change.
    ///
    /// Specifically, subsequent writes or flushes should
    /// have no effect on the returned `FileSlice` object.
    ///
    /// You should only use this to read files create with [Directory::open_write].
    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        Ok(FileSlice::new(file_handle))
    }

    /// Removes a file
    ///
    /// Removing a file will not affect an eventual
    /// existing FileSlice pointing to it.
    ///
    /// Removing a nonexistent file, yields a
    /// `DeleteError::DoesNotExist`.
    fn delete(&self, path: &Path) -> Result<(), DeleteError>;

    /// Returns true iff the file exists
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError>;

    /// Opens a writer for the *virtual file* associated with
    /// a Path.
    ///
    /// Right after this call, the file should be created
    /// and any subsequent call to `open_read` for the
    /// same path should return a `FileSlice`.
    ///
    /// Write operations may be aggressively buffered.
    /// The client of this trait is responsible for calling flush
    /// to ensure that subsequent `read` operations
    /// will take into account preceding `write` operations.
    ///
    /// Flush operation should also be persistent.
    ///
    /// The user shall not rely on `Drop` triggering `flush`.
    /// Note that `RAMDirectory` will panic! if `flush`
    /// was not called.
    ///
    /// The file may not previously exist.
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError>;

    /// Reads the full content file that has been written using
    /// atomic_write.
    ///
    /// This should only be used for small files.
    ///
    /// You should only use this to read files create with [Directory::atomic_write].
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError>;

    /// Atomically replace the content of a file with data.
    ///
    /// This calls ensure that reads can never *observe*
    /// a partially written file.
    ///
    /// The file may or may not previously exist.
    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()>;

    /// Acquire a lock in the given directory.
    ///
    /// The method is blocking or not depending on the `Lock` object.
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let mut box_directory = self.box_clone();
        let mut retry_policy = retry_policy(lock.is_blocking);
        loop {
            match try_acquire_lock(&lock.filepath, &mut *box_directory) {
                Ok(result) => {
                    return Ok(result);
                }
                Err(TryAcquireLockError::FileExists) => {
                    if !retry_policy.wait_and_retry() {
                        return Err(LockError::LockBusy);
                    }
                }
                Err(TryAcquireLockError::IOError(io_error)) => {
                    return Err(LockError::IOError(io_error));
                }
            }
        }
    }

    /// Registers a callback that will be called whenever a change on the `meta.json`
    /// using the `atomic_write` API is detected.
    ///
    /// The behavior when using `.watch()` on a file using [Directory::open_write] is, on the other
    /// hand, undefined.
    ///
    /// The file will be watched for the lifetime of the returned `WatchHandle`. The caller is
    /// required to keep it.
    /// It does not override previous callbacks. When the file is modified, all callback that are
    /// registered (and whose `WatchHandle` is still alive) are triggered.
    ///
    /// Internally, tantivy only uses this API to detect new commits to implement the
    /// `OnCommit` `ReloadPolicy`. Not implementing watch in a `Directory` only prevents the
    /// `OnCommit` `ReloadPolicy` to work properly.
    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle>;
}

/// DirectoryClone
pub trait DirectoryClone {
    /// Clones the directory and boxes the clone
    fn box_clone(&self) -> Box<dyn Directory>;
}

impl<T> DirectoryClone for T
where
    T: 'static + Directory + Clone,
{
    fn box_clone(&self) -> Box<dyn Directory> {
        Box::new(self.clone())
    }
}
