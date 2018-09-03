use directory::error::{DeleteError, OpenReadError, OpenWriteError};
use directory::{ReadOnlySource, WritePtr};
use std::fmt;
use std::io;
use std::marker::Send;
use std::marker::Sync;
use std::path::Path;
use std::result;

/// Write-once read many (WORM) abstraction for where
/// tantivy's data should be stored.
///
/// There are currently two implementations of `Directory`
///
/// - The [`MMapDirectory`](struct.MmapDirectory.html), this
/// should be your default choice.
/// - The [`RAMDirectory`](struct.RAMDirectory.html), which
/// should be used mostly for tests.
///
pub trait Directory: DirectoryClone + fmt::Debug + Send + Sync + 'static {
    /// Opens a virtual file for read.
    ///
    /// Once a virtual file is open, its data may not
    /// change.
    ///
    /// Specifically, subsequent writes or flushes should
    /// have no effect on the returned `ReadOnlySource` object.
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError>;

    /// Removes a file
    ///
    /// Removing a file will not affect an eventual
    /// existing ReadOnlySource pointing to it.
    ///
    /// Removing a nonexistent file, yields a
    /// `DeleteError::DoesNotExist`.
    fn delete(&self, path: &Path) -> result::Result<(), DeleteError>;

    /// Returns true iff the file exists
    fn exists(&self, path: &Path) -> bool;

    /// Opens a writer for the *virtual file* associated with
    /// a Path.
    ///
    /// Right after this call, the file should be created
    /// and any subsequent call to `open_read` for the
    /// same path should return a `ReadOnlySource`.
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
    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError>;

    /// Reads the full content file that has been written using
    /// atomic_write.
    ///
    /// This should only be used for small files.
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError>;

    /// Atomically replace the content of a file with data.
    ///
    /// This calls ensure that reads can never *observe*
    /// a partially written file.
    ///
    /// The file may or may not previously exist.
    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()>;
}

/// DirectoryClone
pub trait DirectoryClone {
  /// Clones the directory and boxes the clone
  fn box_clone(&self) -> Box<Directory>;
}

impl<T> DirectoryClone for T
where
  T: 'static + Directory + Clone,
{
  fn box_clone(&self) -> Box<Directory> {
    Box::new(self.clone())
  }
}
