use std::marker::Send;
use std::marker::Sync;
use std::fmt;
use std::path::Path;
use directory::{FileError, OpenWriteError};
use directory::{ReadOnlySource, WritePtr};
use std::result;
use std::io;

/// Write-once read many (WORM) abstraction for where tantivy's index should be stored. 
///
/// There is currently two implementations of `Directory`
/// 
/// - The [MMapDirectory](struct.MmapDirectory.html), this
/// should be your default choice. 
/// - The [RAMDirectory](struct.RAMDirectory.html), which 
/// should be used mostly for tests.
/// 
pub trait Directory: fmt::Debug + Send + Sync {

    /// Opens a virtual file for read.
    /// 
    /// Once a virtualfile is open, its data may not
    /// change.
    ///
    /// Specifically, subsequent write or flush should
    /// have no effect the returned `ReadOnlySource` object. 
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, FileError>;
    
    /// Removes a file
    ///
    /// Removing a file will not affect eventual
    /// existing ReadOnlySource pointing to it.
    /// 
    /// Removing a non existing files, yields a
    /// `FileError::DoesNotExist`.
    fn delete(&self, path: &Path) -> result::Result<(), FileError>;

    /// Opens a writer for the *virtual file* associated with 
    /// a Path.
    ///
    /// Right after this call, the file should be created
    /// and any subsequent call to `open_read` for the 
    /// same path should return a `ReadOnlySource`.
    /// 
    /// Write operations may be aggressively buffered.
    /// The client of this trait is in charge to call flush
    /// to ensure that subsequent `read` operations 
    /// will take in account preceding `write` operations.
    /// 
    /// Flush operation should also be persistent.
    ///
    /// User shall not rely on `Drop` triggering `flush`.
    /// Note that `RAMDirectory` will panic! if `flush`
    /// was not called.
    ///
    /// The file may not previously exists.
    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError>;
    
    /// Atomically replace the content of a file by data.
    /// 
    /// This calls ensure that reads can never *observe*
    /// a partially written file.
    /// 
    /// The file may or may not previously exists.
    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()>;
}
