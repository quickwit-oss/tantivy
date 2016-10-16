use Directory;
use std::path::Path;
use directory::error::OpenWriteError;

pub const LOCKFILE_NAME: &'static str = ".tantivy-indexer.lock";


/// The directory lock is a mechanism used to
/// prevent the creation of two [`IndexWriter`](struct.IndexWriter.html)
///
/// Only one lock can exist at a time for a given directory.
/// The lock is release automatically on `Drop`.
pub struct DirectoryLock {
    directory: Box<Directory>,
}

impl DirectoryLock {
    pub fn lock(mut directory: Box<Directory>) -> Result<DirectoryLock, OpenWriteError> {
        let lockfile_path = Path::new(LOCKFILE_NAME);
        try!(directory.open_write(lockfile_path));
        Ok(DirectoryLock { directory: directory })
    }
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        let lockfile_path = Path::new(LOCKFILE_NAME);
        if let Err(e) = self.directory.delete(lockfile_path) {
            error!("Failed to remove the lock file. {:?}", e);
        }
    }
}