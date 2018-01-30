use Directory;
use directory::error::OpenWriteError;
use core::LOCKFILE_FILEPATH;

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
        directory.open_write(&*LOCKFILE_FILEPATH)?;
        Ok(DirectoryLock { directory })
    }
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        if let Err(e) = self.directory.delete(&*LOCKFILE_FILEPATH) {
            error!("Failed to remove the lock file. {:?}", e);
        }
    }
}

