use directory::error::OpenWriteError;
use Directory;
use error;
use std::path::Path;
use TantivyError;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
pub(crate) enum LockType {
    // Ensure that there is never two process holding an `IndexWriter` at the same time.
    IndexWriterLock,
    MetaLock
}


impl LockType {
    pub fn acquire_lock(&self, directory: &Directory) -> Result<DirectoryLock, TantivyError> {
        let mut box_directory = directory.box_clone();
        let path = self.filename();
        box_directory.open_write(path)?;
        Ok(DirectoryLock {
            directory: box_directory,
            path: path.to_owned()
        })
    }

    fn filename(&self) -> &Path {
        match *self {
            LockType::IndexWriterLock => {
                Path::new(".tantivy-meta.lock")
            }
            LockType::MetaLock => {
                /// Only one process should be able to write tantivy's index at a time.
                /// This file, when present, is in charge of preventing other processes to open an IndexWriter.
                ///
                /// If the process is killed and this file remains, it is safe to remove it manually.
                Path::new(".tantivy-indexer.lock")
            }
        }
    }
}


/// The directory lock is a mechanism used to
/// prevent the creation of two [`IndexWriter`](struct.IndexWriter.html)
///
/// Only one lock can exist at a time for a given directory.
/// The lock is release automatically on `Drop`.
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
