use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use crate::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use crate::directory::{
    Directory, DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr, META_LOCK,
};

/// A [`Directory`] wrapper for an immutable index.
///
/// Read operations are forwarded to the underlying directory, while operations that could mutate
/// it are rejected. Requests for [`META_LOCK`] are satisfied without touching the underlying
/// directory, and filesystem watching is disabled.
///
/// The underlying index must not be modified or garbage-collected by any process while this
/// directory is in use. This wrapper is intended for indexes stored on read-only filesystems, not
/// for read-only access to an index that another process may update.
#[derive(Clone)]
pub struct ImmutableDirectory {
    directory: Box<dyn Directory>,
}

impl ImmutableDirectory {
    /// Wraps a directory whose contents will remain unchanged for the lifetime of this wrapper.
    pub fn new(directory: impl Into<Box<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
}

impl fmt::Debug for ImmutableDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ImmutableDirectory({:?})", self.directory)
    }
}

impl Directory for ImmutableDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.directory.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(immutable_error()),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.directory.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            immutable_error(),
            path.to_path_buf(),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.directory.atomic_read(path)
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(immutable_error())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        if lock.filepath == META_LOCK.filepath {
            // Reading index metadata normally requires `META_LOCK`. The caller guarantees that the
            // index cannot change, so a no-op guard provides the same protection without creating a
            // lock file in the underlying directory.
            return Ok(DirectoryLock::from(Box::new(())));
        }
        Err(LockError::wrap_io_error(immutable_error()))
    }

    fn watch(&self, _watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        // An immutable index cannot receive commits, so there are no changes to watch for.
        Ok(WatchHandle::empty())
    }
}

fn immutable_error() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "directory is immutable")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::collector::Count;
    use crate::directory::error::LockError;
    use crate::directory::{
        Directory, ImmutableDirectory, RamDirectory, INDEX_WRITER_LOCK, META_LOCK,
    };
    use crate::query::AllQuery;
    use crate::schema::{Schema, TEXT};
    use crate::{Index, TantivyDocument, TantivyError};

    #[test]
    fn test_immutable_directory_rejects_writes() {
        let directory = RamDirectory::create();
        directory
            .atomic_write(Path::new("file"), b"original")
            .unwrap();
        let immutable_directory = ImmutableDirectory::new(directory);

        assert_eq!(
            immutable_directory.atomic_read(Path::new("file")).unwrap(),
            b"original"
        );
        assert!(immutable_directory
            .open_write(Path::new("new-file"))
            .is_err());
        assert!(immutable_directory
            .atomic_write(Path::new("file"), b"changed")
            .is_err());
        assert!(immutable_directory.delete(Path::new("file")).is_err());
        assert_eq!(
            immutable_directory.atomic_read(Path::new("file")).unwrap(),
            b"original"
        );
    }

    #[test]
    fn test_immutable_directory_uses_noop_meta_lock() {
        let immutable_directory = ImmutableDirectory::new(RamDirectory::create());

        assert!(immutable_directory.acquire_lock(&META_LOCK).is_ok());
        let lock_error = immutable_directory
            .acquire_lock(&INDEX_WRITER_LOCK)
            .err()
            .unwrap();
        let LockError::IoError(io_error) = lock_error else {
            panic!("expected an IO error");
        };
        assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn test_immutable_index_can_search_and_reload() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let directory = RamDirectory::create();
        let index = Index::create(directory.clone(), schema, Default::default()).unwrap();
        let mut writer = index.writer_for_tests::<TantivyDocument>().unwrap();
        writer.add_document(crate::doc!(text => "hello")).unwrap();
        writer.commit().unwrap();
        drop(writer);

        let immutable_index = Index::open_immutable(directory).unwrap();
        let reader = immutable_index.reader().unwrap();
        assert_eq!(reader.searcher().search(&AllQuery, &Count).unwrap(), 1);
        reader.reload().unwrap();
        assert_eq!(reader.searcher().search(&AllQuery, &Count).unwrap(), 1);

        let writer_error = immutable_index
            .writer_for_tests::<TantivyDocument>()
            .err()
            .unwrap();
        let TantivyError::LockFailure(LockError::IoError(io_error), _) = writer_error else {
            panic!("expected a lock failure");
        };
        assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[cfg(all(feature = "mmap", unix))]
    #[test]
    fn test_open_immutable_filesystem_index_without_lock_file() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::path::PathBuf;

        struct RestoreDirectoryPermissions(PathBuf);

        impl Drop for RestoreDirectoryPermissions {
            fn drop(&mut self) {
                let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
            }
        }

        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let temp_directory = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(temp_directory.path(), schema).unwrap();
        let mut writer = index.writer_for_tests::<TantivyDocument>().unwrap();
        writer.add_document(crate::doc!(text => "hello")).unwrap();
        writer.commit().unwrap();
        drop(writer);
        drop(index);

        let meta_lock_path = temp_directory.path().join(&META_LOCK.filepath);
        if meta_lock_path.try_exists().unwrap() {
            fs::remove_file(&meta_lock_path).unwrap();
        }
        fs::set_permissions(temp_directory.path(), fs::Permissions::from_mode(0o555)).unwrap();
        let _restore_permissions = RestoreDirectoryPermissions(temp_directory.path().to_path_buf());

        let immutable_index = Index::open_immutable_in_dir(temp_directory.path()).unwrap();
        let reader = immutable_index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);

        let second_index = Index::open_immutable_in_dir(temp_directory.path()).unwrap();
        let second_reader = second_index.reader().unwrap();
        assert_eq!(second_reader.searcher().num_docs(), 1);
        assert!(!meta_lock_path.try_exists().unwrap());
    }
}
