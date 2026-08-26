use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use crate::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use crate::directory::{
    Directory, DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr, META_LOCK,
};

/// A read-only view over a [`Directory`].
///
/// Read operations are forwarded to the underlying directory, while write operations are rejected.
/// [`META_LOCK`] is not acquired, so the underlying index must remain unchanged while this
/// directory is in use.
#[derive(Clone)]
pub struct ReadOnlyDirectory {
    directory: Box<dyn Directory>,
}

impl ReadOnlyDirectory {
    /// Wraps a directory containing an immutable index.
    pub fn new(directory: impl Into<Box<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
}

impl fmt::Debug for ReadOnlyDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReadOnlyDirectory({:?})", self.directory)
    }
}

impl Directory for ReadOnlyDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.directory.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(read_only_error()),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.directory.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            read_only_error(),
            path.to_path_buf(),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.directory.atomic_read(path)
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(read_only_error())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        if lock.filepath == META_LOCK.filepath {
            return Ok(DirectoryLock::from(Box::new(())));
        }
        Err(LockError::wrap_io_error(read_only_error()))
    }

    fn watch(&self, _watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

fn read_only_error() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "directory is read-only")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::collector::Count;
    use crate::directory::error::LockError;
    use crate::directory::{
        Directory, RamDirectory, ReadOnlyDirectory, INDEX_WRITER_LOCK, META_LOCK,
    };
    use crate::query::AllQuery;
    use crate::schema::{Schema, TEXT};
    use crate::{Index, TantivyDocument, TantivyError};

    #[test]
    fn test_read_only_directory_rejects_writes() {
        let directory = RamDirectory::create();
        directory
            .atomic_write(Path::new("file"), b"original")
            .unwrap();
        let read_only_directory = ReadOnlyDirectory::new(directory);

        assert_eq!(
            read_only_directory.atomic_read(Path::new("file")).unwrap(),
            b"original"
        );
        assert!(read_only_directory
            .open_write(Path::new("new-file"))
            .is_err());
        assert!(read_only_directory
            .atomic_write(Path::new("file"), b"changed")
            .is_err());
        assert!(read_only_directory.delete(Path::new("file")).is_err());
        assert_eq!(
            read_only_directory.atomic_read(Path::new("file")).unwrap(),
            b"original"
        );
    }

    #[test]
    fn test_read_only_directory_only_allows_meta_lock() {
        let read_only_directory = ReadOnlyDirectory::new(RamDirectory::create());

        assert!(read_only_directory.acquire_lock(&META_LOCK).is_ok());
        let lock_error = read_only_directory
            .acquire_lock(&INDEX_WRITER_LOCK)
            .err()
            .unwrap();
        let LockError::IoError(io_error) = lock_error else {
            panic!("expected an IO error");
        };
        assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn test_read_only_index_can_search_and_reload() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let directory = RamDirectory::create();
        let index = Index::create(directory.clone(), schema, Default::default()).unwrap();
        let mut writer = index.writer_for_tests::<TantivyDocument>().unwrap();
        writer.add_document(crate::doc!(text => "hello")).unwrap();
        writer.commit().unwrap();
        drop(writer);

        let read_only_index = Index::open_read_only(directory).unwrap();
        let reader = read_only_index.reader().unwrap();
        assert_eq!(reader.searcher().search(&AllQuery, &Count).unwrap(), 1);
        reader.reload().unwrap();
        assert_eq!(reader.searcher().search(&AllQuery, &Count).unwrap(), 1);

        let writer_error = read_only_index
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
    fn test_open_read_only_filesystem_index_without_lock_file() {
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

        let read_only_index = Index::open_read_only_in_dir(temp_directory.path()).unwrap();
        let reader = read_only_index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);

        let second_index = Index::open_read_only_in_dir(temp_directory.path()).unwrap();
        let second_reader = second_index.reader().unwrap();
        assert_eq!(second_reader.searcher().num_docs(), 1);
        assert!(!meta_lock_path.try_exists().unwrap());
    }
}
