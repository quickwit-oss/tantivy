use crate::core::MANAGED_FILEPATH;
use crate::directory::error::{DeleteError, IOError, LockError, OpenReadError, OpenWriteError};
use crate::directory::footer::{Footer, FooterProxy};
use crate::directory::DirectoryLock;
use crate::directory::GarbageCollectionResult;
use crate::directory::Lock;
use crate::directory::META_LOCK;
use crate::directory::{ReadOnlySource, WritePtr};
use crate::directory::{WatchCallback, WatchHandle};
use crate::error::DataCorruption;
use crate::Directory;

use crc32fast::Hasher;
use serde_json;
use std::collections::HashSet;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::RwLockWriteGuard;
use std::sync::{Arc, RwLock};

/// Returns true iff the file is "managed".
/// Non-managed file are not subject to garbage collection.
///
/// Filenames that starts by a "." -typically locks-
/// are not managed.
fn is_managed(path: &Path) -> bool {
    path.to_str()
        .map(|p_str| !p_str.starts_with('.'))
        .unwrap_or(true)
}

/// Wrapper of directories that keeps track of files created by Tantivy.
///
/// A managed directory is just a wrapper of a directory
/// that keeps a (persisted) list of the files that
/// have been created (and not deleted) by tantivy so far.
///
/// Thanks to this list, it implements a `garbage_collect` method
/// that removes the files that were created by tantivy and are not
/// useful anymore.
#[derive(Debug)]
pub struct ManagedDirectory {
    directory: Box<dyn Directory>,
    meta_informations: Arc<RwLock<MetaInformation>>,
}

#[derive(Debug, Default)]
struct MetaInformation {
    managed_paths: HashSet<PathBuf>,
}

/// Saves the file containing the list of existing files
/// that were created by tantivy.
fn save_managed_paths(
    directory: &mut dyn Directory,
    wlock: &RwLockWriteGuard<'_, MetaInformation>,
) -> io::Result<()> {
    let mut w = serde_json::to_vec(&wlock.managed_paths)?;
    writeln!(&mut w)?;
    directory.atomic_write(&MANAGED_FILEPATH, &w[..])?;
    Ok(())
}

impl ManagedDirectory {
    /// Wraps a directory as managed directory.
    pub fn wrap<Dir: Directory>(directory: Dir) -> crate::Result<ManagedDirectory> {
        match directory.atomic_read(&MANAGED_FILEPATH) {
            Ok(data) => {
                let managed_files_json = String::from_utf8_lossy(&data);
                let managed_files: HashSet<PathBuf> = serde_json::from_str(&managed_files_json)
                    .map_err(|e| {
                        DataCorruption::new(
                            MANAGED_FILEPATH.to_path_buf(),
                            format!("Managed file cannot be deserialized: {:?}. ", e),
                        )
                    })?;
                Ok(ManagedDirectory {
                    directory: Box::new(directory),
                    meta_informations: Arc::new(RwLock::new(MetaInformation {
                        managed_paths: managed_files,
                    })),
                })
            }
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(ManagedDirectory {
                directory: Box::new(directory),
                meta_informations: Arc::default(),
            }),
            Err(OpenReadError::IOError(e)) => Err(From::from(e)),
            Err(OpenReadError::IncompatibleIndex(incompatibility)) => {
                // For the moment, this should never happen  `meta.json`
                // do not have any footer and cannot detect incompatibility.
                Err(crate::TantivyError::IncompatibleIndex(incompatibility))
            }
        }
    }

    /// Garbage collect unused files.
    ///
    /// Removes the files that were created by `tantivy` and are not
    /// used by any segment anymore.
    ///
    /// * `living_files` - List of files that are still used by the index.
    ///
    /// The use a callback ensures that the list of living_files is computed
    /// while we hold the lock on meta.
    ///
    /// This method does not panick nor returns errors.
    /// If a file cannot be deleted (for permission reasons for instance)
    /// an error is simply logged, and the file remains in the list of managed
    /// files.
    pub fn garbage_collect<L: FnOnce() -> HashSet<PathBuf>>(
        &mut self,
        get_living_files: L,
    ) -> crate::Result<GarbageCollectionResult> {
        info!("Garbage collect");
        let mut files_to_delete = vec![];

        // It is crucial to get the living files after acquiring the
        // read lock of meta informations. That way, we
        // avoid the following scenario.
        //
        // 1) we get the list of living files.
        // 2) someone creates a new file.
        // 3) we start garbage collection and remove this file
        // even though it is a living file.
        //
        // releasing the lock as .delete() will use it too.
        {
            let meta_informations_rlock = self
                .meta_informations
                .read()
                .expect("Managed directory rlock poisoned in garbage collect.");

            // The point of this second "file" lock is to enforce the following scenario
            // 1) process B tries to load a new set of searcher.
            // The list of segments is loaded
            // 2) writer change meta.json (for instance after a merge or a commit)
            // 3) gc kicks in.
            // 4) gc removes a file that was useful for process B, before process B opened it.
            match self.acquire_lock(&META_LOCK) {
                Ok(_meta_lock) => {
                    let living_files = get_living_files();
                    for managed_path in &meta_informations_rlock.managed_paths {
                        if !living_files.contains(managed_path) {
                            files_to_delete.push(managed_path.clone());
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to acquire lock for GC");
                    return Err(crate::Error::from(err));
                }
            }
        }

        let mut failed_to_delete_files = vec![];
        let mut deleted_files = vec![];

        for file_to_delete in files_to_delete {
            match self.delete(&file_to_delete) {
                Ok(_) => {
                    info!("Deleted {:?}", file_to_delete);
                    deleted_files.push(file_to_delete);
                }
                Err(file_error) => {
                    match file_error {
                        DeleteError::FileDoesNotExist(_) => {
                            deleted_files.push(file_to_delete.clone());
                        }
                        DeleteError::IOError(_) => {
                            failed_to_delete_files.push(file_to_delete.clone());
                            if !cfg!(target_os = "windows") {
                                // On windows, delete is expected to fail if the file
                                // is mmapped.
                                error!("Failed to delete {:?}", file_to_delete);
                            }
                        }
                    }
                }
            }
        }

        if !deleted_files.is_empty() {
            // update the list of managed files by removing
            // the file that were removed.
            let mut meta_informations_wlock = self
                .meta_informations
                .write()
                .expect("Managed directory wlock poisoned (2).");
            let managed_paths_write = &mut meta_informations_wlock.managed_paths;
            for delete_file in &deleted_files {
                managed_paths_write.remove(delete_file);
            }
            save_managed_paths(self.directory.as_mut(), &meta_informations_wlock)?;
        }

        Ok(GarbageCollectionResult {
            deleted_files,
            failed_to_delete_files,
        })
    }

    /// Registers a file as managed
    ///
    /// This method must be called before the file is
    /// actually created to ensure that a failure between
    /// registering the filepath and creating the file
    /// will not lead to garbage files that will
    /// never get removed.
    ///
    /// File starting by "." are reserved to locks.
    /// They are not managed and cannot be subjected
    /// to garbage collection.
    fn register_file_as_managed(&mut self, filepath: &Path) -> io::Result<()> {
        // Files starting by "." (e.g. lock files) are not managed.
        if !is_managed(filepath) {
            return Ok(());
        }
        let mut meta_wlock = self
            .meta_informations
            .write()
            .expect("Managed file lock poisoned");
        let has_changed = meta_wlock.managed_paths.insert(filepath.to_owned());
        if has_changed {
            save_managed_paths(self.directory.as_mut(), &meta_wlock)?;
        }
        Ok(())
    }

    /// Verify checksum of a managed file
    pub fn validate_checksum(&self, path: &Path) -> result::Result<bool, OpenReadError> {
        let reader = self.directory.open_read(path)?;
        let (footer, data) = Footer::extract_footer(reader)
            .map_err(|err| IOError::with_path(path.to_path_buf(), err))?;
        let mut hasher = Hasher::new();
        hasher.update(data.as_slice());
        let crc = hasher.finalize();
        Ok(footer
            .versioned_footer
            .crc()
            .map(|v| v == crc)
            .unwrap_or(false))
    }

    /// List files for which checksum does not match content
    pub fn list_damaged(&self) -> result::Result<HashSet<PathBuf>, OpenReadError> {
        let mut hashset = HashSet::new();
        let managed_paths = self
            .meta_informations
            .read()
            .expect("Managed directory rlock poisoned in list damaged.")
            .managed_paths
            .clone();

        for path in managed_paths.into_iter() {
            if !self.validate_checksum(&path)? {
                hashset.insert(path);
            }
        }
        Ok(hashset)
    }
}

impl Directory for ManagedDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        let read_only_source = self.directory.open_read(path)?;
        let (footer, reader) = Footer::extract_footer(read_only_source)
            .map_err(|err| IOError::with_path(path.to_path_buf(), err))?;
        footer.is_compatible()?;
        Ok(reader)
    }

    fn open_write(&mut self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        self.register_file_as_managed(path)
            .map_err(|e| IOError::with_path(path.to_owned(), e))?;
        Ok(io::BufWriter::new(Box::new(FooterProxy::new(
            self.directory
                .open_write(path)?
                .into_inner()
                .map_err(|_| ())
                .expect("buffer should be empty"),
        ))))
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.register_file_as_managed(path)?;
        self.directory.atomic_write(path, data)
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        self.directory.atomic_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.directory.delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.directory.exists(path)
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        self.directory.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        self.directory.watch(watch_callback)
    }
}

impl Clone for ManagedDirectory {
    fn clone(&self) -> ManagedDirectory {
        ManagedDirectory {
            directory: self.directory.box_clone(),
            meta_informations: Arc::clone(&self.meta_informations),
        }
    }
}

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap_specific {

    use crate::directory::{Directory, ManagedDirectory, MmapDirectory, TerminatingWrite};
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[test]
    fn test_managed_directory() {
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());

        let test_path1: &'static Path = Path::new("some_path_for_test");
        let test_path2: &'static Path = Path::new("some_path_for_test_2");
        {
            let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
            let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
            let write_file = managed_directory.open_write(test_path1).unwrap();
            write_file.terminate().unwrap();
            managed_directory
                .atomic_write(test_path2, &[0u8, 1u8])
                .unwrap();
            assert!(managed_directory.exists(test_path1));
            assert!(managed_directory.exists(test_path2));
            let living_files: HashSet<PathBuf> = [test_path1.to_owned()].iter().cloned().collect();
            assert!(managed_directory.garbage_collect(|| living_files).is_ok());
            assert!(managed_directory.exists(test_path1));
            assert!(!managed_directory.exists(test_path2));
        }
        {
            let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
            let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
            assert!(managed_directory.exists(test_path1));
            assert!(!managed_directory.exists(test_path2));
            let living_files: HashSet<PathBuf> = HashSet::new();
            assert!(managed_directory.garbage_collect(|| living_files).is_ok());
            assert!(!managed_directory.exists(test_path1));
            assert!(!managed_directory.exists(test_path2));
        }
    }

    #[test]
    fn test_managed_directory_gc_while_mmapped() {
        let test_path1: &'static Path = Path::new("some_path_for_test");

        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        let living_files = HashSet::new();

        let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
        let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
        let mut write = managed_directory.open_write(test_path1).unwrap();
        write.write_all(&[0u8, 1u8]).unwrap();
        write.terminate().unwrap();
        assert!(managed_directory.exists(test_path1));

        let _mmap_read = managed_directory.open_read(test_path1).unwrap();
        assert!(managed_directory
            .garbage_collect(|| living_files.clone())
            .is_ok());
        if cfg!(target_os = "windows") {
            // On Windows, gc should try and fail the file as it is mmapped.
            assert!(managed_directory.exists(test_path1));
            // unmap should happen here.
            drop(_mmap_read);
            // The file should still be in the list of managed file and
            // eventually be deleted once mmap is released.
            assert!(managed_directory.garbage_collect(|| living_files).is_ok());
            assert!(!managed_directory.exists(test_path1));
        } else {
            assert!(!managed_directory.exists(test_path1));
        }
    }

    #[test]
    fn test_checksum() {
        let test_path1: &'static Path = Path::new("some_path_for_test");
        let test_path2: &'static Path = Path::new("other_test_path");

        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());

        let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
        let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
        let mut write = managed_directory.open_write(test_path1).unwrap();
        write.write_all(&[0u8, 1u8]).unwrap();
        write.terminate().unwrap();

        let mut write = managed_directory.open_write(test_path2).unwrap();
        write.write_all(&[3u8, 4u8, 5u8]).unwrap();
        write.terminate().unwrap();

        let read_source = managed_directory.open_read(test_path2).unwrap();
        assert_eq!(read_source.as_slice(), &[3u8, 4u8, 5u8]);
        assert!(managed_directory.list_damaged().unwrap().is_empty());

        let mut corrupted_path = tempdir_path.clone();
        corrupted_path.push(test_path2);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&corrupted_path)
            .unwrap();
        file.write_all(&[255u8]).unwrap();
        file.flush().unwrap();
        drop(file);

        let damaged = managed_directory.list_damaged().unwrap();
        assert_eq!(damaged.len(), 1);
        assert!(damaged.contains(test_path2));
    }
}
