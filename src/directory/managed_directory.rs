use core::MANAGED_FILEPATH;
use directory::error::{DeleteError, IOError, OpenReadError, OpenWriteError};
use directory::{ReadOnlySource, WritePtr};
use error::TantivyError;
use serde_json;
use std::collections::HashSet;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::RwLockWriteGuard;
use std::sync::{Arc, RwLock};
use Directory;
use Result;

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
    directory: Box<Directory>,
    meta_informations: Arc<RwLock<MetaInformation>>,
}

#[derive(Debug, Default)]
struct MetaInformation {
    managed_paths: HashSet<PathBuf>,
}

/// Saves the file containing the list of existing files
/// that were created by tantivy.
fn save_managed_paths(
    directory: &mut Directory,
    wlock: &RwLockWriteGuard<MetaInformation>,
) -> io::Result<()> {
    let mut w = serde_json::to_vec(&wlock.managed_paths)?;
    write!(&mut w, "\n")?;
    directory.atomic_write(&MANAGED_FILEPATH, &w[..])?;
    Ok(())
}

impl ManagedDirectory {
    /// Wraps a directory as managed directory.
    pub fn new<Dir: Directory>(directory: Dir) -> Result<ManagedDirectory> {
        match directory.atomic_read(&MANAGED_FILEPATH) {
            Ok(data) => {
                let managed_files_json = String::from_utf8_lossy(&data);
                let managed_files: HashSet<PathBuf> = serde_json::from_str(&managed_files_json)
                    .map_err(|_| TantivyError::CorruptedFile(MANAGED_FILEPATH.clone()))?;
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
        }
    }

    /// Garbage collect unused files.
    ///
    /// Removes the files that were created by `tantivy` and are not
    /// used by any segment anymore.
    ///
    /// * `living_files` - List of files that are still used by the index.
    ///
    /// This method does not panick nor returns errors.
    /// If a file cannot be deleted (for permission reasons for instance)
    /// an error is simply logged, and the file remains in the list of managed
    /// files.
    pub fn garbage_collect<L: FnOnce() -> HashSet<PathBuf>>(&mut self, get_living_files: L) {
        info!("Garbage collect");
        let mut files_to_delete = vec![];
        {
            // releasing the lock as .delete() will use it too.
            let meta_informations_rlock = self.meta_informations
                .read()
                .expect("Managed directory rlock poisoned in garbage collect.");

            // It is crucial to get the living files after acquiring the
            // read lock of meta informations. That way, we
            // avoid the following scenario.
            //
            // 1) we get the list of living files.
            // 2) someone creates a new file.
            // 3) we start garbage collection and remove this file
            // even though it is a living file.
            let living_files = get_living_files();

            for managed_path in &meta_informations_rlock.managed_paths {
                if !living_files.contains(managed_path) {
                    files_to_delete.push(managed_path.clone());
                }
            }
        }

        let mut deleted_files = vec![];
        {
            for file_to_delete in files_to_delete {
                match self.delete(&file_to_delete) {
                    Ok(_) => {
                        info!("Deleted {:?}", file_to_delete);
                        deleted_files.push(file_to_delete);
                    }
                    Err(file_error) => {
                        match file_error {
                            DeleteError::FileDoesNotExist(_) => {
                                deleted_files.push(file_to_delete);
                            }
                            DeleteError::IOError(_) => {
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
        }

        if !deleted_files.is_empty() {
            // update the list of managed files by removing
            // the file that were removed.
            let mut meta_informations_wlock = self.meta_informations
                .write()
                .expect("Managed directory wlock poisoned (2).");
            {
                let managed_paths_write = &mut meta_informations_wlock.managed_paths;
                for delete_file in &deleted_files {
                    managed_paths_write.remove(delete_file);
                }
            }
            if save_managed_paths(self.directory.as_mut(), &meta_informations_wlock).is_err() {
                error!("Failed to save the list of managed files.");
            }
        }
    }

    /// Registers a file as managed
    ///
    /// This method must be called before the file is
    /// actually created to ensure that a failure between
    /// registering the filepath and creating the file
    /// will not lead to garbage files that will
    /// never get removed.
    fn register_file_as_managed(&mut self, filepath: &Path) -> io::Result<()> {
        let mut meta_wlock = self.meta_informations
            .write()
            .expect("Managed file lock poisoned");
        let has_changed = meta_wlock.managed_paths.insert(filepath.to_owned());
        if has_changed {
            save_managed_paths(self.directory.as_mut(), &meta_wlock)?;
        }
        Ok(())
    }
}

impl Directory for ManagedDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        self.directory.open_read(path)
    }

    fn open_write(&mut self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        self.register_file_as_managed(path)
            .map_err(|e| IOError::with_path(path.to_owned(), e))?;
        self.directory.open_write(path)
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
}

impl Clone for ManagedDirectory {
    fn clone(&self) -> ManagedDirectory {
        ManagedDirectory {
            directory: self.directory.box_clone(),
            meta_informations: Arc::clone(&self.meta_informations),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[cfg(feature = "mmap")]
    use directory::MmapDirectory;
    use std::io::Write;
    use std::path::Path;
    use tempdir::TempDir;

    lazy_static! {
        static ref TEST_PATH1: &'static Path = Path::new("some_path_for_test");
        static ref TEST_PATH2: &'static Path = Path::new("some_path_for_test2");
    }

    #[test]
    #[cfg(feature = "mmap")]
    fn test_managed_directory() {
        let tempdir = TempDir::new("index").unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        {
            let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
            let mut managed_directory = ManagedDirectory::new(mmap_directory).unwrap();
            {
                let mut write_file = managed_directory.open_write(*TEST_PATH1).unwrap();
                write_file.flush().unwrap();
            }
            {
                managed_directory
                    .atomic_write(*TEST_PATH2, &vec![0u8, 1u8])
                    .unwrap();
            }
            {
                assert!(managed_directory.exists(*TEST_PATH1));
                assert!(managed_directory.exists(*TEST_PATH2));
            }
            {
                let living_files: HashSet<PathBuf> =
                    [TEST_PATH1.to_owned()].into_iter().cloned().collect();
                managed_directory.garbage_collect(|| living_files);
            }
            {
                assert!(managed_directory.exists(*TEST_PATH1));
                assert!(!managed_directory.exists(*TEST_PATH2));
            }
        }
        {
            let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
            let mut managed_directory = ManagedDirectory::new(mmap_directory).unwrap();
            {
                assert!(managed_directory.exists(*TEST_PATH1));
                assert!(!managed_directory.exists(*TEST_PATH2));
            }
            {
                let living_files: HashSet<PathBuf> = HashSet::new();
                managed_directory.garbage_collect(|| living_files);
            }
            {
                assert!(!managed_directory.exists(*TEST_PATH1));
                assert!(!managed_directory.exists(*TEST_PATH2));
            }
        }
    }

    #[test]
    #[cfg(feature = "mmap ")]
    fn test_managed_directory_gc_while_mmapped() {
        let tempdir = TempDir::new("index").unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        let living_files = HashSet::new();

        let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
        let mut managed_directory = ManagedDirectory::new(mmap_directory).unwrap();
        managed_directory
            .atomic_write(*TEST_PATH1, &vec![0u8, 1u8])
            .unwrap();
        assert!(managed_directory.exists(*TEST_PATH1));

        let _mmap_read = managed_directory.open_read(*TEST_PATH1).unwrap();
        managed_directory.garbage_collect(|| living_files.clone());
        if cfg!(target_os = "windows") {
            // On Windows, gc should try and fail the file as it is mmapped.
            assert!(managed_directory.exists(*TEST_PATH1));
            // unmap should happen here.
            drop(_mmap_read);
            // The file should still be in the list of managed file and
            // eventually be deleted once mmap is released.
            managed_directory.garbage_collect(|| living_files);
            assert!(!managed_directory.exists(*TEST_PATH1));
        } else {
            assert!(!managed_directory.exists(*TEST_PATH1));
        }
    }

}
