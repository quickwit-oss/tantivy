use std::path::{Path, PathBuf};
use directory::error::{FileError, OpenWriteError};
use directory::{ReadOnlySource, WritePtr};
use std::result;
use std::io;
use Directory;
use std::sync::{Arc, RwLock};
use std::collections::HashSet;
use std::io::Write;
use rustc_serialize::json;
use core::MANAGED_FILEPATH;
use Result;
use Error;

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
    managed_paths: Arc<RwLock<HashSet<PathBuf>>>,
}

impl ManagedDirectory {

    /// Wraps a directory as managed directory.
    pub fn new<Dir: Directory>(directory: Dir) -> Result<ManagedDirectory> {
        match directory.atomic_read(&MANAGED_FILEPATH) {
            Ok(data) => {
                let managed_files_json = String::from_utf8_lossy(&data);
                let managed_files: HashSet<PathBuf> = json::decode(&managed_files_json)
                    .map_err(|e| Error::CorruptedFile(MANAGED_FILEPATH.clone(), Box::new(e)))?;
                Ok(ManagedDirectory {
                    directory: box directory,
                    managed_paths: Arc::new(RwLock::new(managed_files)),
                })
            }
            Err(FileError::FileDoesNotExist(_)) => {
                Ok(ManagedDirectory {
                    directory: box directory,
                    managed_paths: Arc::default(),
                })
            }
            Err(FileError::IOError(e)) => {
                Err(From::from(e))
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
    /// This method does not panick nor returns errors.
    /// If a file cannot be deleted (for permission reasons for instance)
    /// an error is simply logged, and the file remains in the list of managed
    /// files.
    pub fn garbage_collect(&mut self, living_files: HashSet<PathBuf>) {
        let mut managed_has_changed: bool = false;
        {
            let mut files_to_delete = vec!();
            let mut managed_paths_write = self.managed_paths.write().unwrap();
            for managed_path in managed_paths_write.iter() {
                if !living_files.contains(managed_path) {
                    files_to_delete.push(managed_path.clone());
                }
            }
            for file_to_delete in files_to_delete {
                match self.directory.delete(&file_to_delete) {
                    Ok(_) => {
                        info!("Deleted {:?}", file_to_delete);
                        managed_has_changed |= managed_paths_write.remove(&file_to_delete);
                    }
                    Err(file_error) => {
                        match file_error {
                            FileError::FileDoesNotExist(_) => {
                                managed_has_changed |= managed_paths_write.remove(&file_to_delete);
                            }
                            FileError::IOError(_) => {
                                error!("Failed to delete {:?}", file_to_delete);
                            }
                            
                        }
                        
                    }
                }
            }
        }
        if managed_has_changed {
            if let Err(_) = self.save_managed_paths() {
                error!("Failed to save the list of managed files.");
            }
        }
    }

    /// Saves the file containing the list of existing files
    /// that were created by tantivy.
    fn save_managed_paths(&mut self,) -> io::Result<()> {
        let managed_files_lock = self.managed_paths
            .read()
            .expect("Managed file lock poisoned");
        let mut w = vec!();
        try!(write!(&mut w, "{}\n", json::as_pretty_json(&*managed_files_lock)));
        self.directory.atomic_write(&MANAGED_FILEPATH, &w[..])?;
        Ok(())
    }

    /// Registers a file as managed
    /// 
    /// This method must be called before the file is 
    /// actually created to ensure that a failure between
    /// registering the filepath and creating the file
    /// will not lead to garbage files that will 
    /// never get removed.
    fn register_file_as_managed(&mut self, filepath: &Path) -> io::Result<()> {
        let has_changed = {
            let mut managed_files_lock = self
                .managed_paths
                .write()
                .expect("Managed file lock poisoned");
            managed_files_lock.insert(filepath.to_owned())
        };
        if has_changed {
            self.save_managed_paths()?;
        }
        Ok(())
    }
}

impl Directory for ManagedDirectory {
    
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, FileError> {
        self.directory.open_read(path)
    }

    fn open_write(&mut self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        self.register_file_as_managed(path)?;
        self.directory.open_write(path)
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.register_file_as_managed(path)?;
        self.directory.atomic_write(path, data)
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, FileError> {
        self.directory.atomic_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), FileError> {
        self.directory.delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.directory.exists(path)
    }
    
    fn box_clone(&self) -> Box<Directory> {
        box self.clone()
    }

}

impl Clone for ManagedDirectory {
    fn clone(&self) -> ManagedDirectory {
        ManagedDirectory {
            directory: self.directory.box_clone(),
            managed_paths: self.managed_paths.clone(),   
        }
    }
}




#[cfg(test)]
mod tests {

    use super::*;
    use directory::MmapDirectory;
    use std::path::Path;   
    use std::io::Write;
    use tempdir::TempDir;
    
    lazy_static! {
        static ref TEST_PATH1: &'static Path = Path::new("some_path_for_test");
        static ref TEST_PATH2: &'static Path = Path::new("some_path_for_test2");
    }

    #[test]
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
                managed_directory.atomic_write(*TEST_PATH2, &vec!(0u8,1u8)).unwrap();
            }
            {
                assert!(managed_directory.exists(*TEST_PATH1));
                assert!(managed_directory.exists(*TEST_PATH2));
            }
            {
                let living_files: HashSet<PathBuf> = [TEST_PATH1.to_owned()]
                    .into_iter()
                    .cloned()
                    .collect();
                managed_directory.garbage_collect(living_files);
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
                managed_directory.garbage_collect(living_files);
            }
            {
                assert!(!managed_directory.exists(*TEST_PATH1));
                assert!(!managed_directory.exists(*TEST_PATH2));
            }
        }   
    }

}
