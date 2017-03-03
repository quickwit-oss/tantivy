use Result;
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



#[derive(Debug)]
pub struct ManagedDirectory {
    directory: Box<Directory>,
    managed_paths: Arc<RwLock<HashSet<PathBuf>>>,
}


impl ManagedDirectory {
    pub fn new<Dir: Directory>(directory: Dir) -> ManagedDirectory {
        ManagedDirectory {
            directory: box directory,
            managed_paths: Arc::default(),
        }
    }

    fn register_file_as_managed(&mut self, filepath: PathBuf) -> Result<()> {
        let mut managed_files_lock = self.managed_paths.write()?;
        if managed_files_lock.insert(filepath) {
            let mut w = vec!();
            try!(write!(&mut w, "{}\n", json::as_pretty_json(&*managed_files_lock)));
            self.directory.atomic_write(&MANAGED_FILEPATH, &w[..])?;
        }
        Ok(())
    }
}

impl Directory for ManagedDirectory {
    
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, FileError> {
        self.directory.open_read(path)
    }

    fn open_write(&mut self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        self.directory.open_write(path)
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
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