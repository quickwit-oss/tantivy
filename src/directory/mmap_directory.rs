use Result;
use std::path::{Path, PathBuf};
use tempdir::TempDir;
use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use fst::raw::MmapReadOnly;
use std::fs::File;
use atomicwrites;
use std::sync::RwLock;
use std::fmt;
use std::io::Write;
use std::io;
use directory::Directory;
use directory::ReadOnlySource;
use directory::WritePtr;
use std::io::BufWriter;
use directory::OpenError;
use std::result;

////////////////////////////////////////////////////////////////
// MmapDirectory

pub struct MmapDirectory {
    root_path: PathBuf,
    mmap_cache: RwLock<HashMap<PathBuf, MmapReadOnly>>,
    _temp_directory: Option<TempDir>,
}

impl fmt::Debug for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "MmapDirectory({:?})", self.root_path)
   }
}


impl MmapDirectory {

    pub fn create_from_tempdir() -> io::Result<MmapDirectory> {
        // TODO error management
        let tempdir = try!(TempDir::new("index"));
        let tempdir_path = PathBuf::from(tempdir.path());
        let directory = MmapDirectory {
            root_path: PathBuf::from(tempdir_path),
            mmap_cache: RwLock::new(HashMap::new()),
            _temp_directory: Some(tempdir)
        };
        Ok(directory)
    }

    pub fn open(filepath: &Path) -> io::Result<MmapDirectory> {
        Ok(MmapDirectory {
            root_path: PathBuf::from(filepath),
            mmap_cache: RwLock::new(HashMap::new()),
            _temp_directory: None
        })
    }

    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }


}

impl Directory for MmapDirectory {
    
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenError> {
        let full_path = self.resolve_path(path);
        let mut mmap_cache = self.mmap_cache.write().unwrap();
        let mmap = match mmap_cache.entry(full_path.clone()) {
            HashMapEntry::Occupied(e) => {
                e.get().clone()
            }
            HashMapEntry::Vacant(vacant_entry) => {
                let new_mmap =  try!(
                    MmapReadOnly::open_path(full_path.clone())
                    .map_err(|err| {
                        if err.kind() == io::ErrorKind::AlreadyExists {
                            OpenError::FileDoesNotExist(PathBuf::from(&full_path))
                        }
                        else {
                            OpenError::IOError(err)
                        }
                    })
                );
                vacant_entry.insert(new_mmap.clone());
                new_mmap
            }
        };
        Ok(ReadOnlySource::Mmap(mmap))
    }
    
    
    fn open_write(&mut self, path: &Path) -> Result<WritePtr> {
        let full_path = self.resolve_path(path);
        let file = try!(File::create(full_path));
        let buf_writer = BufWriter::new(file);
        Ok(Box::new(buf_writer))
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> Result<()> {
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        try!(meta_file.write(|f| {
            f.write_all(data)
        }));
        Ok(())
    }

    fn sync(&self, path: &Path) -> Result<()> {
        let full_path = self.resolve_path(path);
        match File::open(&full_path) {
            Ok(fd) => {
                try!(fd.sync_all());
                Ok(())
            }
            Err(_) => {
                // file does not exists.
                // this is not considered a failure as some of the file (postings) only exists if 
                // a functionality is used
                //
                // TODO be fine-grained about this.
                Ok(())
            }
        }
    }

    fn sync_directory(&self,) -> Result<()> {
        let fd = try!(File::open(&self.root_path));
        try!(fd.sync_all());
        Ok(())
    }
}
