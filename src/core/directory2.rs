use fst::raw::FstData as ReadOnlySource;
use std::io::BufWriter;
use std::io;
use fst::raw::SharedVectorSlice;
use std::io::Write;
use std::fs::File;
use std::fmt;
use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use fst::raw::MmapReadOnly;
use atomicwrites;
use tempdir::TempDir;
use std::cell::RefCell;
use std::path::{Path, PathBuf};


pub enum CreateError {
    RootDirectoryDoesNotExist,
    DirectoryAlreadyExists,
    CannotCreateTempDirectory(io::Error),
}

pub trait Directory: fmt::Debug {
    fn open_read<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadOnlySource>;
    fn open_write<P: AsRef<Path>>(&mut self, path: P) -> io::Result<Box<Write>>;
    fn atomic_write<P: AsRef<Path>>(&mut self, path: P, data: &[u8]) -> io::Result<()>;
}


////////////////////////////////////////////////////////////////
// MmapDirectory

pub struct MmapDirectory {
    root_path: PathBuf,
    mmap_cache: RefCell<HashMap<PathBuf, MmapReadOnly>>,
    _temp_directory: Option<TempDir>,
}

impl fmt::Debug for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "MmapDirectory({:?})", self.root_path)
   }
}



impl MmapDirectory {

    pub fn create_tempdir() -> Result<MmapDirectory, CreateError> {
        // TODO error management
        let tempdir = try!(TempDir::new("index").map_err(CreateError::CannotCreateTempDirectory));
        let tempdir_path = PathBuf::from(tempdir.path());
        let mut directory = MmapDirectory {
            root_path: PathBuf::from(tempdir_path),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: Some(tempdir)
        };
        Ok(directory)
    }

    pub fn create<P: AsRef<Path>>(filepath: P) -> Result<MmapDirectory, CreateError> {
        Ok(MmapDirectory {
            root_path: PathBuf::from(filepath.as_ref()),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: None
        })
    }

    fn resolve_path<P: AsRef<Path>>(&self, relative_path: P) -> PathBuf {
        self.root_path.join(relative_path)
    }
}

impl Directory for MmapDirectory {
    fn open_read<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadOnlySource> {
        let full_path = self.resolve_path(path);
        let mut mmap_cache = self.mmap_cache.borrow_mut();
        let mmap = match mmap_cache.entry(full_path.clone()) {
            HashMapEntry::Occupied(e) => e.get().clone(),
            HashMapEntry::Vacant(vacant_entry) => {
                let new_mmap =  try!(MmapReadOnly::open_path(full_path.clone()));
                vacant_entry.insert(new_mmap.clone());
                new_mmap
            }
        };
        Ok(ReadOnlySource::Mmap(mmap))
    }
    fn open_write<P: AsRef<Path>>(&mut self, path: P) -> io::Result<Box<Write>> {
        let full_path = self.resolve_path(path);
        let file = try!(File::create(full_path));
        Ok(Box::new(file))
    }

    fn atomic_write<P: AsRef<Path>>(&mut self, path: P, data: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        meta_file.write(|f| {
            f.write_all(data)
        });
        Ok(())
    }
}




////////////////////////////////////////////////////////////////
// RAMDirectory


pub struct RAMDirectory {
    fs: RefCell<HashMap<PathBuf, SharedVectorSlice>>,
}

impl fmt::Debug for RAMDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "RAMDirectory")
   }
}


impl RAMDirectory {


    pub fn create() -> Result<RAMDirectory, CreateError> {
        Ok(RAMDirectory {
            fs: RefCell::new(HashMap::new())
        })
    }
}

impl Directory for RAMDirectory {
    fn open_read<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadOnlySource> {
        // let full_path = PathBuf::from(path);
        match self.fs.borrow().get(path.as_ref()) {
            Some(data) => Ok(ReadOnlySource::SharedVector(*data.clone())),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "File has never been created."))
        }
        // Ok(ReadOnlySource::Mmap(mmap))
    }
    fn open_write<P: AsRef<Path>>(&mut self, path: P) -> io::Result<Box<Write>> {
        let full_path = PathBuf::from(path);
        let data = SharedVectorSlice::from(Vec::new());
        self.fs.borrow_mut().insert(full_path, data.clone());
        Ok(Box::new(data))
    }

    fn atomic_write<P: AsRef<Path>>(&mut self, path: P, data: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        meta_file.write(|f| {
            f.write_all(data)
        });
        Ok(())
    }
}
