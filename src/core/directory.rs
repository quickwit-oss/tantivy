use std::io::BufWriter;
use std::io;
use std::io::Write;
use std::fs::File;
use std::fmt;
use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use fst::raw::MmapReadOnly;
use atomicwrites;
use std::sync::Arc;
use std::sync::RwLock;
use tempdir::TempDir;
use std::cell::RefCell;
use std::ops::Deref;
use std::path::{Path, PathBuf};

///////////////////////////////////////////////////////////////

pub enum ReadOnlySource {
    Mmap(MmapReadOnly),
    Anonymous(Vec<u8>),
}

impl Deref for ReadOnlySource {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ReadOnlySource {
    pub fn as_slice(&self,) -> &[u8] {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => unsafe { mmap_read_only.as_slice() },
            ReadOnlySource::Anonymous(ref shared_vec) => shared_vec.as_slice(),
        }
    }

    pub fn slice(&self, from_offset:usize, to_offset:usize) -> ReadOnlySource {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => {
                let sliced_mmap = mmap_read_only.range(from_offset, to_offset - from_offset);
                ReadOnlySource::Mmap(sliced_mmap)
            }
            ReadOnlySource::Anonymous(ref shared_vec) => {
                let sliced_data: Vec<u8> = Vec::from(&shared_vec[from_offset..to_offset]);
                ReadOnlySource::Anonymous(sliced_data)
            },
        }
    }
}

//
// #[derive(Debug)]
// pub enum CreateError {
//     RootDirectoryDoesNotExist,
//     DirectoryAlreadyExists,
//     CannotCreateTempDirectory(io::Error),
// }

pub trait Directory: fmt::Debug {
    fn open_read(&self, path: &Path) -> io::Result<ReadOnlySource>;
    fn open_write(&mut self, path: &Path) -> io::Result<Box<Write>>;
    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()>;
    fn sync(&self, path: &Path) -> io::Result<()>;
    fn sync_directory(&self,) -> io::Result<()>;
}

pub type WritePtr = Box<Write>;



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

    pub fn create_from_tempdir() -> io::Result<MmapDirectory> {
        // TODO error management
        let tempdir = try!(TempDir::new("index"));
        let tempdir_path = PathBuf::from(tempdir.path());
        let directory = MmapDirectory {
            root_path: PathBuf::from(tempdir_path),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: Some(tempdir)
        };
        Ok(directory)
    }

    pub fn create(filepath: &Path) -> io::Result<MmapDirectory> {
        Ok(MmapDirectory {
            root_path: PathBuf::from(filepath),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: None
        })
    }

    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }


}

impl Directory for MmapDirectory {
    fn open_read(&self, path: &Path) -> io::Result<ReadOnlySource> {
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
    fn open_write(&mut self, path: &Path) -> io::Result<WritePtr> {
        let full_path = self.resolve_path(path);
        let file = try!(File::create(full_path));
        let buf_writer = BufWriter::new(file);
        Ok(Box::new(buf_writer))
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        meta_file.write(|f| {
            f.write_all(data)
        });
        Ok(())
    }

    fn sync(&self, path: &Path) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        File::open(&full_path).and_then(|fd| fd.sync_all())
    }

    fn sync_directory(&self,) -> io::Result<()> {
        File::open(&self.root_path).and_then(|fd| fd.sync_all())
    }
}




////////////////////////////////////////////////////////////////
// RAMDirectory


#[derive(Clone)]
struct SharedVec(Arc<RwLock<Vec<u8>>>);


pub struct RAMDirectory {
    fs: HashMap<PathBuf, SharedVec>,
}

impl SharedVec {
    fn new() -> SharedVec {
        SharedVec(Arc::new( RwLock::new(Vec::new()) ))
    }
}

impl Write for SharedVec {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write().unwrap().write(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}


impl fmt::Debug for RAMDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "RAMDirectory")
   }
}

impl RAMDirectory {
    pub fn create() -> RAMDirectory {
        RAMDirectory {
            fs: HashMap::new()
        }
    }
}

impl Directory for RAMDirectory {
    fn open_read(&self, path: &Path) -> io::Result<ReadOnlySource> {
        match self.fs.get(path) {
            Some(ref data) => {
                let data_copy = (*data).0.read().unwrap().clone();
                Ok(ReadOnlySource::Anonymous(data_copy))
            },
            None =>
                Err(io::Error::new(io::ErrorKind::NotFound, "File has never been created."))
        }
    }
    fn open_write(&mut self, path: &Path) -> io::Result<WritePtr> {
        let full_path = PathBuf::from(&path);
        let mut data = SharedVec::new();
        self.fs.insert(full_path, data.clone());
        Ok(Box::new(data))
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        let meta_file = atomicwrites::AtomicFile::new(PathBuf::from(path), atomicwrites::AllowOverwrite);
        meta_file.write(|f| {
            f.write_all(data)
        });
        Ok(())
    }

    fn sync(&self, path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn sync_directory(&self,) -> io::Result<()> {
        Ok(())
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use core::schema::DocId;
    use std::path::Path;

    #[test]
    fn test_ram_directory() {
        let mut ram_directory = RAMDirectory::create();
        test_directory(&mut ram_directory);
    }

    #[test]
    fn test_mmap_directory() {
        let mut mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        test_directory(&mut mmap_directory);
    }

    fn test_directory(directory: &mut Directory) {
        {
            let mut write_file = directory.open_write(Path::new("toto")).unwrap();
            write_file.write_all(&[4]);
            write_file.write_all(&[3]);
            write_file.write_all(&[7,3,5]);
        }
        let read_file = directory.open_read(Path::new("toto")).unwrap();
        let data: &[u8] = &*read_file;
        assert_eq!(data.len(), 5);
        assert_eq!(data[0], 4);
        assert_eq!(data[1], 3);
        assert_eq!(data[2], 7);
        assert_eq!(data[3], 3);
        assert_eq!(data[4], 5);
    }




}
