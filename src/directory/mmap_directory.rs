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
use std::io::{Seek, SeekFrom};
use directory::Directory;
use directory::ReadOnlySource;
use directory::WritePtr;
use std::io::BufWriter;
use std::fs::OpenOptions;
use directory::{OpenWriteError, OpenReadError};
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

    fn sync_directory(&self,) -> Result<(), io::Error> {
        let fd = try!(File::open(&self.root_path));
        try!(fd.sync_all());
        Ok(())
    }
}

/// This Write wraps a File, but has the specificity of 
/// call sync_all on flush.  
struct SafeFileWriter {
    writer: BufWriter<File>,
}

impl SafeFileWriter {
    fn new(file: File) -> SafeFileWriter {
        SafeFileWriter {
            writer: BufWriter::new(file),
        }
    }

}

impl Write for SafeFileWriter {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        try!(self.writer.flush());
        self.writer.get_ref().sync_all()
    }
}

impl Seek for SafeFileWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.writer.seek(pos)
    }
}


impl Directory for MmapDirectory {
    
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
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
                        if err.kind() == io::ErrorKind::NotFound {
                            OpenReadError::FileDoesNotExist(PathBuf::from(&full_path))
                        }
                        else {
                            OpenReadError::IOError(err)
                        }
                    })
                );
                vacant_entry.insert(new_mmap.clone());
                new_mmap
            }
        };
        Ok(ReadOnlySource::Mmap(mmap))
    }
    
    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let full_path = self.resolve_path(path);
        
        let open_res = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(full_path);

        let mut file = try!(
            open_res.map_err(|err| {
                if err.kind() == io::ErrorKind::AlreadyExists {
                    OpenWriteError::FileAlreadyExists(PathBuf::from(path))
                }
                else {
                    OpenWriteError::IOError(err)
                }
            })
        );

        // making sure the file is created.
        try!(file.flush());
        
        // Apparetntly, on some filesystem syncing the parent
        // directory is required.
        try!(self.sync_directory());
        
        let writer = SafeFileWriter::new(file);
        Ok(Box::new(writer))
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        try!(meta_file.write(|f| {
            f.write_all(data)
        }));
        Ok(())
    }

}
