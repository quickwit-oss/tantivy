use std::path::{Path, PathBuf};
use tempdir::TempDir;
use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use fst::raw::MmapReadOnly;
use std::fs::File;
use std::io::Read;
use std::fs::ReadDir;
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
use directory::error::{OpenWriteError, FileError, OpenDirectoryError};
use std::result;
use common::make_io_err;
use std::sync::Arc;
use std::fs;
use directory::shared_vec_slice::SharedVecSlice;


/// Directory storing data in files, read via mmap.
///
/// The Mmap object are cached to limit the 
/// system calls. 
#[derive(Clone)]
pub struct MmapDirectory {
    root_path: PathBuf,
    mmap_cache: Arc<RwLock<HashMap<PathBuf, MmapReadOnly>>>,
    _temp_directory: Arc<Option<TempDir>>,
}

impl fmt::Debug for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "MmapDirectory({:?})", self.root_path)
   }
}



impl MmapDirectory {

    /// Creates a new MmapDirectory in a temporary directory.
    ///
    /// This is mostly useful to test the MmapDirectory itself.
    /// For your unit tests, prefer the RAMDirectory. 
    pub fn create_from_tempdir() -> io::Result<MmapDirectory> {
        let tempdir = try!(TempDir::new("index"));
        let tempdir_path = PathBuf::from(tempdir.path());
        let directory = MmapDirectory {
            root_path: PathBuf::from(tempdir_path),
            mmap_cache: Arc::new(RwLock::new(HashMap::new())),
            _temp_directory: Arc::new(Some(tempdir))
        };
        Ok(directory)
    }


    /// Opens a MmapDirectory in a directory.
    ///
    /// Returns an error if the `directory_path` does not
    /// exist or if it is not a directory.
    pub fn open(directory_path: &Path) -> Result<MmapDirectory, OpenDirectoryError> {
        if !directory_path.exists() {
            Err(OpenDirectoryError::DoesNotExist(PathBuf::from(directory_path)))
        }
        else if !directory_path.is_dir() {
            Err(OpenDirectoryError::NotADirectory(PathBuf::from(directory_path)))
        }
        else {
            Ok(MmapDirectory {
                root_path: PathBuf::from(directory_path),
                mmap_cache: Arc::new(RwLock::new(HashMap::new())),
                _temp_directory: Arc::new(None)
            })
        }
    }

    /// Joins a relative_path to the directory `root_path`
    /// to create a proper complete `filepath`.
    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }

    /// Sync the root directory.
    /// In certain FS, this is required to persistently create
    /// a file.
    fn sync_directory(&self,) -> Result<(), io::Error> {
        let fd = try!(File::open(&self.root_path));
        try!(fd.sync_all());
        Ok(())
    }

}

/// This Write wraps a File, but has the specificity of 
/// call `sync_all` on flush.  
struct SafeFileWriter(File);

impl SafeFileWriter {
    fn new(file: File) -> SafeFileWriter {
        SafeFileWriter(file)
    }
}

impl Write for SafeFileWriter {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        try!(self.0.flush());
        self.0.sync_all()
    }
}

impl Seek for SafeFileWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}


impl Directory for MmapDirectory {

    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, FileError> {
        debug!("Open Read {:?}", path);
        let full_path = self.resolve_path(path);
        
        let mut mmap_cache = try!(
            self.mmap_cache
                .write()
                .map_err(|_| {
                    make_io_err(format!("Failed to acquired write lock on mmap cache while reading {:?}", path))
                })
        );

        let mmap = match mmap_cache.entry(full_path.clone()) {
            HashMapEntry::Occupied(e) => {
                e.get().clone()
            }
            HashMapEntry::Vacant(vacant_entry) => {
                let file = try!(
                    File::open(&full_path).map_err(|err| {
                        if err.kind() == io::ErrorKind::NotFound {
                            FileError::FileDoesNotExist(full_path.clone())
                        }
                        else {
                            FileError::IOError(err)
                        }
                    })
                );
                if try!(file.metadata()).len() == 0 {
                    // if the file size is 0, it will not be possible 
                    // to mmap the file, so we return an anonymous mmap_cache
                    // instead.
                    return Ok(ReadOnlySource::Anonymous(SharedVecSlice::empty()))
                }
                let new_mmap = try!(MmapReadOnly::open(&file));
                vacant_entry.insert(new_mmap.clone());
                new_mmap
            }
        };        
        Ok(ReadOnlySource::Mmap(mmap))
    }
    
    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        debug!("Open Write {:?}", path);
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
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn delete(&self, path: &Path) -> result::Result<(), FileError> {
        debug!("Deleting file {:?}", path);
        let full_path = self.resolve_path(path);
        let mut mmap_cache = try!(self.mmap_cache
            .write()
            .map_err(|_| {
                make_io_err(format!("Failed to acquired write lock on mmap cache while deleting {:?}", path))
            })
        );
        // Removing the entry in the MMap cache.
        // The munmap will appear on Drop,
        // when the last reference is gone.
        mmap_cache.remove(&full_path);
        try!(fs::remove_file(&full_path));
        try!(self.sync_directory());
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        let full_path = self.resolve_path(path);
        full_path.exists()
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, FileError> {
        let full_path = self.resolve_path(path);
        let mut buffer = Vec::new();
        File::open(&full_path)?.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        debug!("Atomic Write {:?}", path);
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        try!(meta_file.write(|f| {
            f.write_all(data)
        }));
        Ok(())
    }

    fn box_clone(&self,) -> Box<Directory> {
        Box::new(self.clone())
    }
    
    fn ls_starting_with(&self, prefix: &str) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(&self.root_path)
        .map(|paths: ReadDir| {
            paths
            .filter_map(|dir_entry_res|
                dir_entry_res
                    .ok()
                    .map(|dir_entry| dir_entry.path())
            )
            .filter(|path| 
                path.to_str()
                    .map(|filepath| filepath.starts_with(prefix))
                    .unwrap_or(false)
            )
            .map(PathBuf::from)
            .collect()
        })
          
    }
}
