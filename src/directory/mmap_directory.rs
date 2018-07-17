use atomicwrites;
use common::make_io_err;
use directory::error::{DeleteError, IOError, OpenDirectoryError, OpenReadError, OpenWriteError};
use directory::shared_vec_slice::SharedVecSlice;
use directory::Directory;
use directory::ReadOnlySource;
use directory::WritePtr;
use fst::raw::MmapReadOnly;
use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::convert::From;
use std::fmt;
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::Arc;
use std::sync::RwLock;
use tempdir::TempDir;

/// Returns None iff the file exists, can be read, but is empty (and hence
/// cannot be mmapped).
///
fn open_mmap(full_path: &Path) -> result::Result<Option<MmapReadOnly>, OpenReadError> {
    let file = File::open(full_path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            OpenReadError::FileDoesNotExist(full_path.to_owned())
        } else {
            OpenReadError::IOError(IOError::with_path(full_path.to_owned(), e))
        }
    })?;

    let meta_data = file.metadata()
        .map_err(|e| IOError::with_path(full_path.to_owned(), e))?;
    if meta_data.len() == 0 {
        // if the file size is 0, it will not be possible
        // to mmap the file, so we return None
        // instead.
        return Ok(None);
    }
    unsafe {
        MmapReadOnly::open(&file)
            .map(Some)
            .map_err(|e| From::from(IOError::with_path(full_path.to_owned(), e)))
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct CacheCounters {
    // Number of time the cache prevents to call `mmap`
    pub hit: usize,
    // Number of time tantivy had to call `mmap`
    // as no entry was in the cache.
    pub miss: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheInfo {
    pub counters: CacheCounters,
    pub mmapped: Vec<PathBuf>,
}

struct MmapCache {
    counters: CacheCounters,
    cache: HashMap<PathBuf, MmapReadOnly>,
}

impl Default for MmapCache {
    fn default() -> MmapCache {
        MmapCache {
            counters: CacheCounters::default(),
            cache: HashMap::new(),
        }
    }
}

impl MmapCache {
    /// Removes a `MmapReadOnly` entry from the mmap cache.
    fn discard_from_cache(&mut self, full_path: &Path) -> bool {
        self.cache.remove(full_path).is_some()
    }

    fn get_info(&mut self) -> CacheInfo {
        let paths: Vec<PathBuf> = self.cache.keys().cloned().collect();
        CacheInfo {
            counters: self.counters.clone(),
            mmapped: paths,
        }
    }

    fn get_mmap(&mut self, full_path: &Path) -> Result<Option<MmapReadOnly>, OpenReadError> {
        Ok(match self.cache.entry(full_path.to_owned()) {
            HashMapEntry::Occupied(occupied_entry) => {
                let mmap = occupied_entry.get();
                self.counters.hit += 1;
                Some(mmap.clone())
            }
            HashMapEntry::Vacant(vacant_entry) => {
                self.counters.miss += 1;
                if let Some(mmap) = open_mmap(full_path)? {
                    vacant_entry.insert(mmap.clone());
                    Some(mmap)
                } else {
                    None
                }
            }
        })
    }
}

/// Directory storing data in files, read via mmap.
///
/// The Mmap object are cached to limit the
/// system calls.
#[derive(Clone)]
pub struct MmapDirectory {
    root_path: PathBuf,
    mmap_cache: Arc<RwLock<MmapCache>>,
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
        let tempdir = TempDir::new("index")?;
        let tempdir_path = PathBuf::from(tempdir.path());
        let directory = MmapDirectory {
            root_path: tempdir_path,
            mmap_cache: Arc::new(RwLock::new(MmapCache::default())),
            _temp_directory: Arc::new(Some(tempdir)),
        };
        Ok(directory)
    }

    /// Opens a MmapDirectory in a directory.
    ///
    /// Returns an error if the `directory_path` does not
    /// exist or if it is not a directory.
    pub fn open<P: AsRef<Path>>(directory_path: P) -> Result<MmapDirectory, OpenDirectoryError> {
        let directory_path: &Path = directory_path.as_ref();
        if !directory_path.exists() {
            Err(OpenDirectoryError::DoesNotExist(PathBuf::from(
                directory_path,
            )))
        } else if !directory_path.is_dir() {
            Err(OpenDirectoryError::NotADirectory(PathBuf::from(
                directory_path,
            )))
        } else {
            Ok(MmapDirectory {
                root_path: PathBuf::from(directory_path),
                mmap_cache: Arc::new(RwLock::new(MmapCache::default())),
                _temp_directory: Arc::new(None),
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
    fn sync_directory(&self) -> Result<(), io::Error> {
        let mut open_opts = OpenOptions::new();

        // Linux needs read to be set, otherwise returns EINVAL
        // write must not be set, or it fails with EISDIR
        open_opts.read(true);

        // On Windows, opening a directory requires FILE_FLAG_BACKUP_SEMANTICS
        // and calling sync_all() only works if write access is requested.
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use winapi::winbase;

            open_opts
                .write(true)
                .custom_flags(winbase::FILE_FLAG_BACKUP_SEMANTICS);
        }

        let fd = open_opts.open(&self.root_path)?;
        fd.sync_all()?;
        Ok(())
    }

    /// Returns some statistical information
    /// about the Mmap cache.
    ///
    /// The `MmapDirectory` embeds a `MmapDirectory`
    /// to avoid multiplying the `mmap` system calls.
    pub fn get_cache_info(&mut self) -> CacheInfo {
        self.mmap_cache
            .write()
            .expect("Mmap cache lock is poisoned.")
            .get_info()
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
        self.0.flush()?;
        self.0.sync_all()
    }
}

impl Seek for SafeFileWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

impl Directory for MmapDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        debug!("Open Read {:?}", path);
        let full_path = self.resolve_path(path);

        let mut mmap_cache = self.mmap_cache.write().map_err(|_| {
            let msg = format!(
                "Failed to acquired write lock \
                 on mmap cache while reading {:?}",
                path
            );
            IOError::with_path(path.to_owned(), make_io_err(msg))
        })?;

        Ok(mmap_cache
            .get_mmap(&full_path)?
            .map(ReadOnlySource::Mmap)
            .unwrap_or_else(|| ReadOnlySource::Anonymous(SharedVecSlice::empty())))
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        debug!("Open Write {:?}", path);
        let full_path = self.resolve_path(path);

        let open_res = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(full_path);

        let mut file = open_res.map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_owned())
            } else {
                IOError::with_path(path.to_owned(), err).into()
            }
        })?;

        // making sure the file is created.
        file.flush()
            .map_err(|e| IOError::with_path(path.to_owned(), e))?;

        // Apparetntly, on some filesystem syncing the parent
        // directory is required.
        self.sync_directory()
            .map_err(|e| IOError::with_path(path.to_owned(), e))?;

        let writer = SafeFileWriter::new(file);
        Ok(BufWriter::new(Box::new(writer)))
    }

    /// Any entry associated to the path in the mmap will be
    /// removed before the file is deleted.
    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        debug!("Deleting file {:?}", path);
        let full_path = self.resolve_path(path);
        let mut mmap_cache = self.mmap_cache.write().map_err(|_| {
            let msg = format!(
                "Failed to acquired write lock \
                 on mmap cache while deleting {:?}",
                path
            );
            IOError::with_path(path.to_owned(), make_io_err(msg))
        })?;
        mmap_cache.discard_from_cache(path);

        // Removing the entry in the MMap cache.
        // The munmap will appear on Drop,
        // when the last reference is gone.
        mmap_cache.cache.remove(&full_path);
        match fs::remove_file(&full_path) {
            Ok(_) => self.sync_directory()
                .map_err(|e| IOError::with_path(path.to_owned(), e).into()),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Err(DeleteError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(IOError::with_path(path.to_owned(), e).into())
                }
            }
        }
    }

    fn exists(&self, path: &Path) -> bool {
        let full_path = self.resolve_path(path);
        full_path.exists()
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let full_path = self.resolve_path(path);
        let mut buffer = Vec::new();
        match File::open(&full_path) {
            Ok(mut file) => {
                file.read_to_end(&mut buffer)
                    .map_err(|e| IOError::with_path(path.to_owned(), e))?;
                Ok(buffer)
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Err(OpenReadError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(IOError::with_path(path.to_owned(), e).into())
                }
            }
        }
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        debug!("Atomic Write {:?}", path);
        let full_path = self.resolve_path(path);
        let meta_file = atomicwrites::AtomicFile::new(full_path, atomicwrites::AllowOverwrite);
        meta_file.write(|f| f.write_all(data))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    // There are more tests in directory/mod.rs
    // The following tests are specific to the MmapDirectory

    use super::*;

    #[test]
    fn test_open_empty() {
        // empty file is actually an edge case because those
        // cannot be mmapped.
        //
        // In that case the directory returns a SharedVecSlice.
        let mut mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        let path = PathBuf::from("test");
        {
            let mut w = mmap_directory.open_write(&path).unwrap();
            w.flush().unwrap();
        }
        let readonlymap = mmap_directory.open_read(&path).unwrap();
        assert_eq!(readonlymap.len(), 0);
    }

    #[test]
    fn test_cache() {
        let content = "abc".as_bytes();

        // here we test if the cache releases
        // mmaps correctly.
        let mut mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        let num_paths = 10;
        let paths: Vec<PathBuf> = (0..num_paths)
            .map(|i| PathBuf::from(&*format!("file_{}", i)))
            .collect();
        {
            for path in &paths {
                let mut w = mmap_directory.open_write(path).unwrap();
                w.write(content).unwrap();
                w.flush().unwrap();
            }
        }
        {
            for (i, path) in paths.iter().enumerate() {
                let _r = mmap_directory.open_read(path).unwrap();
                assert_eq!(mmap_directory.get_cache_info().mmapped.len(), i + 1);
            }
            for path in paths.iter() {
                let _r = mmap_directory.open_read(path).unwrap();
                assert_eq!(mmap_directory.get_cache_info().mmapped.len(), num_paths);
            }
            for (i, path) in paths.iter().enumerate() {
                mmap_directory.delete(path).unwrap();
                assert_eq!(
                    mmap_directory.get_cache_info().mmapped.len(),
                    num_paths - i - 1
                );
            }
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 10);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);
    }

}
