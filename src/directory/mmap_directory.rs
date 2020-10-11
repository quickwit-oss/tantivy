use crate::core::META_FILEPATH;
use crate::directory::error::LockError;
use crate::directory::error::{DeleteError, OpenDirectoryError, OpenReadError, OpenWriteError};
use crate::directory::AntiCallToken;
use crate::directory::BoxedData;
use crate::directory::Directory;
use crate::directory::DirectoryLock;
use crate::directory::FileSlice;
use crate::directory::Lock;
use crate::directory::WatchCallback;
use crate::directory::WatchCallbackList;
use crate::directory::WatchHandle;
use crate::directory::{TerminatingWrite, WritePtr};
use fs2::FileExt;
use memmap::Mmap;
use notify::RawEvent;
use notify::RecursiveMode;
use notify::Watcher;
use serde::{Deserialize, Serialize};
use stable_deref_trait::StableDeref;
use std::convert::From;
use std::fmt;
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;
use std::thread;
use std::{collections::HashMap, ops::Deref};
use tempfile::TempDir;

/// Create a default io error given a string.
pub(crate) fn make_io_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

/// Returns None iff the file exists, can be read, but is empty (and hence
/// cannot be mmapped)
fn open_mmap(full_path: &Path) -> result::Result<Option<Mmap>, OpenReadError> {
    let file = File::open(full_path).map_err(|io_err| {
        if io_err.kind() == io::ErrorKind::NotFound {
            OpenReadError::FileDoesNotExist(full_path.to_path_buf())
        } else {
            OpenReadError::wrap_io_error(io_err, full_path.to_path_buf())
        }
    })?;

    let meta_data = file
        .metadata()
        .map_err(|io_err| OpenReadError::wrap_io_error(io_err, full_path.to_owned()))?;
    if meta_data.len() == 0 {
        // if the file size is 0, it will not be possible
        // to mmap the file, so we return None
        // instead.
        return Ok(None);
    }
    unsafe {
        memmap::Mmap::map(&file)
            .map(Some)
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, full_path.to_path_buf()))
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
    cache: HashMap<PathBuf, Weak<BoxedData>>,
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
    fn get_info(&self) -> CacheInfo {
        let paths: Vec<PathBuf> = self.cache.keys().cloned().collect();
        CacheInfo {
            counters: self.counters.clone(),
            mmapped: paths,
        }
    }

    fn remove_weak_ref(&mut self) {
        let keys_to_remove: Vec<PathBuf> = self
            .cache
            .iter()
            .filter(|(_, mmap_weakref)| mmap_weakref.upgrade().is_none())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys_to_remove {
            self.cache.remove(&key);
        }
    }

    // Returns None if the file exists but as a len of 0 (and hence is not mmappable).
    fn get_mmap(&mut self, full_path: &Path) -> Result<Option<Arc<BoxedData>>, OpenReadError> {
        if let Some(mmap_weak) = self.cache.get(full_path) {
            if let Some(mmap_arc) = mmap_weak.upgrade() {
                self.counters.hit += 1;
                return Ok(Some(mmap_arc));
            }
        }
        self.cache.remove(full_path);
        self.counters.miss += 1;
        let mmap_opt = open_mmap(full_path)?;
        Ok(mmap_opt.map(|mmap| {
            let mmap_arc: Arc<BoxedData> = Arc::new(Box::new(mmap));
            let mmap_weak = Arc::downgrade(&mmap_arc);
            self.cache.insert(full_path.to_owned(), mmap_weak);
            mmap_arc
        }))
    }
}

struct WatcherWrapper {
    _watcher: Mutex<notify::RecommendedWatcher>,
    watcher_router: Arc<WatchCallbackList>,
}

impl WatcherWrapper {
    pub fn new(path: &Path) -> Result<Self, OpenDirectoryError> {
        let (tx, watcher_recv): (Sender<RawEvent>, Receiver<RawEvent>) = channel();
        // We need to initialize the
        let watcher = notify::raw_watcher(tx)
            .and_then(|mut watcher| {
                watcher.watch(path, RecursiveMode::Recursive)?;
                Ok(watcher)
            })
            .map_err(|err| match err {
                notify::Error::PathNotFound => OpenDirectoryError::DoesNotExist(path.to_owned()),
                _ => {
                    panic!("Unknown error while starting watching directory {:?}", path);
                }
            })?;
        let watcher_router: Arc<WatchCallbackList> = Default::default();
        let watcher_router_clone = watcher_router.clone();
        thread::Builder::new()
            .name("meta-file-watch-thread".to_string())
            .spawn(move || {
                loop {
                    match watcher_recv.recv().map(|evt| evt.path) {
                        Ok(Some(changed_path)) => {
                            // ... Actually subject to false positive.
                            // We might want to be more accurate than this at one point.
                            if let Some(filename) = changed_path.file_name() {
                                if filename == *META_FILEPATH {
                                    let _ = watcher_router_clone.broadcast();
                                }
                            }
                        }
                        Ok(None) => {
                            // not an event we are interested in.
                        }
                        Err(_e) => {
                            // the watch send channel was dropped
                            break;
                        }
                    }
                }
            })
            .map_err(|io_error| OpenDirectoryError::IoError {
                io_error,
                directory_path: path.to_path_buf(),
            })?;
        Ok(WatcherWrapper {
            _watcher: Mutex::new(watcher),
            watcher_router,
        })
    }

    pub fn watch(&mut self, watch_callback: WatchCallback) -> WatchHandle {
        self.watcher_router.subscribe(watch_callback)
    }
}

/// Directory storing data in files, read via mmap.
///
/// The Mmap object are cached to limit the
/// system calls.
///
/// In the `MmapDirectory`, locks are implemented using the `fs2` crate definition of locks.
///
/// On MacOS & linux, it relies on `flock` (aka `BSD Lock`). These locks solve most of the
/// problems related to POSIX Locks, but may their contract may not be respected on `NFS`
/// depending on the implementation.
///
/// On Windows the semantics are again different.
#[derive(Clone)]
pub struct MmapDirectory {
    inner: Arc<MmapDirectoryInner>,
}

struct MmapDirectoryInner {
    root_path: PathBuf,
    mmap_cache: RwLock<MmapCache>,
    _temp_directory: Option<TempDir>,
    watcher: RwLock<Option<WatcherWrapper>>,
}

impl MmapDirectoryInner {
    fn new(root_path: PathBuf, temp_directory: Option<TempDir>) -> MmapDirectoryInner {
        MmapDirectoryInner {
            root_path,
            mmap_cache: Default::default(),
            _temp_directory: temp_directory,
            watcher: RwLock::new(None),
        }
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        // a lot of juggling here, to ensure we don't do anything that panics
        // while the rwlock is held. That way we ensure that the rwlock cannot
        // be poisoned.
        //
        // The downside is that we might create a watch wrapper that is not useful.
        let need_initialization = self.watcher.read().unwrap().is_none();
        if need_initialization {
            let watch_wrapper = WatcherWrapper::new(&self.root_path)?;
            let mut watch_wlock = self.watcher.write().unwrap();
            // the watcher could have been initialized when we released the lock, and
            // we do not want to lose the watched files that were set.
            if watch_wlock.is_none() {
                *watch_wlock = Some(watch_wrapper);
            }
        }
        if let Some(watch_wrapper) = self.watcher.write().unwrap().as_mut() {
            Ok(watch_wrapper.watch(watch_callback))
        } else {
            unreachable!("At this point, watch wrapper is supposed to be initialized");
        }
    }
}

impl fmt::Debug for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MmapDirectory({:?})", self.inner.root_path)
    }
}

impl MmapDirectory {
    fn new(root_path: PathBuf, temp_directory: Option<TempDir>) -> MmapDirectory {
        let inner = MmapDirectoryInner::new(root_path, temp_directory);
        MmapDirectory {
            inner: Arc::new(inner),
        }
    }

    /// Creates a new MmapDirectory in a temporary directory.
    ///
    /// This is mostly useful to test the MmapDirectory itself.
    /// For your unit tests, prefer the RAMDirectory.
    pub fn create_from_tempdir() -> Result<MmapDirectory, OpenDirectoryError> {
        let tempdir = TempDir::new().map_err(OpenDirectoryError::FailedToCreateTempDir)?;
        Ok(MmapDirectory::new(
            tempdir.path().to_path_buf(),
            Some(tempdir),
        ))
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
            Ok(MmapDirectory::new(PathBuf::from(directory_path), None))
        }
    }

    /// Joins a relative_path to the directory `root_path`
    /// to create a proper complete `filepath`.
    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.inner.root_path.join(relative_path)
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
            use winapi::um::winbase;

            open_opts
                .write(true)
                .custom_flags(winbase::FILE_FLAG_BACKUP_SEMANTICS);
        }

        let fd = open_opts.open(&self.inner.root_path)?;
        fd.sync_all()?;
        Ok(())
    }

    /// Returns some statistical information
    /// about the Mmap cache.
    ///
    /// The `MmapDirectory` embeds a `MmapDirectory`
    /// to avoid multiplying the `mmap` system calls.
    pub fn get_cache_info(&self) -> CacheInfo {
        self.inner
            .mmap_cache
            .write()
            .expect("mmap cache lock is poisoned")
            .remove_weak_ref();
        self.inner
            .mmap_cache
            .read()
            .expect("Mmap cache lock is poisoned.")
            .get_info()
    }
}

/// We rely on fs2 for file locking. On Windows & MacOS this
/// uses BSD locks (`flock`). The lock is actually released when
/// the `File` object is dropped and its associated file descriptor
/// is closed.
struct ReleaseLockFile {
    _file: File,
    path: PathBuf,
}

impl Drop for ReleaseLockFile {
    fn drop(&mut self) {
        debug!("Releasing lock {:?}", self.path);
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

impl TerminatingWrite for SafeFileWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

#[derive(Clone)]
struct MmapArc(Arc<Box<dyn Deref<Target = [u8]> + Send + Sync>>);

impl Deref for MmapArc {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}
unsafe impl StableDeref for MmapArc {}

impl Directory for MmapDirectory {
    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        debug!("Open Read {:?}", path);
        let full_path = self.resolve_path(path);

        let mut mmap_cache = self.inner.mmap_cache.write().map_err(|_| {
            let msg = format!(
                "Failed to acquired write lock \
                 on mmap cache while reading {:?}",
                path
            );
            let io_err = make_io_err(msg);
            OpenReadError::wrap_io_error(io_err, path.to_path_buf())
        })?;
        if let Some(mmap_arc) = mmap_cache.get_mmap(&full_path)? {
            Ok(FileSlice::from(MmapArc(mmap_arc)))
        } else {
            Ok(FileSlice::empty())
        }
    }

    /// Any entry associated to the path in the mmap will be
    /// removed before the file is deleted.
    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        let full_path = self.resolve_path(path);
        match fs::remove_file(&full_path) {
            Ok(_) => self.sync_directory().map_err(|e| DeleteError::IOError {
                io_error: e,
                filepath: path.to_path_buf(),
            }),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Err(DeleteError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(DeleteError::IOError {
                        io_error: e,
                        filepath: path.to_path_buf(),
                    })
                }
            }
        }
    }

    fn exists(&self, path: &Path) -> bool {
        let full_path = self.resolve_path(path);
        full_path.exists()
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        debug!("Open Write {:?}", path);
        let full_path = self.resolve_path(path);

        let open_res = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(full_path);

        let mut file = open_res.map_err(|io_err| {
            if io_err.kind() == io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_path_buf())
            } else {
                OpenWriteError::wrap_io_error(io_err, path.to_path_buf())
            }
        })?;

        // making sure the file is created.
        file.flush()
            .map_err(|io_error| OpenWriteError::wrap_io_error(io_error, path.to_path_buf()))?;

        // Apparetntly, on some filesystem syncing the parent
        // directory is required.
        self.sync_directory()
            .map_err(|io_err| OpenWriteError::wrap_io_error(io_err, path.to_path_buf()))?;

        let writer = SafeFileWriter::new(file);
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let full_path = self.resolve_path(path);
        let mut buffer = Vec::new();
        match File::open(&full_path) {
            Ok(mut file) => {
                file.read_to_end(&mut buffer).map_err(|io_error| {
                    OpenReadError::wrap_io_error(io_error, path.to_path_buf())
                })?;
                Ok(buffer)
            }
            Err(io_error) => {
                if io_error.kind() == io::ErrorKind::NotFound {
                    Err(OpenReadError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(OpenReadError::wrap_io_error(io_error, path.to_path_buf()))
                }
            }
        }
    }

    fn atomic_write(&self, path: &Path, content: &[u8]) -> io::Result<()> {
        debug!("Atomic Write {:?}", path);
        let mut tempfile = tempfile::Builder::new().tempfile_in(&self.inner.root_path)?;
        tempfile.write_all(content)?;
        tempfile.flush()?;
        let full_path = self.resolve_path(path);
        tempfile.into_temp_path().persist(full_path)?;
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let full_path = self.resolve_path(&lock.filepath);
        // We make sure that the file exists.
        let file: File = OpenOptions::new()
            .write(true)
            .create(true) //< if the file does not exist yet, create it.
            .open(&full_path)
            .map_err(LockError::IOError)?;
        if lock.is_blocking {
            file.lock_exclusive().map_err(LockError::IOError)?;
        } else {
            file.try_lock_exclusive().map_err(|_| LockError::LockBusy)?
        }
        // dropping the file handle will release the lock.
        Ok(DirectoryLock::from(Box::new(ReleaseLockFile {
            path: lock.filepath.clone(),
            _file: file,
        })))
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

#[cfg(test)]
mod tests {

    // There are more tests in directory/mod.rs
    // The following tests are specific to the MmapDirectory

    use super::*;
    use crate::schema::{Schema, SchemaBuilder, TEXT};
    use crate::Index;
    use crate::ReloadPolicy;
    use crate::{common::HasLen, indexer::LogMergePolicy};
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_open_non_existent_path() {
        assert!(MmapDirectory::open(PathBuf::from("./nowhere")).is_err());
    }

    #[test]
    fn test_open_empty() {
        // empty file is actually an edge case because those
        // cannot be mmapped.
        //
        // In that case the directory returns a SharedVecSlice.
        let mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
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
        let content = b"abc";

        // here we test if the cache releases
        // mmaps correctly.
        let mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
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

        let mut keep = vec![];
        for (i, path) in paths.iter().enumerate() {
            keep.push(mmap_directory.open_read(path).unwrap());
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), i + 1);
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 0);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);
        for path in paths.iter() {
            let _r = mmap_directory.open_read(path).unwrap();
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), num_paths);
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 10);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);

        for path in paths.iter() {
            let _r = mmap_directory.open_read(path).unwrap();
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);
        }

        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);
        drop(keep);
        for path in paths.iter() {
            let _r = mmap_directory.open_read(path).unwrap();
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 1);
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 20);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);

        for path in &paths {
            mmap_directory.delete(path).unwrap();
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 20);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);
        for path in paths.iter() {
            assert!(mmap_directory.open_read(path).is_err());
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 30);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);
    }

    #[test]
    fn test_watch_wrapper() {
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let tmp_dirpath = tmp_dir.path().to_owned();
        let mut watch_wrapper = WatcherWrapper::new(&tmp_dirpath).unwrap();
        let tmp_file = tmp_dirpath.join(*META_FILEPATH);
        let _handle = watch_wrapper.watch(Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
        let (sender, receiver) = crossbeam::channel::unbounded();
        let _handle2 = watch_wrapper.watch(Box::new(move || {
            let _ = sender.send(());
        }));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        fs::write(&tmp_file, b"whateverwilldo").unwrap();
        assert!(receiver.recv().is_ok());
        assert!(counter.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn test_mmap_released() {
        let mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        let mut schema_builder: SchemaBuilder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        {
            let index = Index::create(mmap_directory.clone(), schema).unwrap();

            let mut index_writer = index.writer_for_tests().unwrap();
            let mut log_merge_policy = LogMergePolicy::default();
            log_merge_policy.set_min_merge_size(3);
            index_writer.set_merge_policy(Box::new(log_merge_policy));
            for _num_commits in 0..10 {
                for _ in 0..10 {
                    index_writer.add_document(doc!(text_field=>"abc"));
                }
                index_writer.commit().unwrap();
            }

            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .unwrap();

            for _ in 0..4 {
                index_writer.add_document(doc!(text_field=>"abc"));
                index_writer.commit().unwrap();
                reader.reload().unwrap();
            }
            index_writer.wait_merging_threads().unwrap();

            reader.reload().unwrap();
            let num_segments = reader.searcher().segment_readers().len();
            assert!(num_segments <= 4);
            assert_eq!(
                num_segments * 7,
                mmap_directory.get_cache_info().mmapped.len()
            );
        }
        assert!(mmap_directory.get_cache_info().mmapped.is_empty());
    }
}
