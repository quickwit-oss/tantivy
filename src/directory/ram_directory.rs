use crate::core::META_FILEPATH;
use crate::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use crate::directory::AntiCallToken;
use crate::directory::WatchCallbackList;
use crate::directory::{Directory, ReadOnlySource, WatchCallback, WatchHandle};
use crate::directory::{TerminatingWrite, WritePtr};
use fail::fail_point;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufWriter, Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::{Arc, RwLock};
use crate::indexer::ResourceManager;

/// Writer associated with the `RAMDirectory`
///
/// The Writer just writes a buffer.
///
/// # Panics
///
/// On drop, if the writer was left in a *dirty* state.
/// That is, if flush was not called after the last call
/// to write.
///
struct VecWriter {
    path: PathBuf,
    shared_directory: RAMDirectory,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf, shared_directory: RAMDirectory) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: Cursor::new(Vec::new()),
            shared_directory,
            is_flushed: true,
        }
    }
}

impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            panic!(
                "You forgot to flush {:?} before its writter got Drop. Do not rely on drop.",
                self.path
            )
        }
    }
}

impl Seek for VecWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        let mut fs = self.shared_directory.fs.write().unwrap();
        fs.write(self.path.clone(), self.data.get_ref());
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

#[derive(Default)]
struct InnerDirectory {
    fs: HashMap<PathBuf, ReadOnlySource>,
    watch_router: WatchCallbackList,
    memory_manager: ResourceManager,
}

impl InnerDirectory {
    fn write(&mut self, path: PathBuf, data: &[u8]) -> bool {
        let data = ReadOnlySource::new_with_allocation(Vec::from(data), &self.memory_manager);
        self.fs.insert(path, data).is_some()
    }

    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        self.fs
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(PathBuf::from(path)))
            .map(Clone::clone)
    }

    fn delete(&mut self, path: &Path) -> result::Result<(), DeleteError> {
        match self.fs.remove(path) {
            Some(_) => Ok(()),
            None => Err(DeleteError::FileDoesNotExist(PathBuf::from(path))),
        }
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.contains_key(path)
    }

    fn watch(&mut self, watch_handle: WatchCallback) -> WatchHandle {
        self.watch_router.subscribe(watch_handle)
    }

    fn total_mem_usage(&self) -> usize {
        self.fs.values().map(|f| f.len()).sum()
    }
}

impl fmt::Debug for RAMDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RAMDirectory")
    }
}

/// A Directory storing everything in anonymous memory.
///
/// It is mainly meant for unit testing.
/// Writes are only made visible upon flushing.
///
#[derive(Clone, Default)]
pub struct RAMDirectory {
    fs: Arc<RwLock<InnerDirectory>>,
}

impl RAMDirectory {

    pub fn create() -> RAMDirectory {
        RAMDirectory::default()
    }

    /// Constructor
    pub fn create_with_memory_manager(memory_manager: ResourceManager) -> RAMDirectory {
        let inner_directory = InnerDirectory {
            fs: Default::default(),
            watch_router: Default::default(),
            memory_manager
        };
        RAMDirectory {
            fs: Arc::new(RwLock::new(inner_directory))
        }
    }

    /// Returns the sum of the size of the different files
    /// in the RAMDirectory.
    pub fn total_mem_usage(&self) -> usize {
        self.fs.read().unwrap().total_mem_usage()
    }

    /// Write a copy of all of the files saved in the RAMDirectory in the target `Directory`.
    ///
    /// Files are all written using the `Directory::write` meaning, even if they were
    /// written using the `atomic_write` api.
    ///
    /// If an error is encounterred, files may be persisted partially.
    pub fn persist(&self, dest: &mut dyn Directory) -> crate::Result<()> {
        let wlock = self.fs.write().unwrap();
        for (path, source) in wlock.fs.iter() {
            let mut dest_wrt = dest.open_write(path)?;
            dest_wrt.write_all(source.as_slice())?;
            dest_wrt.terminate()?;
        }
        Ok(())
    }
}

impl Directory for RAMDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        self.fs.read().unwrap().open_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        fail_point!("RAMDirectory::delete", |_| {
            use crate::directory::error::IOError;
            let io_error = IOError::from(io::Error::from(io::ErrorKind::Other));
            Err(DeleteError::from(io_error))
        });
        self.fs.write().unwrap().delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.read().unwrap().exists(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let mut fs = self.fs.write().unwrap();
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf.clone(), self.clone());
        let exists = fs.write(path_buf.clone(), &[]);
        // force the creation of the file to mimic the MMap directory.
        if exists {
            Err(OpenWriteError::FileAlreadyExists(path_buf))
        } else {
            Ok(BufWriter::new(Box::new(vec_writer)))
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        Ok(self.open_read(path)?.as_slice().to_owned())
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        fail_point!("RAMDirectory::atomic_write", |msg| Err(io::Error::new(
            io::ErrorKind::Other,
            msg.unwrap_or_else(|| "Undefined".to_string())
        )));
        let path_buf = PathBuf::from(path);

        // Reserve the path to prevent calls to .write() to succeed.
        self.fs.write().unwrap().write(path_buf.clone(), &[]);

        let mut vec_writer = VecWriter::new(path_buf, self.clone());
        vec_writer.write_all(data)?;
        vec_writer.flush()?;
        if path == Path::new(&*META_FILEPATH) {
            let _ = self.fs.write().unwrap().watch_router.broadcast();
        }
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        Ok(self.fs.write().unwrap().watch(watch_callback))
    }
}

#[cfg(test)]
mod tests {
    use super::RAMDirectory;
    use crate::Directory;
    use std::io::Write;
    use std::path::Path;
    use crate::indexer::ResourceManager;
    use crate::directory::TerminatingWrite;
    use std::mem;

    #[test]
    fn test_persist() {
        let msg_atomic: &'static [u8] = b"atomic is the way";
        let msg_seq: &'static [u8] = b"sequential is the way";
        let path_atomic: &'static Path = Path::new("atomic");
        let path_seq: &'static Path = Path::new("seq");
        let mut directory = RAMDirectory::create();
        assert!(directory.atomic_write(path_atomic, msg_atomic).is_ok());
        let mut wrt = directory.open_write(path_seq).unwrap();
        assert!(wrt.write_all(msg_seq).is_ok());
        assert!(wrt.flush().is_ok());
        let mut directory_copy = RAMDirectory::create();
        assert!(directory.persist(&mut directory_copy).is_ok());
        assert_eq!(directory_copy.atomic_read(path_atomic).unwrap(), msg_atomic);
        assert_eq!(directory_copy.atomic_read(path_seq).unwrap(), msg_seq);
    }

    #[test]
    fn test_memory_manager_several_path() {
        let memory_manager = ResourceManager::default();
        let mut ram_directory = RAMDirectory::create_with_memory_manager(memory_manager.clone());
        assert!(ram_directory.atomic_write(Path::new("/titi"), b"abcd").is_ok());
        assert_eq!(memory_manager.total_amount(), 4u64);
        assert!(ram_directory.atomic_write(Path::new("/toto"), b"abcde").is_ok());
        assert_eq!(memory_manager.total_amount(), 9u64);
    }

    #[test]
    fn test_memory_manager_override() {
        let memory_manager = ResourceManager::default();
        let mut ram_directory = RAMDirectory::create_with_memory_manager(memory_manager.clone());
        assert!(ram_directory.atomic_write(Path::new("/titi"), b"abcde").is_ok());
        assert_eq!(memory_manager.total_amount(), 5u64);
        assert!(ram_directory.atomic_write(Path::new("/titi"), b"abcdef").is_ok());
        assert_eq!(memory_manager.total_amount(), 6u64);
    }

    #[test]
    fn test_memory_manager_seq_wrt() {
        let memory_manager = ResourceManager::default();
        let mut ram_directory = RAMDirectory::create_with_memory_manager(memory_manager.clone());
        let mut wrt = ram_directory.open_write(Path::new("/titi")).unwrap();
        assert!(wrt.write_all(b"abcde").is_ok());
        assert!(wrt.terminate().is_ok());
        assert_eq!(memory_manager.total_amount(), 5u64);
        assert!(ram_directory.atomic_write(Path::new("/titi"), b"abcdef").is_ok());
        assert_eq!(memory_manager.total_amount(), 6u64);
    }

    #[test]
    fn test_release_on_drop() {
        let memory_manager = ResourceManager::default();
        let mut ram_directory = RAMDirectory::create_with_memory_manager(memory_manager.clone());
        let mut wrt = ram_directory.open_write(Path::new("/titi")).unwrap();
        assert!(wrt.write_all(b"abcde").is_ok());
        assert!(wrt.terminate().is_ok());
        assert_eq!(memory_manager.total_amount(), 5u64);
        let mut wrt2 = ram_directory.open_write(Path::new("/toto")).unwrap();
        assert!(wrt2.write_all(b"abcdefghijkl").is_ok());
        assert!(wrt2.terminate().is_ok());
        assert_eq!(memory_manager.total_amount(), 17u64);
        let source = ram_directory.open_read(Path::new("/titi")).unwrap();
        let source_clone = source.clone();
        assert_eq!(memory_manager.total_amount(), 17u64);
        mem::drop(ram_directory);
        assert_eq!(memory_manager.total_amount(), 5u64);
        mem::drop(source);
        assert_eq!(memory_manager.total_amount(), 5u64);
        mem::drop(source_clone);
        assert_eq!(memory_manager.total_amount(), 0u64);
    }
}
