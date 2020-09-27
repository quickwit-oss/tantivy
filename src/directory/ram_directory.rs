use crate::core::META_FILEPATH;
use crate::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use crate::directory::AntiCallToken;
use crate::directory::WatchCallbackList;
use crate::directory::{Directory, ReadOnlySource, WatchCallback, WatchHandle};
use crate::directory::{TerminatingWrite, WritePtr};
use fail::fail_point;
use slog::{o, Drain, Logger};
use slog_stdlog::StdLog;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufWriter, Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::{Arc, RwLock};

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
        let mut fs = self.shared_directory.fs.inner_directory.write().unwrap();
        fs.write(self.path.clone(), self.data.get_ref());
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

struct InnerDirectory {
    fs: HashMap<PathBuf, ReadOnlySource>,
    watch_router: WatchCallbackList,
}

impl InnerDirectory {
    fn with_logger(logger: Logger) -> Self {
        InnerDirectory {
            fs: Default::default(),
            watch_router: WatchCallbackList::with_logger(logger.clone()),
        }
    }

    fn write(&mut self, path: PathBuf, data: &[u8]) -> bool {
        let data = ReadOnlySource::new(Vec::from(data));
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

impl Default for RAMDirectory {
    fn default() -> RAMDirectory {
        let logger = Logger::root(StdLog.fuse(), o!());
        Self::with_logger(logger)
    }
}

impl fmt::Debug for RAMDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RAMDirectory")
    }
}

struct Inner {
    inner_directory: RwLock<InnerDirectory>,
    logger: Logger,
}

/// A Directory storing everything in anonymous memory.
///
/// It is mainly meant for unit testing.
/// Writes are only made visible upon flushing.
///
#[derive(Clone)]
pub struct RAMDirectory {
    fs: Arc<Inner>,
}

impl RAMDirectory {
    /// Constructor
    pub fn create() -> RAMDirectory {
        Self::default()
    }

    /// Create a `RAMDirectory` with a custom logger.
    pub fn with_logger(logger: Logger) -> RAMDirectory {
        let inner_directory = InnerDirectory::with_logger(logger.clone()).into();
        RAMDirectory {
            fs: Arc::new(Inner {
                inner_directory,
                logger,
            }),
        }
    }

    /// Returns the sum of the size of the different files
    /// in the RAMDirectory.
    pub fn total_mem_usage(&self) -> usize {
        self.fs.inner_directory.read().unwrap().total_mem_usage()
    }

    /// Write a copy of all of the files saved in the RAMDirectory in the target `Directory`.
    ///
    /// Files are all written using the `Directory::write` meaning, even if they were
    /// written using the `atomic_write` api.
    ///
    /// If an error is encounterred, files may be persisted partially.
    pub fn persist(&self, dest: &mut dyn Directory) -> crate::Result<()> {
        let wlock = self.fs.inner_directory.write().unwrap();
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
        self.fs.inner_directory.read().unwrap().open_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        fail_point!("RAMDirectory::delete", |_| {
            Err(DeleteError::IOError {
                io_error: io::Error::from(io::ErrorKind::Other),
                filepath: path.to_path_buf(),
            })
        });
        self.fs.inner_directory.write().unwrap().delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.inner_directory.read().unwrap().exists(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let mut fs = self.fs.inner_directory.write().unwrap();
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
        self.fs
            .inner_directory
            .write()
            .unwrap()
            .write(path_buf.clone(), &[]);

        let mut vec_writer = VecWriter::new(path_buf, self.clone());
        vec_writer.write_all(data)?;
        vec_writer.flush()?;
        if path == Path::new(&*META_FILEPATH) {
            let _ = self
                .fs
                .inner_directory
                .write()
                .unwrap()
                .watch_router
                .broadcast();
        }
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        Ok(self
            .fs
            .inner_directory
            .write()
            .unwrap()
            .watch(watch_callback))
    }

    fn logger(&self) -> &Logger {
        &self.fs.logger
    }
}

#[cfg(test)]
mod tests {
    use super::RAMDirectory;
    use crate::Directory;
    use std::io::Write;
    use std::path::Path;

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
}
