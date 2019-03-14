use directory::error::{DeleteError, IOError, OpenReadError, OpenWriteError};
use directory::WritePtr;
use directory::{Directory, ReadOnlySource, WatchHandle, WatchCallback};
use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufWriter, Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::{Arc, RwLock};
use directory::WatchEventRouter;

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
        fs.write(self.path.clone(), self.data.get_ref())?;
        let path_clone = self.path.clone();
        fs.watch_router.broadcast(&path_clone);
        Ok(())
    }
}

#[derive(Default)]
struct InnerDirectory {
    fs: HashMap<PathBuf, ReadOnlySource>,
    watch_router: WatchEventRouter
}

impl InnerDirectory {
    // TODO Result is now useless
    fn write(&mut self, path: PathBuf, data: &[u8]) -> io::Result<bool> {
        let prev_value = self.fs.insert(path, ReadOnlySource::new(Vec::from(data)));
        Ok(prev_value.is_some())
    }

    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        self.fs
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(PathBuf::from(path)))
            .map(|el| el.clone())
    }

    fn delete(&mut self, path: &Path) -> result::Result<(), DeleteError> {
        match self.fs.remove(path) {
            Some(_) => {
                self.watch_router.broadcast(path);
                Ok(())
            }
            None => Err(DeleteError::FileDoesNotExist(PathBuf::from(path))),
        }
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.contains_key(path)
    }

    fn watch(&mut self, path: &Path, watch_handle: WatchCallback) -> WatchHandle {
        self.watch_router.subscribe(path, watch_handle)
    }
}

impl fmt::Debug for RAMDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
    /// Constructor
    pub fn create() -> RAMDirectory {
        Self::default()
    }
}

impl Directory for RAMDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        self.fs.read().unwrap().open_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.fs.write().unwrap().delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.read().unwrap().exists(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let mut fs = self.fs.write().unwrap();
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(


            path_buf.clone(), self.clone());
        let exists = fs
            .write(path_buf.clone(), &Vec::new())
            .map_err(|err| IOError::with_path(path.to_owned(), err))?;
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
            msg.unwrap_or("Undefined".to_string())
        )));
        let path_buf = PathBuf::from(path);
        {   // Reserve the path to prevent calls to .write() to succeed.
            let mut inner = self.fs.write().unwrap();
            inner.write(path_buf.clone(), &Vec::new())?;
        }
        let mut vec_writer = VecWriter::new(path_buf.clone(), self.clone());
        vec_writer.write_all(data)?;
        vec_writer.flush()?;
        Ok(())
    }

    fn watch(&self, path: &Path, watch_callback: WatchCallback) -> WatchHandle {
        self.fs.write().unwrap().watch(path, watch_callback)
    }

}
