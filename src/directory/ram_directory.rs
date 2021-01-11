use crate::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use crate::directory::AntiCallToken;
use crate::directory::WatchCallbackList;
use crate::directory::{Directory, FileSlice, WatchCallback, WatchHandle};
use crate::directory::{TerminatingWrite, WritePtr};
use crate::{common::HasLen, core::META_FILEPATH};
use fail::fail_point;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufWriter, Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::{Arc, RwLock};

use super::FileHandle;

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
    fs: HashMap<PathBuf, FileSlice>,
    watch_router: WatchCallbackList,
}

impl InnerDirectory {
    fn write(&mut self, path: PathBuf, data: &[u8]) -> bool {
        let data = FileSlice::from(data.to_vec());
        self.fs.insert(path, data).is_some()
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
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
    /// Constructor
    pub fn create() -> RAMDirectory {
        Self::default()
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
    pub fn persist(&self, dest: &dyn Directory) -> crate::Result<()> {
        let wlock = self.fs.write().unwrap();
        for (path, file) in wlock.fs.iter() {
            let mut dest_wrt = dest.open_write(path)?;
            dest_wrt.write_all(file.read_bytes()?.as_slice())?;
            dest_wrt.terminate()?;
        }
        Ok(())
    }
}

impl Directory for RAMDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Box::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        self.fs.read().unwrap().open_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        fail_point!("RAMDirectory::delete", |_| {
            Err(DeleteError::IOError {
                io_error: io::Error::from(io::ErrorKind::Other),
                filepath: path.to_path_buf(),
            })
        });
        self.fs.write().unwrap().delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self
            .fs
            .read()
            .map_err(|e| OpenReadError::IOError {
                io_error: io::Error::new(io::ErrorKind::Other, e.to_string()),
                filepath: path.to_path_buf(),
            })?
            .exists(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
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
        let bytes =
            self.open_read(path)?
                .read_bytes()
                .map_err(|io_error| OpenReadError::IOError {
                    io_error,
                    filepath: path.to_path_buf(),
                })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        fail_point!("RAMDirectory::atomic_write", |msg| Err(io::Error::new(
            io::ErrorKind::Other,
            msg.unwrap_or_else(|| "Undefined".to_string())
        )));
        let path_buf = PathBuf::from(path);

        self.fs.write().unwrap().write(path_buf, data);

        if path == *META_FILEPATH {
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

    #[test]
    fn test_persist() {
        let msg_atomic: &'static [u8] = b"atomic is the way";
        let msg_seq: &'static [u8] = b"sequential is the way";
        let path_atomic: &'static Path = Path::new("atomic");
        let path_seq: &'static Path = Path::new("seq");
        let directory = RAMDirectory::create();
        assert!(directory.atomic_write(path_atomic, msg_atomic).is_ok());
        let mut wrt = directory.open_write(path_seq).unwrap();
        assert!(wrt.write_all(msg_seq).is_ok());
        assert!(wrt.flush().is_ok());
        let directory_copy = RAMDirectory::create();
        assert!(directory.persist(&directory_copy).is_ok());
        assert_eq!(directory_copy.atomic_read(path_atomic).unwrap(), msg_atomic);
        assert_eq!(directory_copy.atomic_read(path_seq).unwrap(), msg_seq);
    }
}
