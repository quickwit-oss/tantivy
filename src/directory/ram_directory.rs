use super::shared_vec_slice::SharedVecSlice;
use common::make_io_err;
use directory::error::{DeleteError, IOError, OpenReadError, OpenWriteError};
use directory::WritePtr;
use directory::{Directory, ReadOnlySource};
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
    shared_directory: InnerDirectory,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf, shared_directory: InnerDirectory) -> VecWriter {
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
        self.shared_directory
            .write(self.path.clone(), self.data.get_ref())?;
        Ok(())
    }
}

#[derive(Clone)]
struct InnerDirectory(Arc<RwLock<HashMap<PathBuf, Arc<Vec<u8>>>>>);

impl InnerDirectory {
    fn new() -> InnerDirectory {
        InnerDirectory(Arc::new(RwLock::new(HashMap::new())))
    }

    fn write(&self, path: PathBuf, data: &[u8]) -> io::Result<bool> {
        let mut map = self.0.write().map_err(|_| {
            make_io_err(format!(
                "Failed to lock the directory, when trying to write {:?}",
                path
            ))
        })?;
        let prev_value = map.insert(path, Arc::new(Vec::from(data)));
        Ok(prev_value.is_some())
    }

    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        self.0
            .read()
            .map_err(|_| {
                let msg = format!(
                    "Failed to acquire read lock for the \
                     directory when trying to read {:?}",
                    path
                );
                let io_err = make_io_err(msg);
                OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
            })
            .and_then(|readable_map| {
                readable_map
                    .get(path)
                    .ok_or_else(|| OpenReadError::FileDoesNotExist(PathBuf::from(path)))
                    .map(Arc::clone)
                    .map(|data| ReadOnlySource::Anonymous(SharedVecSlice::new(data)))
            })
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.0
            .write()
            .map_err(|_| {
                let msg = format!(
                    "Failed to acquire write lock for the \
                     directory when trying to delete {:?}",
                    path
                );
                let io_err = make_io_err(msg);
                DeleteError::IOError(IOError::with_path(path.to_owned(), io_err))
            })
            .and_then(|mut writable_map| match writable_map.remove(path) {
                Some(_) => Ok(()),
                None => Err(DeleteError::FileDoesNotExist(PathBuf::from(path))),
            })
    }

    fn exists(&self, path: &Path) -> bool {
        self.0
            .read()
            .expect("Failed to get read lock directory.")
            .contains_key(path)
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
#[derive(Clone)]
pub struct RAMDirectory {
    fs: InnerDirectory,
}

impl RAMDirectory {
    /// Constructor
    pub fn create() -> RAMDirectory {
        RAMDirectory {
            fs: InnerDirectory::new(),
        }
    }
}

impl Directory for RAMDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        self.fs.open_read(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf.clone(), self.fs.clone());

        let exists = self
            .fs
            .write(path_buf.clone(), &Vec::new())
            .map_err(|err| IOError::with_path(path.to_owned(), err))?;

        // force the creation of the file to mimic the MMap directory.
        if exists {
            Err(OpenWriteError::FileAlreadyExists(path_buf))
        } else {
            Ok(BufWriter::new(Box::new(vec_writer)))
        }
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.fs.delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.exists(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let read = self.open_read(path)?;
        Ok(read.as_slice().to_owned())
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        let path_buf = PathBuf::from(path);
        let mut vec_writer = VecWriter::new(path_buf.clone(), self.fs.clone());
        self.fs.write(path_buf, &Vec::new())?;
        vec_writer.write_all(data)?;
        vec_writer.flush()?;
        Ok(())
    }

    fn box_clone(&self) -> Box<Directory> {
        Box::new(self.clone())
    }
}
