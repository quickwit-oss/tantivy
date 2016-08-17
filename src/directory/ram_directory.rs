use directory::{Directory, ReadOnlySource};
use std::io::{Cursor, Write, Seek, SeekFrom};
use std::io;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use directory::OpenError;
use directory::WritePtr;
use std::result;
use super::SharedVecSlice;
use Result;


struct VecWriter {
    path: PathBuf,
    shared_directory: InnerDirectory,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
}


impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed  {
            panic!("You forgot to flush {:?} before its writter got Drop. Do not rely on drop.", self.path)
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
        try!(self.data.write(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        self.shared_directory.write(self.path.clone(), self.data.get_ref())
    }
}

#[derive(Clone)]
struct InnerDirectory(Arc<RwLock<HashMap<PathBuf, Arc<Vec<u8>>>>>);

impl InnerDirectory {

    fn new() -> InnerDirectory {
        InnerDirectory(Arc::new(RwLock::new(HashMap::new())))
    }

    fn write(&self, path: PathBuf, data: &Vec<u8>) -> io::Result<()> {
        let mut map = try!(
            self.0
                .write()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, format!("Failed to lock the directory, when trying to write {:?}", path)))
        );
        map.insert(path, Arc::new(data.clone()));
        Ok(())
    }

    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenError> { 
        self.0
            .read()
            .map_err(|_| {
                    let io_err = io::Error::new(io::ErrorKind::Other, format!("Failed to read lock for the directory, when trying to read {:?}", path));
                    OpenError::IOError(io_err)
            })
            .and_then(|readable_map| {
                readable_map
                .get(path)
                .ok_or_else(|| OpenError::FileDoesNotExist(PathBuf::from(path)))
                .map(|data| {
                    ReadOnlySource::Anonymous(SharedVecSlice::new(data.clone()))
                })
            })
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
            fs: InnerDirectory::new()
        }
    }
}


pub struct RAMDirectory {
    fs: InnerDirectory,
}

impl Directory for RAMDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenError> {
        self.fs.open_read(path)
    }
    
    fn open_write(&mut self, path: &Path) -> Result<WritePtr> {
        let mut vec_writer = VecWriter {
            path: PathBuf::from(path),
            data: Cursor::new(Vec::new()),
            shared_directory: self.fs.clone(),
            is_flushed: false,
        };
        // force the creation of the file to mimick the MMap directory.
        try!(vec_writer.flush());
        Ok(Box::new(vec_writer))
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> Result<()> {
        let mut write = try!(self.open_write(path));
        try!(write.write_all(data));
        try!(write.flush());
        Ok(())
    }

    fn sync(&self, _: &Path) -> Result<()> {
        Ok(())
    }

    fn sync_directory(&self,) -> Result<()> {
        Ok(())
    }
}
