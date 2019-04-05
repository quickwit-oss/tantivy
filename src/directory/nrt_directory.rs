use directory::Directory;
use std::path::{PathBuf, Path};
use directory::ReadOnlySource;
use directory::error::OpenReadError;
use directory::error::DeleteError;
use std::io::{BufWriter, Cursor};
use directory::SeekableWrite;
use directory::error::OpenWriteError;
use directory::WatchHandle;
use directory::ram_directory::InnerRamDirectory;
use std::sync::RwLock;
use std::sync::Arc;
use directory::WatchCallback;
use std::fmt;
use std::io;
use std::io::{Seek, Write};
use directory::DirectoryClone;


const BUFFER_LEN: usize = 1_000_000;


pub enum NRTWriter {
    InRam {
        buffer: Cursor<Vec<u8>>,
        path: PathBuf,
        nrt_directory: NRTDirectory
    },
    UnderlyingFile(BufWriter<Box<SeekableWrite>>)
}

impl NRTWriter {
    pub fn new(path: PathBuf, nrt_directory: NRTDirectory) -> NRTWriter {
        NRTWriter::InRam {
            buffer: Cursor::new(Vec::with_capacity(BUFFER_LEN)),
            path,
            nrt_directory,
        }
    }
}

impl io::Seek for NRTWriter {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match self {
            NRTWriter::InRam { buffer, path, nrt_directory } => {
                buffer.seek(pos)
            }
            NRTWriter::UnderlyingFile(file) => {
                file.seek(pos)
            }
        }
    }
}

impl io::Write for NRTWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            NRTWriter::InRam { buffer, path, nrt_directory } => {
                let mut cache_wlock = nrt_directory.cache.write().unwrap();
                cache_wlock.write(path.clone(), buffer.get_ref());
                Ok(())
            }
            NRTWriter::UnderlyingFile(file) => {
                file.flush()
            }
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        // Working around the borrow checker.
        let mut underlying_write_opt: Option<BufWriter<Box<SeekableWrite>>> = None;
        if let NRTWriter::InRam { buffer, path, nrt_directory } = self {
            if buffer.get_ref().len() + buf.len() > BUFFER_LEN {
                // We can't keep this in RAM. Let's move it to the underlying directory.
                underlying_write_opt = Some(nrt_directory.open_write(path)
                    .map_err(|open_err| {
                        io::Error::new(io::ErrorKind::Other, open_err)
                    })?);

            }
        }
        if let Some(underlying_write) = underlying_write_opt {
            *self = NRTWriter::UnderlyingFile(underlying_write);
        }
        match self {
            NRTWriter::InRam { buffer, path, nrt_directory } => {
                assert!(buffer.get_ref().len() + buf.len() <= BUFFER_LEN);
                buffer.write_all(buf)
            }
            NRTWriter::UnderlyingFile(file) => {
                file.write_all(buf)
            }
        }
    }
}

pub struct NRTDirectory {
    underlying: Box<Directory>,
    cache: Arc<RwLock<InnerRamDirectory>>,
}


impl Clone for NRTDirectory {
    fn clone(&self) -> Self {
        NRTDirectory {
            underlying: self.underlying.box_clone(),
            cache: self.cache.clone()
        }
    }
}

impl NRTDirectory {
    fn wrap(underlying: Box<Directory>) -> NRTDirectory {
        NRTDirectory {
            underlying,
            cache: Default::default()
        }
    }
}

impl fmt::Debug for NRTDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NRTDirectory({:?})", self.underlying)
    }
}

impl Directory for NRTDirectory {
    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        unimplemented!()
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        // We explicitly release the lock, to prevent a panic on the underlying directory
        // to poison the lock.
        //
        // File can only go from cache to underlying so the result does not lead to
        // any inconsistency.
        {
            let mut cache_wlock = self.cache.write().unwrap();
            if cache_wlock.exists(path) {
                return cache_wlock.delete(path);
            }
        }
        self.underlying.delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        // We explicitly release the lock, to prevent a panic on the underlying directory
        // to poison the lock.
        //
        // File can only go from cache to underlying so the result does not lead to
        // any inconsistency.
        {
            let rlock_cache = self.cache.read().unwrap();
            if rlock_cache.exists(path) {
                return true;
            }
        }
        self.underlying.exists(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<BufWriter<Box<SeekableWrite>>, OpenWriteError> {
        let mut cache_wlock = self.cache.write().unwrap();
        // TODO might poison our lock. I don't know have a sound solution yet.
        let path_buf = path.to_owned();
        if self.underlying.exists(path) {
            return Err(OpenWriteError::FileAlreadyExists(path_buf));
        }
        let exists = cache_wlock.write(path_buf.clone(), &[]);
        // force the creation of the file to mimic the MMap directory.
        if exists {
            Err(OpenWriteError::FileAlreadyExists(path_buf))
        } else {
            let vec_writer = NRTWriter::new(path_buf.clone(), self.clone());
            Ok(BufWriter::new(Box::new(vec_writer)))
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.underlying.atomic_read(path)
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.underlying.atomic_write(path, data)
    }

    fn watch(&self, watch_callback: WatchCallback) -> WatchHandle {
        self.underlying.watch(watch_callback)
    }
}