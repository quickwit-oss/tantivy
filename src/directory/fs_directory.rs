use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use crate::{
    directory::{error::OpenWriteError, FileHandle, OwnedBytes, TerminatingWrite, WatchHandle},
    Directory, HasLen,
};

use super::{
    error::{DeleteError, OpenReadError},
    AntiCallToken, WatchCallback, WritePtr,
};

#[derive(Debug, Clone)]
pub struct FsDirectory {
    root: PathBuf,
}

impl FsDirectory {
    pub fn new(path: &Path) -> FsDirectory {
        FsDirectory {
            root: path.to_path_buf(),
        }
    }
}

struct Noop {}
impl Write for Noop {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl TerminatingWrite for Noop {
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        Ok(())
    }
}
impl Directory for FsDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        Ok(Box::new(FSFile::new(&self.root.join(path))))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        println!("delete {:?}", path);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        todo!()
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Ok(BufWriter::new(Box::new(Noop {})))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let path = self.root.join(path);
        println!("atomic_read {:?}", path);
        Ok(std::fs::read(path).unwrap())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        todo!()
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[derive(Debug)]
struct FSFile {
    path: PathBuf,
    file: Arc<RwLock<File>>,
    len: usize,
    cache: RwLock<BTreeMap<usize, Vec<u8>>>,
}
const CS: usize = 4096;

impl FSFile {
    pub fn new(path: &Path) -> FSFile {
        let mut f = File::open(path).unwrap();
        let len = f.seek(SeekFrom::End(0)).unwrap();
        FSFile {
            path: path.to_path_buf(),
            file: Arc::new(RwLock::new(f)),
            len: len as usize,
            cache: RwLock::new(BTreeMap::new()),
        }
    }
    fn read_bytes_real(&self, from: usize, to: usize) -> Vec<u8> {
        let len = to - from;

        eprintln!(
            "READ {} @ {}, len {}",
            self.path.to_string_lossy(),
            from,
            len
        );
        if len == 51616 {
            println!("{:?}", backtrace::Backtrace::new());
        }
        if len > 1_000_000 {
            println!("{:?}", backtrace::Backtrace::new());
        }
        if len > 2_000_000 {
            panic!("tried to read too much");
        }
        let mut f = self.file.write().unwrap();
        f.seek(SeekFrom::Start(from as u64)).unwrap();
        let mut buf = Vec::with_capacity(len);
        let flonk = f.deref_mut();
        (flonk).take(len as u64).read_to_end(&mut buf).unwrap();
        return buf;
    }
}
impl FileHandle for FSFile {
    fn read_bytes(&self, from: usize, to: usize) -> std::io::Result<OwnedBytes> {
        let len = to - from;
        eprintln!(
            "GET {} @ {}, len {}",
            self.path.to_string_lossy(),
            from,
            len
        );
        let starti = from / CS;
        let endi = to / CS;
        let startofs = from % CS;
        let endofs = to % CS;
        let mut out_buf = vec![0u8; len];
        //let toget = vec![];
        let mut cache = self.cache.write().unwrap();
        let mut written = 0;
        for i in starti..=endi {
            let startofs = if i == starti { startofs } else { 0 };
            let endofs = if i == endi { endofs } else { CS };
            let chunk = cache.entry(i).or_insert_with(|| {
                self.read_bytes_real(i * CS, std::cmp::min((i + 1) * CS, self.len()))
            });
            let chunk = &chunk[startofs..endofs];
            println!("{} {} {} {}", out_buf.len(), startofs, endofs, chunk.len());
            let write_len = std::cmp::min(chunk.len(), len);
            out_buf[written..written + write_len]
                .copy_from_slice(&chunk);
            written += write_len;
        }

        Ok(OwnedBytes::new(out_buf))
    }
}
impl HasLen for FSFile {
    fn len(&self) -> usize {
        self.len
    }
}
