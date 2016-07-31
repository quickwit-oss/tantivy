use std::marker::Send;
use std::marker::Sync;
use std::io;
use std::fmt;
use std::path::Path;
use directory::{ReadOnlySource, WritePtr};

pub trait Directory: fmt::Debug + Send + Sync {
    fn open_read(&self, path: &Path) -> io::Result<ReadOnlySource>;
    fn open_write(&mut self, path: &Path) -> io::Result<WritePtr>;
    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()>;
    fn sync(&self, path: &Path) -> io::Result<()>;
    fn sync_directory(&self,) -> io::Result<()>;
}
