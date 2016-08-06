use std::marker::Send;
use std::marker::Sync;
use std::fmt;
use std::path::Path;
use directory::{ReadOnlySource, WritePtr, OpenError};
use std::result;
use Result;


/// There is currently two implementations of `Directory`
///     - [RAMDirectory](index.html) 
/// 
pub trait Directory: fmt::Debug + Send + Sync {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenError>;
    fn open_write(&mut self, path: &Path) -> Result<WritePtr>;
    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> Result<()>;
    
    /// Syncs the file if it exists 
    /// If it does not exists, just return Ok(())
    // TODO Change the API
    fn sync(&self, path: &Path) -> Result<()>;
    fn sync_directory(&self,) -> Result<()>;
}
