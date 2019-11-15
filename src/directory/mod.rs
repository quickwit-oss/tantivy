/*!

WORM directory abstraction.

*/

#[cfg(feature = "mmap")]
mod mmap_directory;

mod directory;
mod directory_lock;
mod footer;
mod managed_directory;
mod ram_directory;
mod read_only_source;
mod watch_event_router;

/// Errors specific to the directory module.
pub mod error;

pub use self::directory::DirectoryLock;
pub use self::directory::{Directory, DirectoryClone};
pub use self::directory_lock::{Lock, INDEX_WRITER_LOCK, META_LOCK};
pub use self::ram_directory::RAMDirectory;
pub use self::read_only_source::ReadOnlySource;
pub(crate) use self::watch_event_router::WatchCallbackList;
pub use self::watch_event_router::{WatchCallback, WatchHandle};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

/// Outcome of the Garbage collection
pub struct GarbageCollectionResult {
    /// List of files that were deleted in this cycle
    pub deleted_files: Vec<PathBuf>,
    /// List of files that were schedule to be deleted in this cycle,
    /// but deletion did not work. This typically happens on windows,
    /// as deleting a memory mapped file is forbidden.
    ///
    /// If a searcher is still held, a file cannot be deleted.
    /// This is not considered a bug, the file will simply be deleted
    /// in the next GC.
    pub failed_to_delete_files: Vec<PathBuf>,
}

#[cfg(feature = "mmap")]
pub use self::mmap_directory::MmapDirectory;

pub use self::managed_directory::ManagedDirectory;

/// Struct used to prevent from calling [`terminate_ref`](trait.TerminatingWrite#method.terminate_ref) directly
pub struct AntiCallToken(());

/// Trait used to indicate when no more write need to be done on a writer
pub trait TerminatingWrite: Write {
    /// Indicate that the writer will no longer be used. Internally call terminate_ref.
    fn terminate(mut self) -> io::Result<()>
    where
        Self: Sized,
    {
        self.terminate_ref(AntiCallToken(()))
    }

    /// You should implement this function to define custom behavior.
    /// This function should flush any buffer it may hold.
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()>;
}

impl<W: TerminatingWrite + ?Sized> TerminatingWrite for Box<W> {
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.as_mut().terminate_ref(token)
    }
}

impl<W: TerminatingWrite> TerminatingWrite for BufWriter<W> {
    fn terminate_ref(&mut self, a: AntiCallToken) -> io::Result<()> {
        self.flush()?;
        self.get_mut().terminate_ref(a)
    }
}

/// Write object for Directory.
///
/// `WritePtr` are required to implement both Write
/// and Seek.
pub type WritePtr = BufWriter<Box<dyn TerminatingWrite>>;

#[cfg(test)]
mod tests;
