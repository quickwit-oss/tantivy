/*!

WORM directory abstraction.

*/

#[cfg(feature = "mmap")]
mod mmap_directory;

mod directory;
mod directory_lock;
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
