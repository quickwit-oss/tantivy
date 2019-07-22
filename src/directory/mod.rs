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
use std::io::{BufWriter, Write};

#[cfg(feature = "mmap")]
pub use self::mmap_directory::MmapDirectory;

pub use self::managed_directory::ManagedDirectory;

/// Write object for Directory.
///
/// `WritePtr` are required to implement both Write
/// and Seek.
pub type WritePtr = BufWriter<Box<dyn Write>>;

#[cfg(test)]
mod tests;
