pub mod searcher;
pub mod index;
mod segment_reader;
mod segment_id;
mod segment_component;
mod segment;
mod index_meta;
mod pool;
mod segment_meta;
mod term_iterator;

pub use self::searcher::Searcher;
pub use self::segment_component::SegmentComponent;
pub use self::segment_id::SegmentId;
pub use self::segment_reader::SegmentReader;
pub use self::segment::Segment;
pub use self::segment::SerializableSegment;
pub use self::index::Index;
pub use self::segment_meta::SegmentMeta;
pub use self::index_meta::IndexMeta;
pub use self::term_iterator::TermIterator;


use std::path::PathBuf;

lazy_static! {
    /// The meta file contains all the information about the list of segments and the schema
    /// of the index.
    pub static ref META_FILEPATH: PathBuf = PathBuf::from("meta.json");

    /// The managed file contains a list of files that were created by the tantivy
    /// and will therefore be garbage collected when they are deemed useless by tantivy.
    ///
    /// Removing this file is safe, but will prevent the garbage collection of all of the file that
    /// are currently in the directory
    pub static ref MANAGED_FILEPATH: PathBuf = PathBuf::from(".managed.json");

    /// Only one process should be able to write tantivy's index at a time.
    /// This file, when present, is in charge of preventing other processes to open an IndexWriter.
    ///
    /// If the process is killed and this file remains, it is safe to remove it manually.
    pub static ref LOCKFILE_FILEPATH: PathBuf = PathBuf::from(".tantivy-indexer.lock");
}
