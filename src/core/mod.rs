pub mod index;
mod index_meta;
mod inverted_index_reader;
mod pool;
pub mod searcher;
mod segment;
mod segment_component;
mod segment_id;
mod segment_meta;
mod segment_reader;

pub use self::index::Index;
pub use self::index_meta::IndexMeta;
pub use self::inverted_index_reader::InvertedIndexReader;
pub use self::searcher::Searcher;
pub use self::segment::Segment;
pub use self::segment::SerializableSegment;
pub use self::segment_component::SegmentComponent;
pub use self::segment_id::SegmentId;
pub use self::segment_meta::SegmentMeta;
pub use self::segment_reader::SegmentReader;

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
}
