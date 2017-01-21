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

use std::path::PathBuf;

pub use self::segment_component::SegmentComponent;
pub use self::segment_id::SegmentId;
pub use self::segment_reader::SegmentReader;
pub use self::segment::Segment;
pub use self::segment::SegmentInfo;
pub use self::segment::SerializableSegment;
pub use self::index::Index;
pub use self::segment_meta::SegmentMeta;
pub use self::index_meta::IndexMeta;
pub use self::term_iterator::TermIterator;

lazy_static! {
    pub static ref META_FILEPATH: PathBuf = PathBuf::from("meta.json");
}