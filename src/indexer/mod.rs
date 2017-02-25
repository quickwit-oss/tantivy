pub mod index_writer;
pub mod segment_serializer;
pub mod merger;
mod merge_policy;
mod log_merge_policy;
mod segment_register;
mod segment_writer;
mod segment_manager;
pub mod delete_queue;
pub mod segment_updater;
mod directory_lock;
mod segment_entry;
mod doc_opstamp_mapping;
pub mod operation;


// TODO avoid exposing SegmentState / SegmentEntry if it does not have to be public API

pub use self::segment_entry::{SegmentEntry, SegmentState};
pub use self::segment_serializer::SegmentSerializer;
pub use self::segment_writer::SegmentWriter;
pub use self::index_writer::IndexWriter;
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_policy::{NoMergePolicy, MergeCandidate, MergePolicy};
pub use self::segment_manager::SegmentManager;

/// Alias for the default merge policy, which is the LogMergePolicy.
pub type DefaultMergePolicy = LogMergePolicy;
