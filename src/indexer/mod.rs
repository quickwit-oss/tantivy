pub mod delete_queue;

mod doc_opstamp_mapping;
pub mod index_writer;
mod log_merge_policy;
mod merge_operation;
pub mod merge_policy;
pub mod merger;
pub mod operation;
mod prepared_commit;
mod segment_entry;
mod segment_manager;
mod segment_register;
pub mod segment_serializer;
pub mod segment_updater;
mod segment_writer;
mod stamper;

pub use self::index_writer::IndexWriter;
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_operation::{MergeOperation, MergeOperationInventory};
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub use self::segment_manager::SegmentManager;
pub use self::segment_serializer::SegmentSerializer;
pub use self::segment_writer::SegmentWriter;
pub use self::stamper::Opstamp;

/// Alias for the default merge policy, which is the `LogMergePolicy`.
pub type DefaultMergePolicy = LogMergePolicy;
