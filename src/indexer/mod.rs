pub mod delete_queue;

pub mod demuxer;
pub mod doc_id_mapping;
mod doc_opstamp_mapping;
pub mod index_writer;
mod index_writer_status;
mod json_term_writer;
mod log_merge_policy;
mod merge_operation;
pub mod merge_policy;
pub mod merger;
mod merger_sorted_index_test;
pub mod operation;
pub mod prepared_commit;
mod segment_entry;
mod segment_manager;
mod segment_register;
pub mod segment_serializer;
pub mod segment_updater;
mod segment_writer;
mod stamper;

use crossbeam::channel;
use smallvec::SmallVec;

pub use self::index_writer::IndexWriter;
pub(crate) use self::json_term_writer::JsonTermWriter;
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_operation::MergeOperation;
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub use self::segment_manager::SegmentManager;
pub use self::segment_serializer::SegmentSerializer;
pub use self::segment_updater::{merge_filtered_segments, merge_indices};
pub use self::segment_writer::SegmentWriter;
use crate::indexer::operation::AddOperation;

/// Alias for the default merge policy, which is the `LogMergePolicy`.
pub type DefaultMergePolicy = LogMergePolicy;

// Batch of documents.
// Most of the time, users will send operation one-by-one, but it can be useful to
// send them as a small block to ensure that
// - all docs in the operation will happen on the same segment and continuous doc_ids.
// - all operations in the group are committed at the same time, making the group
// atomic.
type AddBatch = SmallVec<[AddOperation; 4]>;
type AddBatchSender = channel::Sender<AddBatch>;
type AddBatchReceiver = channel::Receiver<AddBatch>;

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap {
    use crate::schema::{self, Schema};
    use crate::{Index, Term};

    #[test]
    fn test_advance_delete_bug() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_from_tempdir(schema_builder.build())?;
        let mut index_writer = index.writer_for_tests()?;
        // there must be one deleted document in the segment
        index_writer.add_document(doc!(text_field=>"b"))?;
        index_writer.delete_term(Term::from_field_text(text_field, "b"));
        // we need enough data to trigger the bug (at least 32 documents)
        for _ in 0..32 {
            index_writer.add_document(doc!(text_field=>"c"))?;
        }
        index_writer.commit()?;
        index_writer.commit()?;
        Ok(())
    }
}
