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
pub use self::merge_operation::MergeOperation;
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub use self::segment_manager::SegmentManager;
pub use self::segment_serializer::SegmentSerializer;
pub use self::segment_updater::merge_segments;
pub use self::segment_writer::SegmentWriter;

/// Alias for the default merge policy, which is the `LogMergePolicy`.
pub type DefaultMergePolicy = LogMergePolicy;

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap {
    use crate::schema::{self, Schema};
    use crate::{Index, Term};

    #[test]
    fn test_advance_delete_bug() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", schema::TEXT);
        let index = Index::create_from_tempdir(schema_builder.build()).unwrap();
        let mut index_writer = index.writer_for_tests().unwrap();
        // there must be one deleted document in the segment
        index_writer.add_document(doc!(text_field=>"b"));
        index_writer.delete_term(Term::from_field_text(text_field, "b"));
        // we need enough data to trigger the bug (at least 32 documents)
        for _ in 0..32 {
            index_writer.add_document(doc!(text_field=>"c"));
        }
        index_writer.commit().unwrap();
        index_writer.commit().unwrap();
    }
}
