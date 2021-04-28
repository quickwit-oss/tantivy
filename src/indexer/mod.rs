pub mod delete_queue;

mod doc_opstamp_mapping;
pub mod index_sorter;
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

#[cfg(test)]
mod tests_indexsorting {
    use crate::schema::Schema;
    use crate::schema::*;
    use crate::{Index, IndexSettings, IndexSortByField, Order};

    #[test]
    fn test_sort_index() {
        let mut schema_builder = Schema::builder();
        let my_number = schema_builder.add_u64_field(
            "my_number",
            IntOptions::default().set_fast(Cardinality::SingleValue),
        );
        let multi_numbers = schema_builder.add_u64_field(
            "multi_numbers",
            IntOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let settings = IndexSettings {
            sort_by_field: IndexSortByField {
                field: "my_number".to_string(),
                order: Order::Asc,
            },
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()
            .unwrap();

        assert_eq!(
            index.settings().as_ref().unwrap().sort_by_field.field,
            "my_number".to_string()
        );
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(my_number=>2_u64, multi_numbers => 5_u64, multi_numbers => 6_u64));
        index_writer.add_document(doc!(my_number=>1_u64));
        index_writer.add_document(doc!(my_number=>3_u64, multi_numbers => 3_u64));
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();

        let fast_field = fast_fields.u64(my_number).unwrap();
        assert_eq!(fast_field.get(0u32), 1u64);
        assert_eq!(fast_field.get(1u32), 2u64);
        assert_eq!(fast_field.get(2u32), 3u64);

        let multifield = fast_fields.u64s(multi_numbers).unwrap();
        let mut vals = vec![];
        multifield.get_vals(0u32, &mut vals); // todo add test which includes mapping
        assert_eq!(vals, &[] as &[u64]);
        let mut vals = vec![];
        multifield.get_vals(1u32, &mut vals); // todo add test which includes mapping
        assert_eq!(vals, &[5, 6]);
        let mut vals = vec![];
        multifield.get_vals(2u32, &mut vals); // todo add test which includes mapping
        assert_eq!(vals, &[3]);
    }
}
