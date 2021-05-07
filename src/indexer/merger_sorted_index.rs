#[cfg(test)]
mod tests {
    use crate::core::Index;
    use crate::schema;
    use crate::schema::Cardinality;
    use crate::schema::Document;
    use crate::schema::IntOptions;
    use crate::IndexSettings;
    use crate::IndexSortByField;
    use crate::IndexWriter;
    use crate::Order;
    use futures::executor::block_on;

    #[test]
    fn test_merge_sorted_index_int_field_desc() {
        let mut schema_builder = schema::Schema::builder();
        let int_options = IntOptions::default()
            .set_fast(Cardinality::SingleValue)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);
        let schema = schema_builder.build();

        let index_builder = Index::builder().schema(schema).settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "intval".to_string(),
                order: Order::Desc,
            }),
        });
        let index = index_builder.create_in_ram().unwrap();

        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let index_doc = |index_writer: &mut IndexWriter, val: u64| {
                let mut doc = Document::default();
                doc.add_u64(int_field, val);
                index_writer.add_document(doc);
            };
            index_doc(&mut index_writer, 1);
            index_doc(&mut index_writer, 3);
            index_doc(&mut index_writer, 2);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, 20);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, 10);
            index_doc(&mut index_writer, 1_000);
            assert!(index_writer.commit().is_ok());
        }
        let reader = index.reader().unwrap();

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            assert!(block_on(index_writer.merge(&segment_ids)).is_ok());
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        reader.reload().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_readers().last().unwrap();

        let fast_fields = segment_reader.fast_fields();
        let fast_field = fast_fields.u64(int_field).unwrap();
        assert_eq!(fast_field.get(5u32), 1u64);
        assert_eq!(fast_field.get(4u32), 2u64);
        assert_eq!(fast_field.get(3u32), 3u64);
        assert_eq!(fast_field.get(2u32), 10u64);
        assert_eq!(fast_field.get(1u32), 20u64);
        assert_eq!(fast_field.get(0u32), 1_000u64);
    }
    #[test]
    fn test_merge_sorted_index_int_field_asc() {
        let mut schema_builder = schema::Schema::builder();
        let int_options = IntOptions::default()
            .set_fast(Cardinality::SingleValue)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);
        let schema = schema_builder.build();

        let index_builder = Index::builder().schema(schema).settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "intval".to_string(),
                order: Order::Asc,
            }),
        });
        let index = index_builder.create_in_ram().unwrap();

        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let index_doc = |index_writer: &mut IndexWriter, val: u64| {
                let mut doc = Document::default();
                doc.add_u64(int_field, val);
                index_writer.add_document(doc);
            };
            index_doc(&mut index_writer, 1);
            index_doc(&mut index_writer, 3);
            index_doc(&mut index_writer, 2);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, 20);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, 10);
            index_doc(&mut index_writer, 1_000);
            assert!(index_writer.commit().is_ok());
        }
        let reader = index.reader().unwrap();

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            assert!(block_on(index_writer.merge(&segment_ids)).is_ok());
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        reader.reload().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_readers().last().unwrap();

        let fast_fields = segment_reader.fast_fields();
        let fast_field = fast_fields.u64(int_field).unwrap();
        assert_eq!(fast_field.get(0u32), 1u64);
        assert_eq!(fast_field.get(1u32), 2u64);
        assert_eq!(fast_field.get(2u32), 3u64);
        assert_eq!(fast_field.get(3u32), 10u64);
        assert_eq!(fast_field.get(4u32), 20u64);
        assert_eq!(fast_field.get(5u32), 1_000u64);
    }
}
