mod reader;
mod writer;

pub use self::reader::MultiValuedFastFieldReader;
pub use self::writer::MultiValuedFastFieldWriter;

#[cfg(test)]
mod tests {

    use chrono::Duration;
    use futures::executor::block_on;
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};
    use test_log::test;

    use crate::collector::TopDocs;
    use crate::indexer::NoMergePolicy;
    use crate::query::QueryParser;
    use crate::schema::{Cardinality, Facet, FacetOptions, NumericOptions, Schema};
    use crate::{Document, Index, Term};

    #[test]
    fn test_multivalued_u64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field(
            "multifield",
            NumericOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(field=>1u64, field=>3u64))?;
        index_writer.add_document(doc!())?;
        index_writer.add_document(doc!(field=>4u64))?;
        index_writer.add_document(doc!(field=>5u64, field=>20u64,field=>1u64))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut vals = Vec::new();
        let multi_value_reader = segment_reader.fast_fields().u64s(field)?;
        {
            multi_value_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[4u64]);
        }
        {
            multi_value_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1u64, 3u64]);
        }
        {
            multi_value_reader.get_vals(1, &mut vals);
            assert!(vals.is_empty());
        }
        Ok(())
    }

    #[test]
    fn test_multivalued_date() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field(
            "multi_date_field",
            NumericOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_indexed()
                .set_fieldnorm()
                .set_stored(),
        );
        let time_i =
            schema_builder.add_i64_field("time_stamp_i", NumericOptions::default().set_stored());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let first_time_stamp = chrono::Utc::now();
        index_writer.add_document(
            doc!(date_field=>first_time_stamp, date_field=>first_time_stamp, time_i=>1i64),
        )?;
        index_writer.add_document(doc!(time_i=>0i64))?;
        // add one second
        index_writer.add_document(
            doc!(date_field=>first_time_stamp + Duration::seconds(1), time_i=>2i64),
        )?;
        // add another second
        let two_secs_ahead = first_time_stamp + Duration::seconds(2);
        index_writer.add_document(doc!(date_field=>two_secs_ahead, date_field=>two_secs_ahead,date_field=>two_secs_ahead, time_i=>3i64))?;
        // add three seconds
        index_writer.add_document(
            doc!(date_field=>first_time_stamp + Duration::seconds(3), time_i=>4i64),
        )?;
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let reader = searcher.segment_reader(0);
        assert_eq!(reader.num_docs(), 5);

        {
            let parser = QueryParser::for_index(&index, vec![]);
            let query = parser.parse_query(&format!(
                "multi_date_field:\"{}\"",
                first_time_stamp.to_rfc3339()
            ))?;
            let results = searcher.search(&query, &TopDocs::with_limit(5))?;
            assert_eq!(results.len(), 1);
            for (_score, doc_address) in results {
                let retrieved_doc = searcher.doc(doc_address)?;
                assert_eq!(
                    retrieved_doc
                        .get_first(date_field)
                        .expect("cannot find value")
                        .as_date()
                        .unwrap()
                        .timestamp(),
                    first_time_stamp.timestamp()
                );
                assert_eq!(
                    retrieved_doc
                        .get_first(time_i)
                        .expect("cannot find value")
                        .as_i64(),
                    Some(1i64)
                );
            }
        }

        {
            let parser = QueryParser::for_index(&index, vec![date_field]);
            let query = parser.parse_query(&format!("\"{}\"", two_secs_ahead.to_rfc3339()))?;
            let results = searcher.search(&query, &TopDocs::with_limit(5))?;

            assert_eq!(results.len(), 1);

            for (_score, doc_address) in results {
                let retrieved_doc = searcher.doc(doc_address).expect("cannot fetch doc");
                assert_eq!(
                    retrieved_doc
                        .get_first(date_field)
                        .expect("cannot find value")
                        .as_date()
                        .unwrap()
                        .timestamp(),
                    two_secs_ahead.timestamp()
                );
                assert_eq!(
                    retrieved_doc
                        .get_first(time_i)
                        .expect("cannot find value")
                        .as_i64(),
                    Some(3i64)
                );
            }
        }

        {
            let parser = QueryParser::for_index(&index, vec![date_field]);
            let range_q = format!(
                "multi_date_field:[{} TO {}}}",
                (first_time_stamp + Duration::seconds(1)).to_rfc3339(),
                (first_time_stamp + Duration::seconds(3)).to_rfc3339()
            );
            let query = parser.parse_query(&range_q)?;
            let results = searcher.search(&query, &TopDocs::with_limit(5))?;

            assert_eq!(results.len(), 2);
            for (i, doc_pair) in results.iter().enumerate() {
                let retrieved_doc = searcher.doc(doc_pair.1).expect("cannot fetch doc");
                let offset_sec = match i {
                    0 => 1,
                    1 => 2,
                    _ => panic!("should not have more than 2 docs"),
                };
                let time_i_val = match i {
                    0 => 2,
                    1 => 3,
                    _ => panic!("should not have more than 2 docs"),
                };
                assert_eq!(
                    retrieved_doc
                        .get_first(date_field)
                        .expect("cannot find value")
                        .as_date()
                        .expect("value not of Date type")
                        .timestamp(),
                    (first_time_stamp + Duration::seconds(offset_sec)).timestamp()
                );
                assert_eq!(
                    retrieved_doc
                        .get_first(time_i)
                        .expect("cannot find value")
                        .as_i64(),
                    Some(time_i_val)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_multivalued_i64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_i64_field(
            "multifield",
            NumericOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(field=> 1i64, field => 3i64))?;
        index_writer.add_document(doc!())?;
        index_writer.add_document(doc!(field=> -4i64))?;
        index_writer.add_document(doc!(field=> -5i64, field => -20i64, field=>1i64))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut vals = Vec::new();
        let multi_value_reader = segment_reader.fast_fields().i64s(field).unwrap();
        multi_value_reader.get_vals(2, &mut vals);
        assert_eq!(&vals, &[-4i64]);
        multi_value_reader.get_vals(0, &mut vals);
        assert_eq!(&vals, &[1i64, 3i64]);
        multi_value_reader.get_vals(1, &mut vals);
        assert!(vals.is_empty());
        multi_value_reader.get_vals(3, &mut vals);
        assert_eq!(&vals, &[-5i64, -20i64, 1i64]);
        Ok(())
    }

    fn test_multivalued_no_panic(ops: &[IndexingOp]) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field(
            "multifield",
            NumericOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_indexed(),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        for &op in ops {
            match op {
                IndexingOp::AddDoc { id } => {
                    match id % 3 {
                        0 => {
                            index_writer.add_document(doc!())?;
                        }
                        1 => {
                            let mut doc = Document::new();
                            for _ in 0..5001 {
                                doc.add_u64(field, id as u64);
                            }
                            index_writer.add_document(doc)?;
                        }
                        _ => {
                            let mut doc = Document::new();
                            doc.add_u64(field, id as u64);
                            index_writer.add_document(doc)?;
                        }
                    };
                }
                IndexingOp::DeleteDoc { id } => {
                    index_writer.delete_term(Term::from_field_u64(field, id as u64));
                }
                IndexingOp::Commit => {
                    index_writer.commit().unwrap();
                }
                IndexingOp::Merge => {
                    let segment_ids = index.searchable_segment_ids()?;
                    if segment_ids.len() >= 2 {
                        block_on(index_writer.merge(&segment_ids))?;
                        index_writer.segment_updater().wait_merging_thread()?;
                    }
                }
            }
        }

        index_writer.commit()?;

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            if !segment_ids.is_empty() {
                block_on(index_writer.merge(&segment_ids)).unwrap();
                assert!(index_writer.wait_merging_threads().is_ok());
            }
        }
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum IndexingOp {
        AddDoc { id: u32 },
        DeleteDoc { id: u32 },
        Commit,
        Merge,
    }

    fn operation_strategy() -> impl Strategy<Value = IndexingOp> {
        prop_oneof![
            (0u32..10u32).prop_map(|id| IndexingOp::DeleteDoc { id }),
            (0u32..10u32).prop_map(|id| IndexingOp::AddDoc { id }),
            (0u32..2u32).prop_map(|_| IndexingOp::Commit),
            (0u32..1u32).prop_map(|_| IndexingOp::Merge),
        ]
    }

    proptest! {
        #[test]
        fn test_multivalued_proptest(ops in proptest::collection::vec(operation_strategy(), 1..10)) {
            assert!(test_multivalued_no_panic(&ops[..]).is_ok());
        }
    }

    #[test]
    fn test_multivalued_proptest_off_by_one_bug_1151() {
        use IndexingOp::*;
        let ops = [
            AddDoc { id: 3 },
            AddDoc { id: 1 },
            AddDoc { id: 3 },
            Commit,
            Merge,
        ];

        assert!(test_multivalued_no_panic(&ops[..]).is_ok());
    }

    #[test]
    #[ignore]
    fn test_many_facets() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_facet_field("facetfield", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        for i in 0..100_000 {
            index_writer
                .add_document(doc!(field=> Facet::from(format!("/lang/{}", i).as_str())))?;
        }
        index_writer.commit()?;
        Ok(())
    }
}
