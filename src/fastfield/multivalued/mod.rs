mod index;
mod reader;
mod writer;

use fastfield_codecs::FastFieldCodecType;
pub use index::MultiValueIndex;

pub use self::reader::MultiValuedFastFieldReader;
pub(crate) use self::writer::MultivalueStartIndex;
pub use self::writer::{MultiValueU128FastFieldWriter, MultiValuedFastFieldWriter};

/// The valid codecs for multivalue values excludes the linear interpolation codec.
///
/// This limitation is only valid for the values, not the offset index of the multivalue index.
pub(crate) fn get_fastfield_codecs_for_multivalue() -> [FastFieldCodecType; 2] {
    [
        FastFieldCodecType::Bitpacked,
        FastFieldCodecType::BlockwiseLinear,
    ]
}

#[cfg(test)]
mod tests {
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};
    use test_log::test;

    use crate::collector::TopDocs;
    use crate::indexer::NoMergePolicy;
    use crate::query::QueryParser;
    use crate::schema::{Cardinality, DateOptions, Facet, FacetOptions, NumericOptions, Schema};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::{Duration, OffsetDateTime};
    use crate::{DateTime, Document, Index, Term};

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
            DateOptions::default()
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
        let first_time_stamp = OffsetDateTime::now_utc();
        index_writer.add_document(doc!(
                date_field => DateTime::from_utc(first_time_stamp),
                date_field => DateTime::from_utc(first_time_stamp),
                time_i=>1i64))?;
        index_writer.add_document(doc!(time_i => 0i64))?;
        // add one second
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(first_time_stamp + Duration::seconds(1)),
            time_i => 2i64))?;
        // add another second
        let two_secs_ahead = first_time_stamp + Duration::seconds(2);
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(two_secs_ahead),
            date_field => DateTime::from_utc(two_secs_ahead),
            date_field => DateTime::from_utc(two_secs_ahead),
            time_i => 3i64))?;
        // add three seconds
        index_writer.add_document(doc!(
                date_field => DateTime::from_utc(first_time_stamp + Duration::seconds(3)),
                time_i => 4i64))?;
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let reader = searcher.segment_reader(0);
        assert_eq!(reader.num_docs(), 5);

        {
            let parser = QueryParser::for_index(&index, vec![]);
            let query = parser.parse_query(&format!(
                "multi_date_field:\"{}\"",
                first_time_stamp.format(&Rfc3339)?,
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
                        .unwrap(),
                    DateTime::from_utc(first_time_stamp),
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
            let query = parser.parse_query(&format!("\"{}\"", two_secs_ahead.format(&Rfc3339)?))?;
            let results = searcher.search(&query, &TopDocs::with_limit(5))?;

            assert_eq!(results.len(), 1);

            for (_score, doc_address) in results {
                let retrieved_doc = searcher.doc(doc_address).expect("cannot fetch doc");
                assert_eq!(
                    retrieved_doc
                        .get_first(date_field)
                        .expect("cannot find value")
                        .as_date()
                        .unwrap(),
                    DateTime::from_utc(two_secs_ahead)
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
                (first_time_stamp + Duration::seconds(1)).format(&Rfc3339)?,
                (first_time_stamp + Duration::seconds(3)).format(&Rfc3339)?
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
                        .expect("value not of Date type"),
                    DateTime::from_utc(first_time_stamp + Duration::seconds(offset_sec)),
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

    #[test]
    fn test_multivalued_bool() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let bool_field = schema_builder.add_bool_field(
            "multifield",
            NumericOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(bool_field=> true, bool_field => false))?;
        index_writer.add_document(doc!())?;
        index_writer.add_document(doc!(bool_field=> false))?;
        index_writer
            .add_document(doc!(bool_field=> true, bool_field => true, bool_field => false))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut vals = Vec::new();
        let multi_value_reader = segment_reader.fast_fields().bools(bool_field).unwrap();
        multi_value_reader.get_vals(2, &mut vals);
        assert_eq!(&vals, &[false]);
        multi_value_reader.get_vals(0, &mut vals);
        assert_eq!(&vals, &[true, false]);
        multi_value_reader.get_vals(1, &mut vals);
        assert!(vals.is_empty());
        multi_value_reader.get_vals(3, &mut vals);
        assert_eq!(&vals, &[true, true, false]);
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
                        index_writer.merge(&segment_ids).wait()?;
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
                index_writer.merge(&segment_ids).wait()?;
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
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(5))]
        #[test]
        fn test_multivalued_proptest(ops in proptest::collection::vec(operation_strategy(), 1..10)) {
            assert!(test_multivalued_no_panic(&ops[..]).is_ok());
        }
    }

    #[test]
    fn test_multivalued_proptest_gcd() {
        use IndexingOp::*;
        let ops = [AddDoc { id: 9 }, AddDoc { id: 9 }, Merge];

        assert!(test_multivalued_no_panic(&ops[..]).is_ok());
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

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::collections::HashMap;
    use std::path::Path;

    use test::{self, Bencher};

    use super::*;
    use crate::directory::{CompositeFile, Directory, RamDirectory, WritePtr};
    use crate::fastfield::{CompositeFastFieldSerializer, FastFieldsWriter};
    use crate::indexer::doc_id_mapping::DocIdMapping;
    use crate::schema::{Cardinality, NumericOptions, Schema};
    use crate::Document;

    fn bench_multi_value_ff_merge_opt(
        num_docs: usize,
        segments_every_n_docs: usize,
        merge_policy: impl crate::indexer::MergePolicy + 'static,
    ) {
        let mut builder = crate::schema::SchemaBuilder::new();

        let fast_multi =
            crate::schema::NumericOptions::default().set_fast(Cardinality::MultiValues);
        let multi_field = builder.add_f64_field("f64s", fast_multi);

        let index = crate::Index::create_in_ram(builder.build());

        let mut writer = index.writer_for_tests().unwrap();
        writer.set_merge_policy(Box::new(merge_policy));

        for i in 0..num_docs {
            let mut doc = crate::Document::new();
            doc.add_f64(multi_field, 0.24);
            doc.add_f64(multi_field, 0.27);
            doc.add_f64(multi_field, 0.37);
            if i % 3 == 0 {
                doc.add_f64(multi_field, 0.44);
            }

            writer.add_document(doc).unwrap();
            if i % segments_every_n_docs == 0 {
                writer.commit().unwrap();
            }
        }

        {
            writer.wait_merging_threads().unwrap();
            let mut writer = index.writer_for_tests().unwrap();
            let segment_ids = index.searchable_segment_ids().unwrap();
            writer.merge(&segment_ids).wait().unwrap();
        }

        // If a merging thread fails, we should end up with more
        // than one segment here
        assert_eq!(1, index.searchable_segments().unwrap().len());
    }

    #[bench]
    fn bench_multi_value_ff_merge_many_segments(b: &mut Bencher) {
        let num_docs = 100_000;
        b.iter(|| {
            bench_multi_value_ff_merge_opt(num_docs, 1_000, crate::indexer::NoMergePolicy);
        });
    }

    #[bench]
    fn bench_multi_value_ff_merge_many_segments_log_merge(b: &mut Bencher) {
        let num_docs = 100_000;
        b.iter(|| {
            let merge_policy = crate::indexer::LogMergePolicy::default();
            bench_multi_value_ff_merge_opt(num_docs, 1_000, merge_policy);
        });
    }

    #[bench]
    fn bench_multi_value_ff_merge_few_segments(b: &mut Bencher) {
        let num_docs = 100_000;
        b.iter(|| {
            bench_multi_value_ff_merge_opt(num_docs, 33_000, crate::indexer::NoMergePolicy);
        });
    }

    fn multi_values(num_docs: usize, vals_per_doc: usize) -> Vec<Vec<u64>> {
        let mut vals = vec![];
        for _i in 0..num_docs {
            let mut block = vec![];
            for j in 0..vals_per_doc {
                block.push(j as u64);
            }
            vals.push(block);
        }

        vals
    }

    #[bench]
    fn bench_multi_value_fflookup(b: &mut Bencher) {
        let num_docs = 100_000;

        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        let field = {
            let options = NumericOptions::default().set_fast(Cardinality::MultiValues);
            let mut schema_builder = Schema::builder();
            let field = schema_builder.add_u64_field("field", options);
            let schema = schema_builder.build();

            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for block in &multi_values(num_docs, 3) {
                let mut doc = Document::new();
                for val in block {
                    doc.add_u64(field, *val);
                }
                fast_field_writers.add_document(&doc).unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
            field
        };
        let file = directory.open_read(path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data_idx = fast_fields_composite
                .open_read_with_idx(field, 0)
                .unwrap()
                .read_bytes()
                .unwrap();
            let idx_reader = fastfield_codecs::open(data_idx).unwrap();

            let data_vals = fast_fields_composite
                .open_read_with_idx(field, 1)
                .unwrap()
                .read_bytes()
                .unwrap();
            let vals_reader = fastfield_codecs::open(data_vals).unwrap();
            let fast_field_reader = MultiValuedFastFieldReader::open(idx_reader, vals_reader);
            b.iter(|| {
                let mut sum = 0u64;
                let mut data = Vec::with_capacity(10);
                for i in 0u32..num_docs as u32 {
                    fast_field_reader.get_vals(i, &mut data);
                    sum += data.iter().sum::<u64>();
                }
                sum
            });
        }
    }

    #[bench]
    fn bench_multi_value_ff_creation(b: &mut Bencher) {
        // 3 million ff entries
        let num_docs = 1_000_000;
        let multi_values = multi_values(num_docs, 3);

        b.iter(|| {
            let directory: RamDirectory = RamDirectory::create();
            let options = NumericOptions::default().set_fast(Cardinality::MultiValues);
            let mut schema_builder = Schema::builder();
            let field = schema_builder.add_u64_field("field", options);
            let schema = schema_builder.build();

            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for block in &multi_values {
                let mut doc = Document::new();
                for val in block {
                    doc.add_u64(field, *val);
                }
                fast_field_writers.add_document(&doc).unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        });
    }

    #[bench]
    fn bench_multi_value_ff_creation_with_sorting(b: &mut Bencher) {
        // 3 million ff entries
        let num_docs = 1_000_000;
        let multi_values = multi_values(num_docs, 3);

        let doc_id_mapping =
            DocIdMapping::from_new_id_to_old_id((0..1_000_000).collect::<Vec<_>>());

        b.iter(|| {
            let directory: RamDirectory = RamDirectory::create();
            let options = NumericOptions::default().set_fast(Cardinality::MultiValues);
            let mut schema_builder = Schema::builder();
            let field = schema_builder.add_u64_field("field", options);
            let schema = schema_builder.build();

            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for block in &multi_values {
                let mut doc = Document::new();
                for val in block {
                    doc.add_u64(field, *val);
                }
                fast_field_writers.add_document(&doc).unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), Some(&doc_id_mapping))
                .unwrap();
            serializer.close().unwrap();
        });
    }
}
