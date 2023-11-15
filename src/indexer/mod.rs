//! Indexing and merging data.
//!
//! Contains code to create and merge segments.
//! `IndexWriter` is the main entry point for that, which created from
//! [`Index::writer`](crate::Index::writer).

pub(crate) mod delete_queue;
pub(crate) mod path_to_unordered_id;

pub(crate) mod doc_id_mapping;
mod doc_opstamp_mapping;
mod flat_map_with_buffer;
pub(crate) mod index_writer;
pub(crate) mod index_writer_status;
mod log_merge_policy;
mod merge_operation;
pub(crate) mod merge_policy;
pub(crate) mod merger;
mod merger_sorted_index_test;
pub(crate) mod operation;
pub(crate) mod prepared_commit;
mod segment_entry;
mod segment_manager;
mod segment_register;
pub(crate) mod segment_serializer;
pub(crate) mod segment_updater;
pub(crate) mod segment_writer;
mod stamper;

use crossbeam_channel as channel;
use smallvec::SmallVec;

pub use self::index_writer::IndexWriter;
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_operation::MergeOperation;
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
pub use self::operation::UserOperation;
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub(crate) use self::segment_serializer::SegmentSerializer;
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
type AddBatch<D> = SmallVec<[AddOperation<D>; 4]>;
type AddBatchSender<D> = channel::Sender<AddBatch<D>>;
type AddBatchReceiver<D> = channel::Receiver<AddBatch<D>>;

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap {

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::AggregationCollector;
    use crate::collector::{Count, TopDocs};
    use crate::query::{AllQuery, QueryParser};
    use crate::schema::{JsonObjectOptions, Schema, Type, FAST, INDEXED, STORED, TEXT};
    use crate::{FieldMetadata, Index, IndexWriter, Term};

    #[test]
    fn test_advance_delete_bug() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_from_tempdir(schema_builder.build())?;
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        // there must be one deleted document in the segment
        index_writer.add_document(doc!(text_field=>"b"))?;
        index_writer.delete_term(Term::from_field_text(text_field, "b"));
        // we need enough data to trigger the bug (at least 32 documents)
        for _ in 0..32 {
            index_writer.add_document(doc!(text_field=>"c"))?;
        }
        index_writer.commit()?;
        Ok(())
    }

    #[test]
    fn test_json_field_expand_dots_disabled_dot_escaped_required() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello"});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
        let parse_query = QueryParser::for_index(&index, Vec::new());
        {
            let query = parse_query
                .parse_query(r"json.k8s\.container\.name:prometheus")
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1);
        }
        {
            let query = parse_query
                .parse_query(r#"json.k8s.container.name:prometheus"#)
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 0);
        }
    }

    #[test]
    fn test_json_field_number() {
        // this test was added specifically to reach some cases related to using json fields, with
        // frequency enabled, to store integers, with enough documents containing a single integer
        // that the posting list can be bitpacked.
        let mut schema_builder = Schema::builder();

        let json_field = schema_builder.add_json_field("json", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        for _ in 0..256 {
            let json = serde_json::json!({"somekey": 1u64, "otherkey": -2i64});
            index_writer.add_document(doc!(json_field=>json)).unwrap();

            let json = serde_json::json!({"somekey": "1str", "otherkey": "2str"});
            index_writer.add_document(doc!(json_field=>json)).unwrap();
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 512);
        let parse_query = QueryParser::for_index(&index, Vec::new());
        {
            let query = parse_query.parse_query(r"json.somekey:1").unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 256);
        }
    }

    #[test]
    fn test_json_field_expand_dots_enabled_dot_escape_not_required() {
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions =
            JsonObjectOptions::from(TEXT).set_expand_dots_enabled();
        let json_field = schema_builder.add_json_field("json", json_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello"});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
        let parse_query = QueryParser::for_index(&index, Vec::new());
        {
            let query = parse_query
                .parse_query(r#"json.k8s.container.name:prometheus"#)
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1);
        }
        {
            let query = parse_query
                .parse_query(r"json.k8s\.container\.name:prometheus")
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1);
        }
    }

    #[test]
    fn test_json_field_list_fields() {
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions = JsonObjectOptions::from(TEXT);
        let json_field = schema_builder.add_json_field("json", json_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello", "sub": {"a": 1, "b": 2}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello", "suber": {"a": 1, "b": 2}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello", "suber": {"a": "mixed", "b": 2}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 3);

        let reader = &searcher.segment_readers()[0];
        let inverted_index = reader.inverted_index(json_field).unwrap();
        assert_eq!(
            inverted_index.list_encoded_fields().unwrap(),
            [
                ("k8s.container.name".to_string(), Type::Str),
                ("sub\u{1}a".to_string(), Type::I64),
                ("sub\u{1}b".to_string(), Type::I64),
                ("suber\u{1}a".to_string(), Type::I64),
                ("suber\u{1}a".to_string(), Type::Str),
                ("suber\u{1}b".to_string(), Type::I64),
                ("val".to_string(), Type::Str),
            ]
        );
    }

    #[test]
    fn test_json_fields_metadata_expanded_dots() {
        test_json_fields_metadata(true);
    }
    #[test]
    fn test_json_fields_metadata_no_expanded_dots() {
        test_json_fields_metadata(false);
    }

    fn test_json_fields_metadata(expanded_dots: bool) {
        use pretty_assertions::assert_eq;
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions =
            JsonObjectOptions::from(TEXT).set_fast(None).set_stored();
        let json_options = if expanded_dots {
            json_options.set_expand_dots_enabled()
        } else {
            json_options
        };
        schema_builder.add_json_field("json.confusing", json_options.clone());
        let json_field = schema_builder.add_json_field("json.shadow", json_options.clone());
        let json_field2 = schema_builder.add_json_field("json", json_options.clone());
        schema_builder.add_json_field("empty_json", json_options);
        let number_field = schema_builder.add_u64_field("numbers", FAST);
        schema_builder.add_u64_field("empty", FAST | INDEXED | STORED);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        let json =
            serde_json::json!({"k8s.container.name": "a", "val": "a", "sub": {"a": 1, "b": 1}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json =
            serde_json::json!({"k8s.container.name": "a", "val": "a", "suber": {"a": 1, "b": 1}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json = serde_json::json!({"k8s.container.name": "a", "k8s.container.name": "a", "val": "a", "suber": {"a": "a", "b": 1}});
        index_writer
            .add_document(doc!(number_field => 50u64, json_field=>json, json_field2=>json!({"shadow": {"val": "a"}})))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 3);

        let reader = &searcher.segment_readers()[0];
        let fields_metadata = reader.fields_metadata().unwrap();
        assert_eq!(
            fields_metadata,
            [
                FieldMetadata {
                    field_name: "empty".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::U64
                },
                FieldMetadata {
                    field_name: "empty_json".to_string(),
                    indexed: true,
                    stored: true,
                    fast: false,
                    typ: Type::Json
                },
                FieldMetadata {
                    field_name: "json".to_string(),
                    indexed: true,
                    stored: true,
                    fast: false,
                    typ: Type::Json
                },
                FieldMetadata {
                    field_name: "json.confusing".to_string(),
                    indexed: true,
                    stored: true,
                    fast: false,
                    typ: Type::Json
                },
                FieldMetadata {
                    field_name: "json.shadow".to_string(),
                    indexed: true,
                    stored: true,
                    fast: false,
                    typ: Type::Json
                },
                FieldMetadata {
                    field_name: if expanded_dots {
                        "json.shadow.k8s.container.name".to_string()
                    } else {
                        "json.shadow.k8s\\.container\\.name".to_string()
                    },
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::Str
                },
                FieldMetadata {
                    field_name: "json.shadow.sub.a".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::I64
                },
                FieldMetadata {
                    field_name: "json.shadow.sub.b".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::I64
                },
                FieldMetadata {
                    field_name: "json.shadow.suber.a".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::I64
                },
                FieldMetadata {
                    field_name: "json.shadow.suber.a".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::Str
                },
                FieldMetadata {
                    field_name: "json.shadow.suber.b".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::I64
                },
                FieldMetadata {
                    field_name: "json.shadow.val".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::Str
                },
                FieldMetadata {
                    field_name: "json.shadow.val".to_string(),
                    indexed: true,
                    stored: true,
                    fast: true,
                    typ: Type::Str
                },
                FieldMetadata {
                    field_name: "numbers".to_string(),
                    indexed: false,
                    stored: false,
                    fast: true,
                    typ: Type::U64
                }
            ]
        );
        let query_parser = QueryParser::for_index(&index, vec![]);
        // Test if returned field name can be queried
        for indexed_field in fields_metadata.iter().filter(|meta| meta.indexed) {
            let val = if indexed_field.typ == Type::Str {
                "a"
            } else {
                "1"
            };
            let query_str = &format!("{}:{}", indexed_field.field_name, val);
            let query = query_parser.parse_query(query_str).unwrap();
            let count_docs = searcher.search(&*query, &TopDocs::with_limit(2)).unwrap();
            if indexed_field.field_name.contains("empty") || indexed_field.typ == Type::Json {
                assert_eq!(count_docs.len(), 0);
            } else {
                assert!(!count_docs.is_empty(), "{}", indexed_field.field_name);
            }
        }
        // Test if returned field name can be used for aggregation
        for fast_field in fields_metadata.iter().filter(|meta| meta.fast) {
            let agg_req_str = json!(
            {
              "termagg": {
                "terms": {
                  "field": fast_field.field_name,
                }
              }
            });

            let agg_req: Aggregations = serde_json::from_value(agg_req_str).unwrap();
            let collector = AggregationCollector::from_aggs(agg_req, Default::default());
            let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
            let res = serde_json::to_value(agg_res).unwrap();
            if !fast_field.field_name.contains("empty") && fast_field.typ != Type::Json {
                assert!(
                    !res["termagg"]["buckets"].as_array().unwrap().is_empty(),
                    "{}",
                    fast_field.field_name
                );
            }
        }
    }

    #[test]
    fn test_json_field_shadowing_field_name_bug() {
        /// This test is only there to display a bug on addressing a field if it gets shadowed
        /// The issues only occurs if the field name that shadows contains a dot.
        ///
        /// Happens independently of the `expand_dots` option. Since that option does not
        /// affect the field name itself.
        use pretty_assertions::assert_eq;
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions =
            JsonObjectOptions::from(TEXT).set_fast(None).set_stored();
        // let json_options = json_options.set_expand_dots_enabled();
        let json_field_shadow = schema_builder.add_json_field("json.shadow", json_options.clone());
        let json_field = schema_builder.add_json_field("json", json_options.clone());
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(
                doc!(json_field_shadow=>json!({"val": "b"}), json_field=>json!({"shadow": {"val": "a"}})),
            )
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();

        let fields_and_vals = vec![
            // Only way to address or it gets shadowed by `json.shadow` field
            ("json.shadow\u{1}val".to_string(), "a"), // Succeeds
            //("json.shadow.val".to_string(), "a"),   // Fails
            ("json.shadow.val".to_string(), "b"), // Succeeds
        ];

        let query_parser = QueryParser::for_index(&index, vec![]);
        // Test if field name can be queried
        for (indexed_field, val) in fields_and_vals.iter() {
            let query_str = &format!("{}:{}", indexed_field, val);
            let query = query_parser.parse_query(query_str).unwrap();
            let count_docs = searcher.search(&*query, &TopDocs::with_limit(2)).unwrap();
            assert!(!count_docs.is_empty(), "{}:{}", indexed_field, val);
        }
        // Test if field name can be used for aggregation
        for (field_name, val) in fields_and_vals.iter() {
            let agg_req_str = json!(
            {
              "termagg": {
                "terms": {
                  "field": field_name,
                }
              }
            });

            let agg_req: Aggregations = serde_json::from_value(agg_req_str).unwrap();
            let collector = AggregationCollector::from_aggs(agg_req, Default::default());
            let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
            let res = serde_json::to_value(agg_res).unwrap();
            assert_eq!(
                res["termagg"]["buckets"].as_array().unwrap()[0]["key"]
                    .as_str()
                    .unwrap(),
                *val,
                "{}",
                field_name
            );
        }
    }
}
