use columnar::{Column, ColumnType};
use rustc_hash::FxHashMap;

use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::bucket::term_agg::TermsAggregation;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateKey, IntermediateTermBucketEntry, IntermediateTermBucketResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;

/// Special aggregation to handle missing values for term aggregations.
/// This missing aggregation will check multiple columns for existence.
///
/// This is needed when:
/// - The field is multi-valued and we therefore have multiple columns
/// - The field is not text and missing is provided as string (we cannot use the numeric missing
///   value optimization)
#[derive(Default)]
pub struct MissingTermAggReqData {
    /// The accessors to check for existence of a value.
    pub accessors: Vec<(Column<u64>, ColumnType)>,
    /// The name of the aggregation.
    pub name: String,
    /// The original terms aggregation request.
    pub req: TermsAggregation,
}

impl MissingTermAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// The specialized missing term aggregation.
#[derive(Default, Debug, Clone)]
pub struct TermMissingAgg {
    missing_count: u32,
    accessor_idx: usize,
    sub_agg: Option<Box<dyn SegmentAggregationCollector>>,
}
impl TermMissingAgg {
    pub(crate) fn new(
        req_data: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        let has_sub_aggregations = !node.children.is_empty();
        let accessor_idx = node.idx_in_req_data;
        let sub_agg = if has_sub_aggregations {
            let sub_aggregation = build_segment_agg_collectors(req_data, &node.children)?;
            Some(sub_aggregation)
        } else {
            None
        };

        Ok(Self {
            accessor_idx,
            sub_agg,
            ..Default::default()
        })
    }
}

impl SegmentAggregationCollector for TermMissingAgg {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let req_data = agg_data.get_missing_term_req_data(self.accessor_idx);
        let term_agg = &req_data.req;
        let missing = term_agg
            .missing
            .as_ref()
            .expect("TermMissingAgg collector, but no missing found in agg req")
            .clone();
        let mut entries: FxHashMap<IntermediateKey, IntermediateTermBucketEntry> =
            Default::default();

        let mut missing_entry = IntermediateTermBucketEntry {
            doc_count: self.missing_count,
            sub_aggregation: Default::default(),
        };
        if let Some(sub_agg) = self.sub_agg {
            let mut res = IntermediateAggregationResults::default();
            sub_agg.add_intermediate_aggregation_result(agg_data, &mut res)?;
            missing_entry.sub_aggregation = res;
        }
        entries.insert(missing.into(), missing_entry);

        let bucket = IntermediateBucketResult::Terms {
            buckets: IntermediateTermBucketResult {
                entries,
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: 0,
            },
        };

        results.push(
            req_data.name.to_string(),
            IntermediateAggregationResult::Bucket(bucket),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let req_data = agg_data.get_missing_term_req_data(self.accessor_idx);
        let has_value = req_data
            .accessors
            .iter()
            .any(|(acc, _)| acc.index.has_value(doc));
        if !has_value {
            self.missing_count += 1;
            if let Some(sub_agg) = self.sub_agg.as_mut() {
                sub_agg.collect(doc, agg_data)?;
            }
        }
        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        for doc in docs {
            self.collect(*doc, agg_data)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request_with_query;
    use crate::schema::{Schema, FAST};
    use crate::{Index, IndexWriter};

    #[test]
    fn terms_aggregation_missing_mixed_type_mult_seg_sub_agg() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let score = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with all values numeric
        index_writer
            .add_document(doc!(score => 1.0, json => json!({"mixed_type": 10.0})))
            .unwrap();
        index_writer.add_document(doc!(score => 5.0))?;
        // index_writer.commit().unwrap();
        //// => Segment with all values text
        index_writer
            .add_document(doc!(score => 1.0, json => json!({"mixed_type": "blue"})))
            .unwrap();
        index_writer.add_document(doc!(score => 5.0))?;
        // index_writer.commit().unwrap();

        // => Segment with mixed values
        index_writer.add_document(doc!(json => json!({"mixed_type": "red"})))?;
        index_writer.add_document(doc!(json => json!({"mixed_type": -20.5})))?;
        index_writer.add_document(doc!(json => json!({"mixed_type": true})))?;
        index_writer.add_document(doc!(score => 5.0))?;

        index_writer.commit().unwrap();
        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
                "aggs": {
                    "sum_score": {
                        "sum": {
                            "field": "score"
                        }
                    }
                }
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 3);
        assert_eq!(
            res["replace_null"]["buckets"][0]["sum_score"]["value"],
            15.0
        );
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_mixed_type_sub_agg_reg1() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let score = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with all values numeric
        index_writer.add_document(doc!(score => 1.0, json => json!({"mixed_type": 10.0})))?;
        index_writer.add_document(doc!(score => 5.0))?;
        index_writer.add_document(doc!(score => 5.0))?;

        index_writer.commit().unwrap();
        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
                "aggs": {
                    "sum_score": {
                        "sum": {
                            "field": "score"
                        }
                    }
                }
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 2);
        assert_eq!(
            res["replace_null"]["buckets"][0]["sum_score"]["value"],
            10.0
        );
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_mult_seg_empty() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let score = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();

        index_writer.add_document(doc!(score => 5.0))?;
        index_writer.commit().unwrap();
        index_writer.add_document(doc!(score => 5.0))?;
        index_writer.commit().unwrap();
        index_writer.add_document(doc!(score => 5.0))?;

        index_writer.commit().unwrap();
        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
                "aggs": {
                    "sum_score": {
                        "sum": {
                            "field": "score"
                        }
                    }
                }
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 3);
        assert_eq!(
            res["replace_null"]["buckets"][0]["sum_score"]["value"],
            15.0
        );
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_single_seg_empty() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let score = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();

        index_writer.add_document(doc!(score => 5.0))?;
        index_writer.add_document(doc!(score => 5.0))?;
        index_writer.add_document(doc!(score => 5.0))?;

        index_writer.commit().unwrap();
        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
                "aggs": {
                    "sum_score": {
                        "sum": {
                            "field": "score"
                        }
                    }
                }
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 3);
        assert_eq!(
            res["replace_null"]["buckets"][0]["sum_score"]["value"],
            15.0
        );
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_mixed_type_mult_seg() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with all values numeric
        index_writer
            .add_document(doc!(json => json!({"mixed_type": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();
        //// => Segment with all values text
        index_writer
            .add_document(doc!(json => json!({"mixed_type": "blue"})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        // => Segment with mixed values
        index_writer
            .add_document(doc!(json => json!({"mixed_type": "red"})))
            .unwrap();
        index_writer
            .add_document(doc!(json => json!({"mixed_type": -20.5})))
            .unwrap();
        index_writer
            .add_document(doc!(json => json!({"mixed_type": true})))
            .unwrap();
        index_writer.add_document(doc!())?;

        index_writer.commit().unwrap();
        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
            },
            "replace_num": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": 1337
                },
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["replace_num"]["buckets"][0]["key"], 1337.0);
        assert_eq!(res["replace_num"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_str_on_numeric_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with all values numeric
        index_writer
            .add_document(doc!(json => json!({"mixed_type": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.add_document(doc!())?;

        index_writer
            .add_document(doc!(json => json!({"mixed_type": -20.5})))
            .unwrap();
        index_writer.add_document(doc!())?;

        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_mixed_type_one_seg() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with all values numeric
        index_writer
            .add_document(doc!(json => json!({"mixed_type": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        //// => Segment with all values text
        index_writer
            .add_document(doc!(json => json!({"mixed_type": "blue"})))
            .unwrap();
        index_writer.add_document(doc!())?;

        // => Segment with mixed values
        index_writer
            .add_document(doc!(json => json!({"mixed_type": "red"})))
            .unwrap();
        index_writer
            .add_document(doc!(json => json!({"mixed_type": -20.5})))
            .unwrap();
        index_writer
            .add_document(doc!(json => json!({"mixed_type": true})))
            .unwrap();
        index_writer.add_document(doc!())?;

        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "replace_null": {
                "terms": {
                    "field": "json.mixed_type",
                    "missing": "NULL"
                },
            },
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["replace_null"]["buckets"][0]["key"], "NULL");
        assert_eq!(res["replace_null"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["replace_null"]["sum_other_doc_count"], 0);
        assert_eq!(res["replace_null"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }
}
