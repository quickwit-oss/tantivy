// # Aggregation example
//
// This example shows how you can use built-in aggregations.
// As an example, we will use range buckets and compute the average in each bucket.
//

use serde_json::Value;
use tantivy::aggregation::agg_req::{Aggregations, BucketAggregation};
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::{
    Aggregation, AggregationCollector, BucketAggregationType, MetricAggregation,
    RangeAggregationReq,
};
// ---
// Importing tantivy...
use tantivy::query::TermQuery;
use tantivy::schema::{self, Cardinality, IndexRecordOption, Schema, TextFieldIndexing};
use tantivy::{doc, Index, Term};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let text_fieldtype = schema::TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqs),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text", text_fieldtype);
    let score_fieldtype = crate::schema::IntOptions::default().set_fast(Cardinality::SingleValue);
    let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
    let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
    let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Lets index a bunch of documents for this example.
    let index = Index::create_in_ram(schema);

    let mut index_writer = index.writer(50_000_000)?;
    // writing the segment
    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 1u64,
        score_field_f64 => 1f64,
        score_field_i64 => 1i64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 3u64,
        score_field_f64 => 3f64,
        score_field_i64 => 3i64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 5u64,
        score_field_f64 => 5f64,
        score_field_i64 => 5i64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "nohit",
        score_field => 6u64,
        score_field_f64 => 6f64,
        score_field_i64 => 6i64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 7u64,
        score_field_f64 => 7f64,
        score_field_i64 => 7i64,
    ))?;
    index_writer.commit()?;
    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 11u64,
        score_field_f64 => 11f64,
        score_field_i64 => 11i64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 14u64,
        score_field_f64 => 14f64,
        score_field_i64 => 14i64,
    ))?;

    index_writer.add_document(doc!(
        text_field => "cool",
        score_field => 44u64,
        score_field_f64 => 44.5f64,
        score_field_i64 => 44i64,
    ))?;

    index_writer.commit()?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let sub_agg_req_1: Aggregations = vec![(
        "average_in_range".to_string(),
        Aggregation::Metric(MetricAggregation::Average {
            field_name: "score".to_string(),
        }),
    )]
    .into_iter()
    .collect();

    let agg_req_1: Aggregations = vec![
        (
            "average".to_string(),
            Aggregation::Metric(MetricAggregation::Average {
                field_name: "score".to_string(),
            }),
        ),
        (
            "range".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {
                    field_name: "score".to_string(),
                    buckets: vec![(3f64..7f64), (7f64..20f64)],
                }),
                sub_aggregation: sub_agg_req_1.clone(),
            }),
        ),
        (
            "rangef64".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {
                    field_name: "score_f64".to_string(),
                    buckets: vec![(3f64..7f64), (7f64..20f64)],
                }),
                sub_aggregation: sub_agg_req_1.clone(),
            }),
        ),
        (
            "rangei64".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {
                    field_name: "score_i64".to_string(),
                    buckets: vec![(3f64..7f64), (7f64..20f64)],
                }),
                sub_aggregation: sub_agg_req_1,
            }),
        ),
    ]
    .into_iter()
    .collect();

    let collector = AggregationCollector::from_aggs(agg_req_1);

    let searcher = reader.searcher();
    let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap().into();

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
    println!("{}", serde_json::to_string_pretty(&res)?);

    Ok(())
}
