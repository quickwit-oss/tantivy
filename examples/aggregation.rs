// # Aggregation example
//
// This example shows how you can use built-in aggregations.
// We will use range buckets and compute the average in each bucket.
//

use serde_json::Value;
use tantivy::aggregation::agg_req::{
    Aggregation, Aggregations, BucketAggregation, BucketAggregationType, MetricAggregation,
    RangeAggregation,
};
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::metric::AverageAggregation;
use tantivy::aggregation::AggregationCollector;
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
    let score_fieldtype =
        crate::schema::NumericOptions::default().set_fast(Cardinality::SingleValue);
    let highscore_field = schema_builder.add_f64_field("highscore", score_fieldtype.clone());
    let price_field = schema_builder.add_f64_field("price", score_fieldtype.clone());

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Lets index a bunch of documents for this example.
    let index = Index::create_in_ram(schema);

    let mut index_writer = index.writer(50_000_000)?;
    // writing the segment
    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 1f64,
        price_field => 0f64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 3f64,
        price_field => 1f64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 5f64,
        price_field => 1f64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "nohit",
        highscore_field => 6f64,
        price_field => 2f64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 7f64,
        price_field => 2f64,
    ))?;
    index_writer.commit()?;
    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 11f64,
        price_field => 10f64,
    ))?;
    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 14f64,
        price_field => 15f64,
    ))?;

    index_writer.add_document(doc!(
        text_field => "cool",
        highscore_field => 15f64,
        price_field => 20f64,
    ))?;

    index_writer.commit()?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let sub_agg_req_1: Aggregations = vec![(
        "average_price".to_string(),
        Aggregation::Metric(MetricAggregation::Average(
            AverageAggregation::from_field_name("price".to_string()),
        )),
    )]
    .into_iter()
    .collect();

    let agg_req_1: Aggregations = vec![(
        "score_ranges".to_string(),
        Aggregation::Bucket(BucketAggregation {
            bucket_agg: BucketAggregationType::Range(RangeAggregation {
                field: "highscore".to_string(),
                ranges: vec![
                    (-1f64..9f64).into(),
                    (9f64..14f64).into(),
                    (14f64..20f64).into(),
                ],
            }),
            sub_aggregation: sub_agg_req_1.clone(),
        }),
    )]
    .into_iter()
    .collect();

    let collector = AggregationCollector::from_aggs(agg_req_1);

    let searcher = reader.searcher();
    let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
    println!("{}", serde_json::to_string_pretty(&res)?);

    Ok(())
}
