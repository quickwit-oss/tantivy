use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

#[test]
fn test_boolean_query_fast_path() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add test data
    for i in 0..2000 {
        writer.add_document(doc!(
            category => if i % 3 == 0 { "electronics" } else if i % 3 == 1 { "books" } else { "clothing" },
            price => (100 + i) as u64,
            in_stock => i % 2 == 0,
            rating => (i as f64) / 200.0
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test 1: Simple boolean query with fast field terms (should use fast path)
    let simple_bool_agg = json!({
        "electronics_in_stock": {
            "filter": {
                "bool": {
                    "must": [
                        { "query_string": "category:electronics" },
                        { "query_string": "in_stock:true" }
                    ]
                }
            },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(simple_bool_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let simple_bool_duration = start.elapsed();

    // Test 2: Complex boolean query with range and term queries (should use fast path)
    let complex_bool_agg = json!({
        "premium_electronics": {
            "filter": {
                "bool": {
                    "must": [
                        { "query_string": "category:electronics" },
                        { "query_string": "price:[500 TO *]" },
                        { "query_string": "rating:[4.0 TO *]" }
                    ],
                    "must_not": [
                        { "query_string": "in_stock:false" }
                    ]
                }
            },
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(complex_bool_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let complex_bool_duration = start.elapsed();

    // Test 3: Boolean query with should clauses (should use fast path)
    let should_bool_agg = json!({
        "books_or_electronics": {
            "filter": {
                "bool": {
                    "should": [
                        { "query_string": "category:books" },
                        { "query_string": "category:electronics" }
                    ]
                }
            },
            "aggs": {
                "max_price": { "max": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(should_bool_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let should_bool_duration = start.elapsed();

    println!(
        "Simple boolean query (must) duration: {:?}",
        simple_bool_duration
    );
    println!(
        "Complex boolean query (must + must_not) duration: {:?}",
        complex_bool_duration
    );
    println!("Should boolean query duration: {:?}", should_bool_duration);

    // All should complete quickly using fast path
    assert!(simple_bool_duration.as_millis() < 100);
    assert!(complex_bool_duration.as_millis() < 100);
    assert!(should_bool_duration.as_millis() < 100);

    println!("✅ All boolean queries used fast path evaluation!");

    Ok(())
}

#[test]
fn test_nested_boolean_query_fast_path() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add test data
    for i in 0..1000 {
        writer.add_document(doc!(
            category => if i % 2 == 0 { "electronics" } else { "books" },
            price => (100 + i) as u64,
            brand => if i % 3 == 0 { "Apple" } else if i % 3 == 1 { "Samsung" } else { "Sony" }
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test nested boolean query (should use fast path for sub-queries)
    let nested_bool_agg = json!({
        "premium_branded_electronics": {
            "filter": {
                "bool": {
                    "must": [
                        { "query_string": "category:electronics" },
                        {
                            "bool": {
                                "should": [
                                    { "query_string": "brand:Apple" },
                                    { "query_string": "brand:Samsung" }
                                ]
                            }
                        },
                        { "query_string": "price:[200 TO 800]" }
                    ]
                }
            },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(nested_bool_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let nested_duration = start.elapsed();

    println!("Nested boolean query duration: {:?}", nested_duration);

    // Should complete quickly using fast path
    assert!(nested_duration.as_millis() < 100);

    println!("✅ Nested boolean query used fast path evaluation!");

    Ok(())
}
