use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, Term, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

fn setup_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    writer.add_document(doc!(
        category => "electronics", brand => "apple",
        price => 999u64, rating => 4.5f64, in_stock => true
    ))?;
    writer.add_document(doc!(
        category => "electronics", brand => "samsung",
        price => 799u64, rating => 4.2f64, in_stock => true
    ))?;
    writer.add_document(doc!(
        category => "clothing", brand => "nike",
        price => 120u64, rating => 4.1f64, in_stock => false
    ))?;
    writer.add_document(doc!(
        category => "books", brand => "penguin",
        price => 25u64, rating => 4.8f64, in_stock => true
    ))?;

    writer.commit()?;
    Ok((index, schema))
}

#[test]
fn basic_filter() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("electronics"));
    Ok(())
}

#[test]
fn multiple_filters() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "in_stock": {
            "filter": { "query_string": "in_stock:true" },
            "aggs": { "count": { "value_count": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("electronics"));
    assert!(result.0.contains_key("in_stock"));
    Ok(())
}

#[test]
fn nested_filters() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "all": {
            "filter": { "query_string": "*" },
            "aggs": {
                "electronics": {
                    "filter": { "query_string": "category:electronics" },
                    "aggs": {
                        "expensive": {
                            "filter": { "query_string": "price:[800 TO *]" },
                            "aggs": {
                                "count": { "value_count": { "field": "price" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("all"));
    Ok(())
}

#[test]
fn filter_with_base_query() -> tantivy::Result<()> {
    let (index, schema) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let in_stock_field = schema.get_field("in_stock").unwrap();
    let base_query = TermQuery::new(
        Term::from_field_bool(in_stock_field, true),
        IndexRecordOption::Basic,
    );

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": { "count": { "value_count": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&base_query, &collector)?;

    assert!(result.0.contains_key("electronics"));
    Ok(())
}

#[test]
fn bool_queries() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "premium_electronics": {
            "filter": {
                "bool": {
                    "must": ["category:electronics", "price:[800 TO *]"]
                }
            },
            "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("premium_electronics"));
    Ok(())
}

#[test]
fn empty_results() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "nonexistent": {
            "filter": { "query_string": "category:furniture" },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("nonexistent"));
    Ok(())
}

#[test]
fn mixed_aggregations() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "price_stats": { "stats": { "field": "price" } },
                "brands": {
                    "terms": { "field": "brand" },
                    "aggs": {
                        "avg_price": { "avg": { "field": "price" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("electronics"));
    Ok(())
}
