use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{doc, Index, IndexWriter};

fn create_index() -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);
    let status = schema_builder.add_text_field("status", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    let data = vec![
        ("electronics", "Apple", "available", 1000),
        ("electronics", "Samsung", "available", 800),
        ("clothing", "Nike", "available", 150),
        ("books", "Penguin", "available", 25),
        ("electronics", "Sony", "discontinued", 900),
    ];

    for (cat, br, st, pr) in data {
        writer.add_document(doc!(
            category => cat, brand => br, status => st, price => pr as u64
        ))?;
    }
    writer.commit()?;
    Ok(index)
}

#[test]
fn test_single_traversal() -> tantivy::Result<()> {
    let index = create_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Multiple filters should be evaluated in a single traversal
    let agg = json!({
        "total": { "value_count": { "field": "status" } },
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": { "count": { "value_count": { "field": "category" } } }
        },
        "apple": {
            "filter": { "query_string": "brand:Apple" },
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let schema = index.schema();
    let status_field = schema.get_field("status").unwrap();
    let parser = QueryParser::new(schema, vec![status_field], Default::default());
    let base_query = parser.parse_query("status:available")?;

    let result = searcher.search(&base_query, &collector)?;

    // All aggregations should be present from single traversal
    assert!(result.0.contains_key("total"));
    assert!(result.0.contains_key("electronics"));
    assert!(result.0.contains_key("apple"));

    Ok(())
}

#[test]
fn test_nested_efficiency() -> tantivy::Result<()> {
    let index = create_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Nested filters should use document-level evaluation
    let agg = json!({
        "all": {
            "filter": { "query_string": "*" },
            "aggs": {
                "electronics": {
                    "filter": { "query_string": "category:electronics" },
                    "aggs": {
                        "available": {
                            "filter": { "query_string": "status:available" },
                            "aggs": { "count": { "value_count": { "field": "brand" } } }
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
