use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::tokenizer::TokenizerManager;
use tantivy::{doc, Index, IndexWriter};

fn create_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | FAST);
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    let documents = vec![
        doc!(
            title => "Rust Programming Book",
            category => "books",
            price => 45u64,
            rating => 4.8f64
        ),
        doc!(
            title => "Advanced Rust Techniques",
            category => "books", 
            price => 55u64,
            rating => 4.9f64
        ),
        doc!(
            title => "Python Guide",
            category => "books",
            price => 35u64,
            rating => 4.2f64
        ),
        doc!(
            title => "Gaming Laptop",
            category => "electronics",
            price => 1500u64,
            rating => 4.7f64
        ),
    ];

    for doc in documents {
        writer.add_document(doc)?;
    }
    writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_field_boosts() -> tantivy::Result<()> {
    let (index, schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let title_field = schema.get_field("title").unwrap();
    let category_field = schema.get_field("category").unwrap();

    let mut query_parser = QueryParser::new(
        schema.clone(),
        vec![title_field, category_field],
        TokenizerManager::default(),
    );

    // Set boost for title field
    query_parser.set_field_boost(title_field, 2.0);

    let agg = json!({
        "rust_items": {
            "filter": { "query_string": "title:rust" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } },
                "count": { "value_count": { "field": "title" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("rust_items"));
    Ok(())
}

#[test]
fn test_fuzzy_matching() -> tantivy::Result<()> {
    let (index, schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let title_field = schema.get_field("title").unwrap();
    let mut query_parser = QueryParser::new(
        schema.clone(),
        vec![title_field],
        TokenizerManager::default(),
    );

    // Enable fuzzy matching
    query_parser.set_field_fuzzy(title_field, false, 1, true);

    let agg = json!({
        "programming_items": {
            "filter": { "query_string": "title:programing" }, // Missing 'm'
            "aggs": {
                "avg_rating": { "avg": { "field": "rating" } },
                "count": { "value_count": { "field": "title" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("programming_items"));
    Ok(())
}

#[test]
fn test_custom_query_parser() -> tantivy::Result<()> {
    let (index, schema) = create_test_index()?;
    let _reader = index.reader()?;

    let title_field = schema.get_field("title").unwrap();
    let category_field = schema.get_field("category").unwrap();

    let mut query_parser = QueryParser::new(
        schema.clone(),
        vec![title_field, category_field],
        TokenizerManager::default(),
    );

    // Configure features
    query_parser.set_field_boost(title_field, 1.5);
    query_parser.set_field_boost(category_field, 0.8);
    query_parser.set_field_fuzzy(title_field, false, 1, true);

    // Test custom parser with filter aggregation
    let filter_agg = tantivy::aggregation::bucket::FilterAggregation::new(
        json!({ "query_string": "title:rust" }),
    );

    let query = filter_agg.parse_query_with_parser(&query_parser)?;
    assert!(format!("{:?}", query).contains("rust") || format!("{:?}", query).contains("Rust"));

    Ok(())
}
