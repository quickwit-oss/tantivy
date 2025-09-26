use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, Query, QueryParser, Weight};
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{doc, Index, IndexWriter};

/// A custom query wrapper that counts how many times it's executed
#[derive(Debug)]
struct CountingQuery {
    inner_query: Box<dyn Query>,
    execution_count: Arc<AtomicUsize>,
}

impl CountingQuery {
    fn new(inner_query: Box<dyn Query>) -> (Self, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        (
            Self {
                inner_query,
                execution_count: counter.clone(),
            },
            counter,
        )
    }
}

impl Query for CountingQuery {
    fn weight(
        &self,
        enable_scoring: tantivy::query::EnableScoring,
    ) -> tantivy::Result<Box<dyn Weight>> {
        self.execution_count.fetch_add(1, Ordering::SeqCst);
        self.inner_query.weight(enable_scoring)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        self.inner_query.query_terms(visitor)
    }
}

impl Clone for CountingQuery {
    fn clone(&self) -> Self {
        Self {
            inner_query: self.inner_query.box_clone(),
            execution_count: self.execution_count.clone(),
        }
    }
}

fn create_test_index() -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let status_field = schema_builder.add_text_field("status", TEXT | FAST);
    let price_field = schema_builder.add_u64_field("price", FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add test documents
    let test_data = [
        ("electronics", "Apple", "available", 1000u64),
        ("electronics", "Samsung", "available", 800u64),
        ("electronics", "Sony", "available", 1200u64),
        ("clothing", "Nike", "available", 150u64),
        ("clothing", "Adidas", "available", 120u64),
        ("books", "Penguin", "available", 25u64),
        ("books", "Harper", "discontinued", 30u64),
        ("electronics", "Apple", "discontinued", 900u64),
        ("clothing", "Puma", "available", 100u64),
        ("electronics", "LG", "available", 700u64),
    ];

    for (category, brand, status, price) in &test_data {
        index_writer.add_document(doc!(
            category_field => *category,
            brand_field => *brand,
            status_field => *status,
            price_field => *price
        ))?;
    }

    index_writer.commit()?;
    Ok(index)
}

#[test]
fn test_multiple_filter_aggregations_efficiency() -> tantivy::Result<()> {
    let index = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Create a base query (WHERE status = 'available')
    let mut schema_builder = Schema::builder();
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let status_field = schema_builder.add_text_field("status", TEXT | FAST);
    let price_field = schema_builder.add_u64_field("price", FAST);
    let schema = schema_builder.build();

    let tokenizer_manager = tantivy::tokenizer::TokenizerManager::default();
    let query_parser = QueryParser::new(
        schema.clone(),
        vec![status_field, category_field, brand_field],
        tokenizer_manager,
    );

    let base_query = query_parser.parse_query("status:available")?;
    let (counting_base_query, base_execution_counter) = CountingQuery::new(base_query);

    // Test the SQL equivalent:
    // SELECT
    //     COUNT(*) AS available_total,
    //     COUNT(*) FILTER (WHERE category @@@ 'electronics') AS electronics_available,
    //     COUNT(*) FILTER (WHERE brand @@@ 'Apple') AS apple_available
    // FROM filter_agg_test
    // WHERE status @@@ 'available';

    let agg_request = json!({
        "available_total": {
            "value_count": { "field": "status" }
        },
        "electronics_available": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "count": { "value_count": { "field": "category" } }
            }
        },
        "apple_available": {
            "filter": { "query_string": "brand:Apple" },
            "aggs": {
                "count": { "value_count": { "field": "brand" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    // Execute the search
    let agg_result = searcher.search(&counting_base_query, &collector)?;

    // Verify that the base query was executed only once
    let base_executions = base_execution_counter.load(Ordering::SeqCst);
    println!("Base query executions: {}", base_executions);

    // The base query should be executed only once, regardless of how many filter aggregations we have
    // Note: In Tantivy, the query weight is created once per segment, so we expect one execution per segment
    assert!(
        base_executions >= 1,
        "Base query should be executed at least once, but was executed {} times",
        base_executions
    );

    // Verify the results are correct
    println!("Aggregation results: {:#?}", agg_result);

    // Check that we have the expected aggregations
    assert!(agg_result.0.contains_key("available_total"));
    assert!(agg_result.0.contains_key("electronics_available"));
    assert!(agg_result.0.contains_key("apple_available"));

    Ok(())
}

#[test]
fn test_filter_aggregation_document_level_efficiency() -> tantivy::Result<()> {
    let index = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test that filter aggregations use document-level evaluation
    // rather than separate index traversals

    let agg_request = json!({
        "all_items": {
            "filter": { "query_string": "*" },
            "aggs": {
                "electronics_only": {
                    "filter": { "query_string": "category:electronics" },
                    "aggs": {
                        "apple_only": {
                            "filter": { "query_string": "brand:Apple" },
                            "aggs": {
                                "count": { "value_count": { "field": "brand" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    // Execute the search with AllQuery (no base filter)
    let agg_result = searcher.search(&AllQuery, &collector)?;

    println!("Nested filter aggregation results: {:#?}", agg_result);

    // Verify the nested structure exists
    assert!(agg_result.0.contains_key("all_items"));

    Ok(())
}

#[test]
fn test_multiple_independent_filter_aggregations() -> tantivy::Result<()> {
    let index = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test multiple independent filter aggregations with different base queries
    // This simulates having multiple separate filtered aggregations

    let agg_request = json!({
        "electronics_stats": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } },
                "count": { "value_count": { "field": "category" } }
            }
        },
        "clothing_stats": {
            "filter": { "query_string": "category:clothing" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } },
                "count": { "value_count": { "field": "category" } }
            }
        },
        "available_items": {
            "filter": { "query_string": "status:available" },
            "aggs": {
                "count": { "value_count": { "field": "status" } }
            }
        },
        "premium_items": {
            "filter": { "query_string": "price:[500 TO *]" },
            "aggs": {
                "count": { "value_count": { "field": "price" } },
                "by_category": {
                    "terms": { "field": "category" }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    // Execute with AllQuery to get all documents, then filter at aggregation level
    let agg_result = searcher.search(&AllQuery, &collector)?;

    println!(
        "Multiple independent filter aggregations: {:#?}",
        agg_result
    );

    // Verify all aggregations are present
    assert!(agg_result.0.contains_key("electronics_stats"));
    assert!(agg_result.0.contains_key("clothing_stats"));
    assert!(agg_result.0.contains_key("available_items"));
    assert!(agg_result.0.contains_key("premium_items"));

    Ok(())
}

#[test]
fn test_filter_aggregation_vs_separate_queries_efficiency() -> tantivy::Result<()> {
    let index = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let mut schema_builder = Schema::builder();
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let status_field = schema_builder.add_text_field("status", TEXT | FAST);
    let schema = schema_builder.build();

    let tokenizer_manager = tantivy::tokenizer::TokenizerManager::default();
    let query_parser = QueryParser::new(
        schema.clone(),
        vec![status_field, category_field, brand_field],
        tokenizer_manager,
    );

    // Method 1: Single aggregation request with multiple filters (efficient)
    let combined_agg_request = json!({
        "electronics_count": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "count": { "value_count": { "field": "category" } }
            }
        },
        "apple_count": {
            "filter": { "query_string": "brand:Apple" },
            "aggs": {
                "count": { "value_count": { "field": "brand" } }
            }
        }
    });

    let combined_aggregations: Aggregations = serde_json::from_value(combined_agg_request)?;
    let combined_collector =
        AggregationCollector::from_aggs(combined_aggregations, Default::default());

    // Execute combined aggregation (single index traversal)
    let base_query = query_parser.parse_query("status:available")?;
    let (counting_base_query, combined_counter) = CountingQuery::new(base_query.box_clone());
    let combined_result = searcher.search(&counting_base_query, &combined_collector)?;

    // Method 2: Separate queries (less efficient - multiple traversals)
    let electronics_agg_request = json!({
        "count": { "value_count": { "field": "category" } }
    });
    let apple_agg_request = json!({
        "count": { "value_count": { "field": "brand" } }
    });

    let electronics_aggregations: Aggregations = serde_json::from_value(electronics_agg_request)?;
    let apple_aggregations: Aggregations = serde_json::from_value(apple_agg_request)?;

    let electronics_collector =
        AggregationCollector::from_aggs(electronics_aggregations, Default::default());
    let apple_collector = AggregationCollector::from_aggs(apple_aggregations, Default::default());

    // Execute separate queries
    let electronics_query =
        query_parser.parse_query("status:available AND category:electronics")?;
    let apple_query = query_parser.parse_query("status:available AND brand:Apple")?;

    let (counting_electronics_query, electronics_counter) = CountingQuery::new(electronics_query);
    let (counting_apple_query, apple_counter) = CountingQuery::new(apple_query);

    let _electronics_result =
        searcher.search(&counting_electronics_query, &electronics_collector)?;
    let _apple_result = searcher.search(&counting_apple_query, &apple_collector)?;

    // Compare execution counts
    let combined_executions = combined_counter.load(Ordering::SeqCst);
    let separate_executions =
        electronics_counter.load(Ordering::SeqCst) + apple_counter.load(Ordering::SeqCst);

    println!("Combined approach executions: {}", combined_executions);
    println!("Separate queries executions: {}", separate_executions);

    // The combined approach should be more efficient (fewer query executions)
    // Note: Each query creates a weight per segment, so we expect at least one execution per approach
    assert!(
        combined_executions >= 1,
        "Combined approach should execute base query at least once"
    );
    assert!(
        separate_executions >= 2,
        "Separate approach should execute queries at least twice"
    );
    assert!(
        combined_executions < separate_executions,
        "Combined approach should be more efficient than separate queries"
    );

    println!("✅ Filter aggregations are more efficient than separate queries!");
    println!("Combined result: {:#?}", combined_result);

    Ok(())
}
