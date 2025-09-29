use serde_json::json;
use std::fmt;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::bucket::filter::FilterAggregation;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, EnableScoring, Query, TermQuery, Weight};
use tantivy::schema::{IndexRecordOption, Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, SegmentReader, Term};

/// A custom user-defined Query type that demonstrates extensibility
/// This simulates what users might implement for specialized search needs
#[derive(Debug, Clone)]
struct CustomPrefixQuery {
    /// The field to search in
    field_name: String,
    /// The prefix to match
    prefix: String,
    /// Whether to boost results
    boost: f32,
}

impl CustomPrefixQuery {
    fn new(field_name: String, prefix: String, boost: f32) -> Self {
        Self {
            field_name,
            prefix,
            boost,
        }
    }
}

impl fmt::Display for CustomPrefixQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CustomPrefixQuery(field: '{}', prefix: '{}', boost: {})",
            self.field_name, self.prefix, self.boost
        )
    }
}

impl Query for CustomPrefixQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        // For this demo, we'll delegate to a TermQuery
        // In a real implementation, this would have custom prefix matching logic
        let schema = enable_scoring.schema();
        let field = schema.get_field(&self.field_name).map_err(|_| {
            tantivy::TantivyError::InvalidArgument(format!("Field '{}' not found", self.field_name))
        })?;

        // Create a term query for the prefix (simplified - real implementation would use prefix matching)
        let term = Term::from_field_text(field, &self.prefix);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);

        // Apply boost if needed
        if (self.boost - 1.0).abs() > f32::EPSILON {
            use tantivy::query::BoostQuery;
            let boosted_query = BoostQuery::new(Box::new(term_query), self.boost);
            boosted_query.weight(enable_scoring)
        } else {
            term_query.weight(enable_scoring)
        }
    }

    fn query_terms(
        &self,
        field: tantivy::schema::Field,
        segment_reader: &SegmentReader,
        visitor: &mut dyn FnMut(&Term, bool),
    ) {
        // For this demo, we'll create a term for the prefix
        if let Ok(schema_field) = segment_reader.schema().get_field(&self.field_name) {
            if schema_field == field {
                let term = Term::from_field_text(field, &self.prefix);
                visitor(&term, false);
            }
        }
    }
}

/// Another custom Query type that combines multiple conditions
#[derive(Debug, Clone)]
struct CustomMultiFieldQuery {
    conditions: Vec<(String, String)>, // (field_name, value) pairs
    require_all: bool,                 // true for AND, false for OR
}

impl CustomMultiFieldQuery {
    fn new(conditions: Vec<(String, String)>, require_all: bool) -> Self {
        Self {
            conditions,
            require_all,
        }
    }
}

impl Query for CustomMultiFieldQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        // For this demo, we'll use BooleanQuery to combine conditions
        use tantivy::query::{BooleanQuery, Occur};

        let schema = enable_scoring.schema();
        let mut subqueries = Vec::new();
        let occur = if self.require_all {
            Occur::Must
        } else {
            Occur::Should
        };

        for (field_name, value) in &self.conditions {
            let field = schema.get_field(field_name).map_err(|_| {
                tantivy::TantivyError::InvalidArgument(format!("Field '{}' not found", field_name))
            })?;

            let term = Term::from_field_text(field, value);
            let term_query = TermQuery::new(term, IndexRecordOption::Basic);
            subqueries.push((occur, Box::new(term_query) as Box<dyn Query>));
        }

        if subqueries.is_empty() {
            return Ok(AllQuery.weight(enable_scoring)?);
        }

        let boolean_query = BooleanQuery::new(subqueries);
        boolean_query.weight(enable_scoring)
    }

    fn query_terms(
        &self,
        field: tantivy::schema::Field,
        segment_reader: &SegmentReader,
        visitor: &mut dyn FnMut(&Term, bool),
    ) {
        let schema = segment_reader.schema();
        for (field_name, value) in &self.conditions {
            if let Ok(schema_field) = schema.get_field(field_name) {
                if schema_field == field {
                    let term = Term::from_field_text(field, value);
                    visitor(&term, false);
                }
            }
        }
    }
}

fn create_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let title_field = schema_builder.add_text_field("title", TEXT | FAST);
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
    let in_stock_field = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut index_writer = index.writer(50_000_000)?;

    // Add test documents
    let docs = vec![
        ("Laptop Pro", "electronics", "apple", 2500u64, true),
        ("Laptop Air", "electronics", "apple", 1500u64, true),
        ("Phone Pro", "electronics", "samsung", 1200u64, true),
        ("Phone Basic", "electronics", "samsung", 800u64, false),
        ("Programming Book", "books", "oreilly", 45u64, true),
        ("Design Book", "books", "oreilly", 55u64, true),
        ("Running Shoes", "clothing", "nike", 120u64, true),
        ("Basketball Shoes", "clothing", "nike", 180u64, false),
    ];

    for (title, category, brand, price, in_stock) in docs {
        index_writer.add_document(doc!(
            title_field => title,
            category_field => category,
            brand_field => brand,
            price_field => price,
            in_stock_field => in_stock
        ))?;
    }

    index_writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_custom_prefix_query_in_filter_aggregation() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Create a custom prefix query that matches titles containing "Laptop"
    // Note: For this demo, we're using exact term matching, not actual prefix matching
    let custom_query = CustomPrefixQuery::new("title".to_string(), "laptop".to_string(), 1.5);

    println!("Testing with: {}", custom_query);

    // Use the custom query directly in FilterAggregation
    let filter_agg = FilterAggregation::new_with_query(Box::new(custom_query));

    // Build aggregation request with the custom filter
    // Note: We can't use the custom query directly in JSON, so we'll just test the query parsing
    let agg_req: Aggregations = serde_json::from_value(json!({
        "test_agg": {
            "terms": { "field": "brand" }
        }
    }))?;

    // Manually create the aggregation structure since we can't serialize custom queries
    // In a real application, this would be handled by the aggregation system
    let _collector = AggregationCollector::from_aggs(agg_req, Default::default());

    // Test that the custom query works by running it directly
    let parsed_query = filter_agg.parse_query(searcher.schema())?;
    let count = parsed_query.count(&searcher)?;
    assert_eq!(count, 2, "Should match 2 laptop documents");

    // Test cloning works
    let cloned_filter = filter_agg.clone();
    let cloned_count = cloned_filter
        .parse_query(searcher.schema())?
        .count(&searcher)?;
    assert_eq!(
        count, cloned_count,
        "Cloned filter should have same results"
    );

    Ok(())
}

#[test]
fn test_custom_multi_field_query_in_filter_aggregation() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Create a custom multi-field query: (category:electronics AND brand:apple)
    let custom_query = CustomMultiFieldQuery::new(
        vec![
            ("category".to_string(), "electronics".to_string()),
            ("brand".to_string(), "apple".to_string()),
        ],
        true, // require_all = true (AND logic)
    );

    println!("Testing multi-field query with AND logic");

    // Use the custom query in FilterAggregation
    let filter_agg = FilterAggregation::new_with_query(Box::new(custom_query));

    // Test the query
    let parsed_query = filter_agg.parse_query(searcher.schema())?;
    let count = parsed_query.count(&searcher)?;
    assert_eq!(
        count, 2,
        "Should match 2 Apple electronics (Laptop Pro, Laptop Air)"
    );

    // Test with OR logic
    let or_query = CustomMultiFieldQuery::new(
        vec![
            ("category".to_string(), "books".to_string()),
            ("brand".to_string(), "nike".to_string()),
        ],
        false, // require_all = false (OR logic)
    );

    let or_filter_agg = FilterAggregation::new_with_query(Box::new(or_query));
    let or_parsed_query = or_filter_agg.parse_query(searcher.schema())?;
    let or_count = or_parsed_query.count(&searcher)?;
    assert_eq!(
        or_count, 4,
        "Should match 4 documents (2 books + 2 nike shoes)"
    );

    Ok(())
}

#[test]
fn test_custom_query_with_json_aggregation() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Create a custom query for electronics
    let custom_query =
        CustomPrefixQuery::new("category".to_string(), "electronics".to_string(), 1.0);

    // Test that we can use it in a complete aggregation workflow
    let filter_agg = FilterAggregation::new_with_query(Box::new(custom_query));

    // Verify the query works as expected
    let parsed_query = filter_agg.parse_query(searcher.schema())?;
    let count = parsed_query.count(&searcher)?;
    assert_eq!(count, 4, "Should match 4 electronics documents");

    // Test serialization behavior
    let serialization_result = serde_json::to_string(&filter_agg);
    assert!(
        serialization_result.is_err(),
        "Custom queries should not be serializable"
    );
    assert!(serialization_result
        .unwrap_err()
        .to_string()
        .contains("Direct Query objects cannot be serialized"));

    Ok(())
}

#[test]
fn test_mixed_json_and_custom_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test JSON query (now using simple query string)
    let json_filter = FilterAggregation::new("category:books".to_string());
    let json_count = json_filter
        .parse_query(searcher.schema())?
        .count(&searcher)?;
    assert_eq!(json_count, 2, "JSON query should match 2 books");

    // Test custom query for the same condition
    let custom_query = CustomPrefixQuery::new("category".to_string(), "books".to_string(), 1.0);
    let custom_filter = FilterAggregation::new_with_query(Box::new(custom_query));
    let custom_count = custom_filter
        .parse_query(searcher.schema())?
        .count(&searcher)?;
    assert_eq!(custom_count, 2, "Custom query should match 2 books");

    // Both should give same results for equivalent queries
    assert_eq!(
        json_count, custom_count,
        "JSON and custom queries should be equivalent"
    );

    // Test that JSON queries can still be serialized
    let json_serialized = serde_json::to_string(&json_filter)?;
    assert!(
        json_serialized.contains("books"),
        "JSON query should serialize correctly"
    );

    // Test that custom queries cannot be serialized
    let custom_serialization = serde_json::to_string(&custom_filter);
    assert!(
        custom_serialization.is_err(),
        "Custom queries should not serialize"
    );

    Ok(())
}

#[test]
fn test_custom_query_equality_and_cloning() -> tantivy::Result<()> {
    let custom_query1 = CustomPrefixQuery::new("title".to_string(), "Laptop".to_string(), 1.0);
    let custom_query2 = CustomPrefixQuery::new("title".to_string(), "Laptop".to_string(), 1.0);
    let custom_query3 = CustomPrefixQuery::new("title".to_string(), "Phone".to_string(), 1.0);

    let filter1 = FilterAggregation::new_with_query(Box::new(custom_query1));
    let filter2 = FilterAggregation::new_with_query(Box::new(custom_query2));
    let filter3 = FilterAggregation::new_with_query(Box::new(custom_query3));

    // Direct queries cannot be compared for equality (by design)
    assert_ne!(
        filter1, filter2,
        "Direct queries should not be equal even if logically equivalent"
    );
    assert_ne!(
        filter1, filter3,
        "Different direct queries should not be equal"
    );

    // But cloning should work
    let cloned_filter1 = filter1.clone();
    assert_ne!(
        filter1, cloned_filter1,
        "Even cloned direct queries are not equal"
    );

    // JSON queries should be equal
    let json_filter1 = FilterAggregation::new("title:Laptop".to_string());
    let json_filter2 = FilterAggregation::new("title:Laptop".to_string());
    assert_eq!(
        json_filter1, json_filter2,
        "Identical JSON queries should be equal"
    );

    Ok(())
}
