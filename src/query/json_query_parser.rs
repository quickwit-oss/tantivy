//! JSON Query Parser for Elasticsearch-compatible queries
//!
//! This module provides functionality to parse Elasticsearch-style JSON queries
//! into Tantivy Query objects. It's designed to be reusable across different
//! parts of the codebase that need to handle JSON queries.

use crate::query::{AllQuery, BooleanQuery, Query, QueryParser};
use crate::schema::Schema;
use crate::tokenizer::TokenizerManager;
use crate::TantivyError;
use serde_json::Value;

/// Parse JSON query into Tantivy Query objects
///
/// This function handles the conversion of JSON queries into Tantivy's internal
/// Query representation. It supports common query types like term, range, bool,
/// match, and match_all.
///
/// # Arguments
/// * `query_json` - The JSON query object
/// * `schema` - The schema to validate fields against
///
/// # Returns
/// A boxed Query object that can be used with Tantivy's search APIs
///
/// # Example
/// ```rust
/// use serde_json::json;
/// use tantivy::schema::{Schema, TEXT, FAST, INDEXED};
/// use tantivy::query::parse_query;
///
/// let mut schema_builder = Schema::builder();
/// let category_field = schema_builder.add_text_field("category", TEXT);
/// let price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
/// let schema = schema_builder.build();
///
/// let query_json = json!({ "term": { "category": "electronics" } });
/// let query = parse_query(&query_json, &schema).unwrap();
/// ```
pub fn parse_query(
    query_json: &serde_json::Value,
    schema: &Schema,
) -> crate::Result<Box<dyn Query>> {
    match query_json {
        Value::Object(obj) => {
            if obj.is_empty() {
                return Err(TantivyError::InvalidArgument(
                    "Query object cannot be empty".to_string(),
                ));
            }

            if obj.len() > 1 {
                return Err(TantivyError::InvalidArgument(
                    "Query object should contain exactly one query type".to_string(),
                ));
            }

            let (query_type, query_params) = obj.iter().next().unwrap();
            match query_type.as_str() {
                "term" => parse_term_query(query_params, schema),
                "range" => parse_range_query(query_params, schema),
                "bool" => parse_bool_query(query_params, schema),
                "match" => parse_match_query(query_params, schema),
                "match_all" => Ok(Box::new(AllQuery)),
                _ => Err(TantivyError::InvalidArgument(format!(
                    "Unsupported query type: {}",
                    query_type
                ))),
            }
        }
        _ => Err(TantivyError::InvalidArgument(
            "Query must be a JSON object".to_string(),
        )),
    }
}

/// Parse Elasticsearch term query: { "term": { "field": "value" } }
fn parse_term_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    let obj = params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Term query parameters must be an object".to_string())
    })?;

    if obj.len() != 1 {
        return Err(TantivyError::InvalidArgument(
            "Term query must specify exactly one field".to_string(),
        ));
    }

    let (field_name, field_value) = obj.iter().next().unwrap();
    let field = schema.get_field(field_name).map_err(|_| {
        TantivyError::InvalidArgument(format!("Field '{}' not found in schema", field_name))
    })?;

    // Convert the JSON value to a string for parsing
    let value_str = json_value_to_query_string(field_value)?;

    // Use Tantivy's QueryParser to properly handle type conversion
    let tokenizer_manager = TokenizerManager::default();
    let query_parser = QueryParser::new(schema.clone(), vec![field], tokenizer_manager);

    // Create a query string in the format "field:value"
    let query_str = format!("{}:{}", field_name, value_str);

    query_parser
        .parse_query(&query_str)
        .map_err(|e| TantivyError::InvalidArgument(format!("Failed to parse term query: {}", e)))
}

/// Parse Elasticsearch range query: { "range": { "field": { "gte": 10, "lt": 20 } } }
fn parse_range_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    let obj = params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Range query parameters must be an object".to_string())
    })?;

    if obj.len() != 1 {
        return Err(TantivyError::InvalidArgument(
            "Range query must specify exactly one field".to_string(),
        ));
    }

    let (field_name, range_params) = obj.iter().next().unwrap();
    let field = schema.get_field(field_name).map_err(|_| {
        TantivyError::InvalidArgument(format!("Field '{}' not found in schema", field_name))
    })?;

    let range_obj = range_params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Range query field parameters must be an object".to_string())
    })?;

    // Build a range query string in Tantivy's format
    let mut lower_bound = None;
    let mut upper_bound = None;
    let mut lower_inclusive = true;
    let mut upper_inclusive = true;

    for (bound_type, bound_value) in range_obj {
        let value_str = json_value_to_range_string(bound_value)?;

        match bound_type.as_str() {
            "gte" => {
                lower_bound = Some(value_str);
                lower_inclusive = true;
            }
            "gt" => {
                lower_bound = Some(value_str);
                lower_inclusive = false;
            }
            "lte" => {
                upper_bound = Some(value_str);
                upper_inclusive = true;
            }
            "lt" => {
                upper_bound = Some(value_str);
                upper_inclusive = false;
            }
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported range bound: {}",
                    bound_type
                )))
            }
        }
    }

    if lower_bound.is_none() && upper_bound.is_none() {
        return Err(TantivyError::InvalidArgument(
            "Range query must specify at least one bound".to_string(),
        ));
    }

    // Use Tantivy's QueryParser to properly handle type conversion
    let tokenizer_manager = TokenizerManager::default();
    let query_parser = QueryParser::new(schema.clone(), vec![field], tokenizer_manager);

    // Create a range query string in Tantivy's format: "field:[lower TO upper]"
    let lower_str = lower_bound.as_deref().unwrap_or("*");
    let upper_str = upper_bound.as_deref().unwrap_or("*");
    let left_bracket = if lower_inclusive { "[" } else { "{" };
    let right_bracket = if upper_inclusive { "]" } else { "}" };

    let query_str = format!(
        "{}:{}{} TO {}{}",
        field_name, left_bracket, lower_str, upper_str, right_bracket
    );

    query_parser
        .parse_query(&query_str)
        .map_err(|e| TantivyError::InvalidArgument(format!("Failed to parse range query: {}", e)))
}

/// Parse Elasticsearch bool query: { "bool": { "must": [...], "should": [...], "must_not": [...] } }
fn parse_bool_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    use crate::query::Occur;

    let obj = params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Bool query parameters must be an object".to_string())
    })?;

    let mut subqueries = Vec::new();

    for (clause_type, clause_queries) in obj {
        let occur = match clause_type.as_str() {
            "must" => Occur::Must,
            "should" => Occur::Should,
            "must_not" => Occur::MustNot,
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported bool query clause: {}",
                    clause_type
                )))
            }
        };

        let queries_array = clause_queries.as_array().ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "Bool query '{}' clause must be an array",
                clause_type
            ))
        })?;

        for query_json in queries_array {
            let query = parse_query(query_json, schema)?;
            subqueries.push((occur, query));
        }
    }

    Ok(Box::new(BooleanQuery::new(subqueries)))
}

/// Parse Elasticsearch match query: { "match": { "field": "value" } }
/// Note: This implementation uses Tantivy's QueryParser which handles text analysis and tokenization
fn parse_match_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    // For match queries, we can use the same logic as term queries since QueryParser handles tokenization
    parse_term_query(params, schema)
}

/// Convert a JSON value to a string suitable for query parsing
fn json_value_to_query_string(value: &serde_json::Value) -> crate::Result<String> {
    match value {
        Value::String(s) => {
            if s.is_empty() {
                // Handle empty string case - use quotes to make it a valid query
                Ok("\"\"".to_string())
            } else if s.trim().is_empty() {
                // Handle whitespace-only strings - use quotes to preserve them
                Ok(format!("\"{}\"", s))
            } else if s.contains(':') || s.contains('-') || s.contains('T') {
                // Handle date/time strings and other special formats - use quotes
                Ok(format!("\"{}\"", s))
            } else {
                Ok(s.clone())
            }
        }
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(TantivyError::InvalidArgument(
            "Query value must be string, number, or boolean".to_string(),
        )),
    }
}

/// Convert a JSON value to a string suitable for range query parsing
/// Range queries don't need quotes around values as they're parsed differently
fn json_value_to_range_string(value: &serde_json::Value) -> crate::Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(TantivyError::InvalidArgument(
            "Range query value must be string, number, or boolean".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, FAST, INDEXED, TEXT};
    use serde_json::json;

    fn create_test_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        let _category_field = schema_builder.add_text_field("category", TEXT);
        let _price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
        let _rating_field = schema_builder.add_f64_field("rating", FAST | INDEXED);
        let _in_stock_field = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
        schema_builder.build()
    }

    #[test]
    fn test_parse_term_query() {
        let schema = create_test_schema();

        // Test string term query
        let query_json = json!({ "category": "electronics" });
        let _query = parse_term_query(&query_json, &schema).unwrap();

        // Test numeric term query
        let query_json = json!({ "price": 100 });
        let _query = parse_term_query(&query_json, &schema).unwrap();
    }

    #[test]
    fn test_parse_range_query() {
        let schema = create_test_schema();

        let query_json = json!({ "price": { "gte": 10, "lt": 100 } });
        let _query = parse_range_query(&query_json, &schema).unwrap();
    }

    #[test]
    fn test_parse_elasticsearch_query() {
        let schema = create_test_schema();

        // Test term query
        let query_json = json!({ "term": { "category": "electronics" } });
        let _query = parse_query(&query_json, &schema).unwrap();

        // Test range query
        let query_json = json!({ "range": { "price": { "gte": 10, "lt": 100 } } });
        let _query = parse_query(&query_json, &schema).unwrap();

        // Test match_all query
        let query_json = json!({ "match_all": {} });
        let _query = parse_query(&query_json, &schema).unwrap();
    }

    #[test]
    fn test_json_value_to_query_string() {
        assert_eq!(
            json_value_to_query_string(&json!("hello")).unwrap(),
            "hello"
        );
        assert_eq!(json_value_to_query_string(&json!("")).unwrap(), "\"\"");
        assert_eq!(json_value_to_query_string(&json!(" ")).unwrap(), "\" \"");
        assert_eq!(
            json_value_to_query_string(&json!("2023-01-01T00:00:00Z")).unwrap(),
            "\"2023-01-01T00:00:00Z\""
        );
        assert_eq!(json_value_to_query_string(&json!(42)).unwrap(), "42");
        assert_eq!(json_value_to_query_string(&json!(true)).unwrap(), "true");
    }
}
