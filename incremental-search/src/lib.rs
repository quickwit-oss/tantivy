use derive_builder::Builder;
use std::str::FromStr;
use tantivy::schema::Field;
use tantivy::{Searcher, TantivyError};

#[derive(Builder, Default)]
pub struct IncrementalSearch {
    nhits: usize,
    #[builder(default)]
    search_fields: Vec<Field>,
    #[builder(default)]
    return_fields: Vec<Field>,
}

#[derive(Debug)]
pub struct IncrementalSearchQuery {
    pub terms: Vec<String>,
    pub prefix: Option<String>,
}

// TODO have a smarter, more robust query parser.
// This is a first stab

#[derive(Debug)]
pub struct ParseIncrementalQueryError;

impl Into<TantivyError> for ParseIncrementalQueryError {
    fn into(self) -> TantivyError {
        TantivyError::InvalidArgument(format!("Invalid query: {:?}", self))
    }
}

impl FromStr for IncrementalSearchQuery {
    type Err = ParseIncrementalQueryError;

    fn from_str(query_str: &str) -> Result<Self, Self::Err> {
        let mut terms: Vec<String> = query_str
            .split_whitespace()
            .map(ToString::to_string)
            .collect();
        if query_str.ends_with(|c: char| c.is_whitespace()) {
            Ok(IncrementalSearchQuery {
                terms,
                prefix: None,
            })
        } else {
            let prefix = terms.pop();
            Ok(IncrementalSearchQuery { terms, prefix })
        }
    }
}

pub struct IncrementalSearchResult;

impl IncrementalSearch {
    pub fn search(
        &self,
        query: &str,
        searcher: &Searcher,
    ) -> tantivy::Result<IncrementalSearchResult> {
        let query: IncrementalSearchQuery =
            FromStr::from_str(query).map_err(Into::<TantivyError>::into)?;
        let result = IncrementalSearchResult;

        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::{IncrementalSearch, IncrementalSearchBuilder};
    use crate::IncrementalSearchQuery;
    use std::str::FromStr;

    #[test]
    fn test_incremental_search() {
        let incremental_search = IncrementalSearchBuilder::default()
            .nhits(10)
            .build()
            .unwrap();
    }

    #[test]
    fn test_incremental_search_query_parse_empty() {
        let query = IncrementalSearchQuery::from_str("").unwrap();
        assert_eq!(query.terms, Vec::<String>::new());
        assert_eq!(query.prefix, None);
    }

    #[test]
    fn test_incremental_search_query_parse_trailing_whitespace() {
        let query = IncrementalSearchQuery::from_str("hello happy tax pa ").unwrap();
        assert_eq!(query.terms, vec!["hello", "happy", "tax", "pa"]);
        assert_eq!(query.prefix, None);
    }

    #[test]
    fn test_incremental_search_query_parse_unicode_whitespace() {
        let query = IncrementalSearchQuery::from_str("hello  happy tax pa ").unwrap();
        assert_eq!(query.terms, vec!["hello", "happy", "tax", "pa"]);
        assert_eq!(query.prefix, None);
    }

    #[test]
    fn test_incremental_search_query_parse() {
        let query = IncrementalSearchQuery::from_str("hello happy tax pa").unwrap();
        assert_eq!(query.terms, vec!["hello", "happy", "tax"]);
        assert_eq!(query.prefix, Some("pa".to_string()));
    }
}
