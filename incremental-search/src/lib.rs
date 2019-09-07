use tantivy::query::{BooleanQuery, FuzzyTermQuery, EmptyQuery};
use derive_builder::Builder;
use std::str::FromStr;
use tantivy::query::{FuzzyConfiguration, FuzzyConfigurationBuilder, Query, Occur};
use tantivy::schema::Field;
use tantivy::{Searcher, TantivyError, DocAddress, Term, Document};
use tantivy::collector::TopDocs;
use std::ops::Deref;


#[derive(Debug)]
pub struct IncrementalSearchQuery {
    pub terms: Vec<String>,
    pub last_is_prefix: bool,
}

impl IncrementalSearchQuery {
    pub fn fuzzy_configurations(&self) -> Vec<FuzzyConfigurations> {
        if self.terms.is_empty() {
            return Vec::default();
        }
        let single_term_confs: Vec<FuzzyConfigurationBuilder> = (0u8..3u8)
            .map(|d: u8| {
                let mut builder = FuzzyConfigurationBuilder::default();
                builder.distance(d).transposition_cost_one(true);
                builder
            })
            .collect();
        let mut configurations: Vec<Vec<FuzzyConfigurationBuilder>> = single_term_confs
            .iter()
            .map(|conf| vec![conf.clone()])
            .collect();
        let mut new_configurations = Vec::new();
        for _ in 1..self.terms.len() {
            new_configurations.clear();
            for single_term_conf in &single_term_confs {
                for configuration in &configurations {
                    let mut new_configuration: Vec<FuzzyConfigurationBuilder> = configuration.clone();
                    new_configuration.push(single_term_conf.clone());
                    new_configurations.push(new_configuration);
                }
            }
            std::mem::swap(&mut configurations, &mut new_configurations);
        }
        if self.last_is_prefix {
            for configuration in &mut configurations {
                if let Some(last_conf) = configuration.last_mut() {
                    last_conf.prefix(true);
                }
            }
        }
        let mut fuzzy_configurations: Vec<FuzzyConfigurations> = configurations
            .into_iter()
            .map(FuzzyConfigurations::from)
            .collect();
        fuzzy_configurations.sort_by(|left, right| left.cost.partial_cmp(&right.cost).unwrap());
        fuzzy_configurations
    }

    fn search_query(&self, fields: &[Field], configurations: FuzzyConfigurations) -> Box<dyn Query> {
        if self.terms.is_empty() {
            Box::new(EmptyQuery)
        } else if self.terms.len() == 1 {
            build_query_for_fields(fields, &self.terms[0], &configurations.configurations[0])
        } else {
            Box::new(BooleanQuery::from(self.terms.iter()
                .zip(configurations.configurations.iter())
                .map(|(term, configuration)|
                         (Occur::Must, build_query_for_fields(fields, &term, &configuration))
                )
                .collect::<Vec<_>>()))
        }
    }
}

#[derive(Debug)]
pub struct FuzzyConfigurations {
    configurations: Vec<FuzzyConfiguration>,
    cost: f64,
}


fn compute_cost(fuzzy_confs: &[FuzzyConfiguration]) -> f64 {
    fuzzy_confs
        .iter()
        .map(|fuzzy_conf| {
            let weight = if fuzzy_conf.prefix { 30f64 } else { 5f64 };
            weight * f64::from(fuzzy_conf.distance)
        })
        .sum()
}

impl From<Vec<FuzzyConfigurationBuilder>> for FuzzyConfigurations {
    fn from(fuzzy_conf_builder: Vec<FuzzyConfigurationBuilder>) -> FuzzyConfigurations {
        let configurations = fuzzy_conf_builder
            .into_iter()
            .map(|conf| conf.build().unwrap())
            .collect::<Vec<FuzzyConfiguration>>();
        let cost = compute_cost(&configurations);
        FuzzyConfigurations {
            configurations,
            cost,
        }
    }
}

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
        let terms: Vec<String> = query_str
            .split_whitespace()
            .map(ToString::to_string)
            .collect();
        Ok(IncrementalSearchQuery {
            terms,
            last_is_prefix: query_str
                .chars()
                .last()
                .map(|c| !c.is_whitespace())
                .unwrap_or(false),
        })
    }
}

fn build_query_for_fields(fields: &[Field], term_text: &str, conf: &FuzzyConfiguration) -> Box<dyn Query> {
    assert!(fields.len() > 0);
    if fields.len() > 1 {
        let term_queries: Vec<(Occur, Box<dyn Query>)> = fields
            .iter()
            .map(|&field| {
                let term = Term::from_field_text(field, term_text);
                let query = FuzzyTermQuery::new_from_configuration(term, conf.clone());
                let boxed_query: Box<dyn Query> = Box::new(query);
                (Occur::Must, boxed_query)
            })
            .collect();
        Box::new(BooleanQuery::from(term_queries))
    } else {
        let term = Term::from_field_text(fields[0], term_text);
        Box::new( FuzzyTermQuery::new_from_configuration(term, conf.clone()))
    }

}

pub struct IncrementalSearchResult {
    pub docs: Vec<Document>
}

#[derive(Builder, Default)]
pub struct IncrementalSearch {
    nhits: usize,
    #[builder(default)]
    search_fields: Vec<Field>,
    #[builder(default)]
    return_fields: Vec<Field>,
}

impl IncrementalSearch {

    pub fn search<S: Deref<Target=Searcher>>(
        &self,
        query: &str,
        searcher: &S,
    ) -> tantivy::Result<IncrementalSearchResult> {
        let searcher = searcher.deref();
        let inc_search_query: IncrementalSearchQuery =
            FromStr::from_str(query).map_err(Into::<TantivyError>::into)?;

        let mut results: Vec<DocAddress> = Vec::default();
        let mut remaining = self.nhits;
        for fuzzy_conf in inc_search_query.fuzzy_configurations() {
            if remaining == 0 {
                break;
            }
            let query = inc_search_query.search_query(&self.search_fields[..], fuzzy_conf);
            let new_docs = searcher.search(query.as_ref(), &TopDocs::with_limit(remaining))?;
            // TODO(pmasurel) remove already added docs.
            results.extend(new_docs.into_iter()
                .map(|(_, doc_address)| doc_address));
            remaining = self.nhits - results.len();
            if remaining == 0 {
                break;
            }
        }
        let docs: Vec<Document> = results.into_iter()
            .map(|doc_address: DocAddress| searcher.doc(doc_address))
            .collect::<tantivy::Result<_>>()?;
        Ok(IncrementalSearchResult {
            docs
        })
    }
}
#[cfg(test)]
mod tests {
    use tantivy::doc;
    use crate::{IncrementalSearch, IncrementalSearchBuilder, IncrementalSearchQuery};
    use std::str::FromStr;
    use tantivy::schema::{SchemaBuilder, TEXT, STORED};
    use tantivy::Index;

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
        assert!(!query.last_is_prefix);
    }

    #[test]
    fn test_incremental_search_query_parse_trailing_whitespace() {
        let query = IncrementalSearchQuery::from_str("hello happy tax pa ").unwrap();
        assert_eq!(query.terms, vec!["hello", "happy", "tax", "pa"]);
        assert!(!query.last_is_prefix);
    }

    #[test]
    fn test_incremental_search_query_parse_unicode_whitespace() {
        let query = IncrementalSearchQuery::from_str("hello  happy tax pa ").unwrap();
        assert_eq!(query.terms, vec!["hello", "happy", "tax", "pa"]);
        assert!(!query.last_is_prefix);
    }

    #[test]
    fn test_incremental_search_query_parse() {
        let query = IncrementalSearchQuery::from_str("hello happy tax pa").unwrap();
        assert_eq!(query.terms, vec!["hello", "happy", "tax", "pa"]);
        assert!(query.last_is_prefix);
    }

    #[test]
    fn test_blop() {
        let mut schema_builder = SchemaBuilder::new();
        let body = schema_builder.add_text_field("body", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 30_000_000).unwrap();
        index_writer.add_document(doc!(body=> "hello happy tax payer"));
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let incremental_search: IncrementalSearch = IncrementalSearchBuilder::default()
            .nhits(1)
            .search_fields(vec![body])
            .build()
            .unwrap();
        let top_docs = incremental_search.search("hello hapy t", &searcher).unwrap();
        assert_eq!(top_docs.docs.len(), 1);
    }
}
