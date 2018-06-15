use error::ErrorKind;
use fst_regex::Regex;
use query::{AutomatonWeight, Query, Weight};
use schema::Field;
use std::clone::Clone;
use Result;
use Searcher;

// A Regex Query matches all of the documents
/// containing a specific term that matches
/// a regex pattern
#[derive(Debug, Clone)]
pub struct RegexQuery {
  regex_pattern: String,
  field: Field,
}

impl RegexQuery {
  /// Creates a new Fuzzy Query
  pub fn new(regex_pattern: String, field: Field) -> RegexQuery {
    RegexQuery {
      regex_pattern,
      field,
    }
  }

  fn specialized_weight(&self) -> Result<AutomatonWeight<Regex>> {
    let automaton = Regex::new(&self.regex_pattern)
      .map_err(|_| ErrorKind::InvalidArgument(self.regex_pattern.clone()))?;

    Ok(AutomatonWeight::new(self.field.clone(), automaton))
  }
}

impl Query for RegexQuery {
  fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<Weight>> {
    Ok(Box::new(self.specialized_weight()?))
  }
}

#[cfg(test)]
mod test {
  use super::RegexQuery;
  use collector::TopCollector;
  use schema::SchemaBuilder;
  use schema::TEXT;
  use tests::assert_nearly_equals;
  use Index;

  #[test]
  pub fn test_regex_query() {
    let mut schema_builder = SchemaBuilder::new();
    let country_field = schema_builder.add_text_field("country", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    {
      let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
      index_writer.add_document(doc!(
                country_field => "japan",
            ));
      index_writer.add_document(doc!(
                country_field => "korea",
            ));
      index_writer.commit().unwrap();
    }
    index.load_searchers().unwrap();
    let searcher = index.searcher();
    {
      let mut collector = TopCollector::with_limit(2);

      let regex_query = RegexQuery::new("jap[ao]n".to_string(), country_field);
      searcher.search(&regex_query, &mut collector).unwrap();
      let scored_docs = collector.score_docs();
      assert_eq!(scored_docs.len(), 1, "Expected only 1 document");
      let (score, _) = scored_docs[0];
      assert_nearly_equals(1f32, score);
    }

    let searcher = index.searcher();
    {
      let mut collector = TopCollector::with_limit(2);

      let regex_query = RegexQuery::new("jap[A-Z]n".to_string(), country_field);
      searcher.search(&regex_query, &mut collector).unwrap();
      let scored_docs = collector.score_docs();
      assert_eq!(scored_docs.len(), 0, "Expected ZERO document");
    }
  }
}
