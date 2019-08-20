use crate::error::TantivyError;
use crate::query::{AutomatonWeight, Query, Weight};
use crate::schema::Field;
use crate::Result;
use crate::Searcher;
use std::clone::Clone;
use std::sync::Arc;
use tantivy_fst::{Automaton, Regex};

/// A Regex Query matches all of the documents
/// containing a specific term that matches
/// a regex pattern.
///
/// ```rust
/// use tantivy::collector::Count;
/// use tantivy::query::RegexQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, Result, Term};
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer(3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ));
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ));
///         index_writer.commit().unwrap();
///     }
///
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     let term = Term::from_field_text(title, "Diary");
///     let query = RegexQuery::new("d[ai]{2}ry".to_string(), title);
///     let count = searcher.search(&query, &Count)?;
///     assert_eq!(count, 3);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RegexQuery {
    regex: ShareableRegex,
    field: Field,
}

#[derive(Debug, Clone)]
struct ShareableRegex(Arc<Regex>);

impl Automaton for ShareableRegex {
    type State = <Regex as Automaton>::State;

    #[inline]
    fn start(&self) -> Self::State {
        self.0.start()
    }

    #[inline]
    fn is_match(&self, state: &Self::State) -> bool {
        self.0.is_match(state)
    }

    #[inline]
    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.accept(state, byte)
    }
}

impl RegexQuery {
    /// Creates a new RegexQuery
    pub fn new(regex_pattern: String, field: Field) -> RegexQuery {
        let regex = Regex::new(&regex_pattern)
            .map_err(|_| TantivyError::InvalidArgument(regex_pattern.clone()))
            .expect("can't build Regex"); // TODO: convert into a Result

        RegexQuery {
            regex: ShareableRegex(Arc::new(regex)),
            field,
        }
    }

    fn specialized_weight(&self) -> AutomatonWeight<ShareableRegex> {
        AutomatonWeight::new(self.field, self.regex.clone())
    }
}

impl Query for RegexQuery {
    fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()))
    }
}

#[cfg(test)]
mod test {
    use super::RegexQuery;
    use crate::collector::TopDocs;
    use crate::schema::Schema;
    use crate::schema::TEXT;
    use crate::tests::assert_nearly_equals;
    use crate::Index;

    #[test]
    pub fn test_regex_query() {
        let mut schema_builder = Schema::builder();
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
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        {
            let regex_query = RegexQuery::new("jap[ao]n".to_string(), country_field);
            let scored_docs = searcher
                .search(&regex_query, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(scored_docs.len(), 1, "Expected only 1 document");
            let (score, _) = scored_docs[0];
            assert_nearly_equals(1f32, score);
        }
        let regex_query = RegexQuery::new("jap[A-Z]n".to_string(), country_field);
        let top_docs = searcher
            .search(&regex_query, &TopDocs::with_limit(2))
            .unwrap();
        assert!(top_docs.is_empty(), "Expected ZERO document");
    }
}
