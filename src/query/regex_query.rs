use crate::error::TantivyError;
use crate::query::{AutomatonWeight, Query, Weight};
use crate::schema::Field;
use crate::Searcher;
use std::clone::Clone;
use std::sync::Arc;
use tantivy_fst::Regex;

/// A Regex Query matches all of the documents
/// containing a specific term that matches
/// a regex pattern.
///
/// ```rust
/// use tantivy::collector::Count;
/// use tantivy::query::RegexQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, Term};
///
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
/// {
///     let mut index_writer = index.writer(3_000_000)?;
///     index_writer.add_document(doc!(
///         title => "The Name of the Wind",
///     ));
///     index_writer.add_document(doc!(
///         title => "The Diary of Muadib",
///     ));
///     index_writer.add_document(doc!(
///         title => "A Dairy Cow",
///     ));
///     index_writer.add_document(doc!(
///         title => "The Diary of a Young Girl",
///     ));
///     index_writer.commit().unwrap();
/// }
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let term = Term::from_field_text(title, "Diary");
/// let query = RegexQuery::from_pattern("d[ai]{2}ry", title)?;
/// let count = searcher.search(&query, &Count)?;
/// assert_eq!(count, 3);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct RegexQuery {
    regex: Arc<Regex>,
    field: Field,
}

impl RegexQuery {
    /// Creates a new RegexQuery from a given pattern
    pub fn from_pattern(regex_pattern: &str, field: Field) -> crate::Result<Self> {
        let regex = Regex::new(&regex_pattern)
            .map_err(|_| TantivyError::InvalidArgument(regex_pattern.to_string()))?;
        Ok(RegexQuery::from_regex(regex, field))
    }

    /// Creates a new RegexQuery from a fully built Regex
    pub fn from_regex<T: Into<Arc<Regex>>>(regex: T, field: Field) -> Self {
        RegexQuery {
            regex: regex.into(),
            field,
        }
    }

    fn specialized_weight(&self) -> AutomatonWeight<Regex> {
        AutomatonWeight::new(self.field, self.regex.clone())
    }
}

impl Query for RegexQuery {
    fn weight(
        &self,
        _searcher: &Searcher,
        _scoring_enabled: bool,
    ) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()))
    }
}

#[cfg(test)]
mod test {
    use super::RegexQuery;
    use crate::assert_nearly_equals;
    use crate::collector::TopDocs;
    use crate::schema::TEXT;
    use crate::schema::{Field, Schema};
    use crate::{Index, IndexReader};
    use std::sync::Arc;
    use tantivy_fst::Regex;

    fn build_test_index() -> (IndexReader, Field) {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.add_document(doc!(
                country_field => "japan",
            ));
            index_writer.add_document(doc!(
                country_field => "korea",
            ));
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();

        (reader, country_field)
    }

    fn verify_regex_query(
        query_matching_one: RegexQuery,
        query_matching_zero: RegexQuery,
        reader: IndexReader,
    ) {
        let searcher = reader.searcher();
        {
            let scored_docs = searcher
                .search(&query_matching_one, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(scored_docs.len(), 1, "Expected only 1 document");
            let (score, _) = scored_docs[0];
            assert_nearly_equals!(1.0, score);
        }
        let top_docs = searcher
            .search(&query_matching_zero, &TopDocs::with_limit(2))
            .unwrap();
        assert!(top_docs.is_empty(), "Expected ZERO document");
    }

    #[test]
    pub fn test_regex_query() {
        let (reader, field) = build_test_index();

        let matching_one = RegexQuery::from_pattern("jap[ao]n", field).unwrap();
        let matching_zero = RegexQuery::from_pattern("jap[A-Z]n", field).unwrap();

        verify_regex_query(matching_one, matching_zero, reader);
    }

    #[test]
    pub fn test_construct_from_regex() {
        let (reader, field) = build_test_index();

        let matching_one = RegexQuery::from_regex(Regex::new("jap[ao]n").unwrap(), field);
        let matching_zero = RegexQuery::from_regex(Regex::new("jap[A-Z]n").unwrap(), field);

        verify_regex_query(matching_one, matching_zero, reader);
    }

    #[test]
    pub fn test_construct_from_reused_regex() {
        let r1 = Arc::new(Regex::new("jap[ao]n").unwrap());
        let r2 = Arc::new(Regex::new("jap[A-Z]n").unwrap());

        let (reader, field) = build_test_index();

        let matching_one = RegexQuery::from_regex(r1.clone(), field);
        let matching_zero = RegexQuery::from_regex(r2.clone(), field);

        verify_regex_query(matching_one, matching_zero, reader.clone());

        let matching_one = RegexQuery::from_regex(r1.clone(), field);
        let matching_zero = RegexQuery::from_regex(r2.clone(), field);

        verify_regex_query(matching_one, matching_zero, reader.clone());
    }
}
