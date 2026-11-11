use std::clone::Clone;
use std::fmt;
use std::sync::Arc;

use tantivy_fst::Regex;

use crate::error::TantivyError;
use crate::query::{AutomatonWeight, EnableScoring, MatchedTermsSink, Query, Weight};
use crate::schema::Field;

/// A Regex Query matches all of the documents
/// containing a specific term that matches
/// a regex pattern.
///
/// Wildcard queries (e.g. ho*se) can be achieved
/// by converting them to their regex counterparts.
///
/// ```rust
/// use tantivy::collector::Count;
/// use tantivy::query::RegexQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, IndexWriter, Term};
///
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
/// {
///     let mut index_writer: IndexWriter = index.writer(15_000_000)?;
///     index_writer.add_document(doc!(
///         title => "The Name of the Wind",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "The Diary of Muadib",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "A Dairy Cow",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "The Diary of a Young Girl",
///     ))?;
///     index_writer.commit()?;
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
#[derive(Clone)]
pub struct RegexQuery {
    regex: Arc<Regex>,
    field: Field,
    matched_terms_sink: Option<MatchedTermsSink>,
}

impl fmt::Debug for RegexQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("RegexQuery");
        debug_struct
            .field("regex", &self.regex)
            .field("field", &self.field);
        if let Some(sink) = &self.matched_terms_sink {
            debug_struct.field("matched_terms_sink", sink);
        }
        debug_struct.finish()
    }
}

impl RegexQuery {
    /// Creates a new RegexQuery from a given pattern
    pub fn from_pattern(regex_pattern: &str, field: Field) -> crate::Result<Self> {
        let regex = Regex::new(regex_pattern)
            .map_err(|err| TantivyError::InvalidArgument(format!("RegexQueryError: {err}")))?;
        Ok(RegexQuery::from_regex(regex, field))
    }

    /// Creates a new RegexQuery from a fully built Regex
    pub fn from_regex<T: Into<Arc<Regex>>>(regex: T, field: Field) -> Self {
        RegexQuery {
            regex: regex.into(),
            field,
            matched_terms_sink: None,
        }
    }

    /// Records the terms this query matches in the term dictionary into `sink` while the search
    /// runs. See [`MatchedTermsSink`].
    pub fn set_matched_terms_sink(&mut self, sink: MatchedTermsSink) {
        self.matched_terms_sink = Some(sink);
    }

    fn specialized_weight(&self) -> AutomatonWeight<Regex> {
        let mut weight = AutomatonWeight::new(self.field, self.regex.clone());
        if let Some(sink) = &self.matched_terms_sink {
            weight.set_matched_terms_sink(sink.clone());
        }
        weight
    }
}

impl Query for RegexQuery {
    fn weight(&self, _enabled_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tantivy_fst::Regex;

    use super::RegexQuery;
    use crate::collector::{Count, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::MatchedTermsSink;
    use crate::schema::{Field, Schema, TEXT};
    use crate::{assert_nearly_equals, Index, IndexReader, IndexWriter};

    fn build_test_index() -> crate::Result<(IndexReader, Field)> {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            index_writer.add_document(doc!(
                country_field => "japan",
            ))?;
            index_writer.add_document(doc!(
                country_field => "korea",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;

        Ok((reader, country_field))
    }

    fn verify_regex_query(
        query_matching_one: RegexQuery,
        query_matching_zero: RegexQuery,
        reader: IndexReader,
    ) {
        let searcher = reader.searcher();
        {
            let scored_docs = searcher
                .search(
                    &query_matching_one,
                    &TopDocs::with_limit(2).order_by_score(),
                )
                .unwrap();
            assert_eq!(scored_docs.len(), 1, "Expected only 1 document");
            let (score, _) = scored_docs[0];
            assert_nearly_equals!(1.0, score);
        }
        let top_docs = searcher
            .search(
                &query_matching_zero,
                &TopDocs::with_limit(2).order_by_score(),
            )
            .unwrap();
        assert!(top_docs.is_empty(), "Expected ZERO document");
    }

    #[test]
    pub fn test_regex_query() -> crate::Result<()> {
        let (reader, field) = build_test_index()?;

        let matching_one = RegexQuery::from_pattern("jap[ao]n", field)?;
        let matching_zero = RegexQuery::from_pattern("jap[A-Z]n", field)?;
        verify_regex_query(matching_one, matching_zero, reader);
        Ok(())
    }

    #[test]
    pub fn test_construct_from_regex() -> crate::Result<()> {
        let (reader, field) = build_test_index()?;

        let matching_one = RegexQuery::from_regex(Regex::new("jap[ao]n").unwrap(), field);
        let matching_zero = RegexQuery::from_regex(Regex::new("jap[A-Z]n").unwrap(), field);

        verify_regex_query(matching_one, matching_zero, reader);
        Ok(())
    }

    #[test]
    pub fn test_construct_from_reused_regex() -> crate::Result<()> {
        let r1 = Arc::new(Regex::new("jap[ao]n").unwrap());
        let r2 = Arc::new(Regex::new("jap[A-Z]n").unwrap());

        let (reader, field) = build_test_index()?;

        let matching_one = RegexQuery::from_regex(r1.clone(), field);
        let matching_zero = RegexQuery::from_regex(r2.clone(), field);

        verify_regex_query(matching_one, matching_zero, reader.clone());

        let matching_one = RegexQuery::from_regex(r1, field);
        let matching_zero = RegexQuery::from_regex(r2, field);

        verify_regex_query(matching_one, matching_zero, reader);
        Ok(())
    }

    #[test]
    pub fn test_regex_query_matched_terms_sink_multiple_segments() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("text", TEXT);
        let mut index = Index::create_in_ram(schema_builder.build());
        index.set_multithread_executor(2)?;
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        for segment_words in [["japan", "korea"], ["japan", "jamaica"], ["china", "chile"]] {
            for word in segment_words {
                index_writer.add_document(doc!(field => word))?;
            }
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 3);

        let mut query = RegexQuery::from_pattern("ja.*", field)?;
        let sink = MatchedTermsSink::new();
        query.set_matched_terms_sink(sink.clone());
        assert_eq!(searcher.search(&query, &Count)?, 3);
        // "japan" matched in two segments and is recorded once
        assert_eq!(sink.len(), 2);
        assert_eq!(format!("{sink:?}"), "MatchedTermsSink { len: 2 }");
        // searching again does not add duplicates
        assert_eq!(searcher.search(&query, &Count)?, 3);
        assert_eq!(
            sink.take_bytes(),
            vec![b"jamaica".to_vec(), b"japan".to_vec()]
        );
        assert!(sink.is_empty());

        // a query without a sink matches the same documents
        let query = RegexQuery::from_pattern("ja.*", field)?;
        assert_eq!(searcher.search(&query, &Count)?, 3);
        Ok(())
    }

    #[test]
    pub fn test_pattern_error() {
        let (_reader, field) = build_test_index().unwrap();

        match RegexQuery::from_pattern(r"(foo", field) {
            Err(crate::TantivyError::InvalidArgument(msg)) => {
                assert!(msg.contains("error: unclosed group"))
            }
            res => panic!("unexpected result: {res:?}"),
        }
    }
}
