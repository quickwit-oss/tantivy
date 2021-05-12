use super::MoreLikeThis;

use crate::{
    query::{Query, Weight},
    schema::{Field, FieldValue},
    DocAddress, Result, Searcher,
};

/// A query that matches all of the documents similar to a document
/// or a set of field values provided.
///
/// # Examples
///
/// ```
/// use tantivy::DocAddress;
/// use tantivy::query::MoreLikeThisQuery;
///
/// let query = MoreLikeThisQuery::builder()
///     .with_min_doc_frequency(1)
///     .with_max_doc_frequency(10)
///     .with_min_term_frequency(1)
///     .with_min_word_length(2)
///     .with_max_word_length(5)
///     .with_boost_factor(1.0)
///     .with_stop_words(vec!["for".to_string()])
///     .with_document(DocAddress::new(2, 1));
///
/// ```
#[derive(Debug, Clone)]
pub struct MoreLikeThisQuery {
    mlt: MoreLikeThis,
    target: TargetDocument,
}

#[derive(Debug, PartialEq, Clone)]
enum TargetDocument {
    DocumentAdress(DocAddress),
    DocumentFields(Vec<(Field, Vec<FieldValue>)>),
}

impl MoreLikeThisQuery {
    /// Creates a new builder.
    pub fn builder() -> MoreLikeThisQueryBuilder {
        MoreLikeThisQueryBuilder::default()
    }
}

impl Query for MoreLikeThisQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        match &self.target {
            TargetDocument::DocumentAdress(doc_address) => self
                .mlt
                .query_with_document(searcher, *doc_address)?
                .weight(searcher, scoring_enabled),
            TargetDocument::DocumentFields(doc_fields) => self
                .mlt
                .query_with_document_fields(searcher, doc_fields)?
                .weight(searcher, scoring_enabled),
        }
    }
}

/// The builder for more-like-this query
#[derive(Debug, Clone)]
pub struct MoreLikeThisQueryBuilder {
    mlt: MoreLikeThis,
}

impl Default for MoreLikeThisQueryBuilder {
    fn default() -> Self {
        Self {
            mlt: MoreLikeThis::default(),
        }
    }
}

impl MoreLikeThisQueryBuilder {
    /// Sets the minimum document frequency.
    ///
    /// The resulting query will ignore words which do not occur
    /// in at least this many docs.
    pub fn with_min_doc_frequency(mut self, value: u64) -> Self {
        self.mlt.min_doc_frequency = Some(value);
        self
    }

    /// Sets the maximum document frequency.
    ///
    /// The resulting query will ignore words which occur
    /// in more than this many docs.
    pub fn with_max_doc_frequency(mut self, value: u64) -> Self {
        self.mlt.max_doc_frequency = Some(value);
        self
    }

    /// Sets the minimum term frequency.
    ///
    /// The resulting query will ignore words less
    /// frequent that this number.
    pub fn with_min_term_frequency(mut self, value: usize) -> Self {
        self.mlt.min_term_frequency = Some(value);
        self
    }

    /// Sets the maximum query terms.
    ///
    /// The resulting query will not return a query with more clause than this.
    pub fn with_max_query_terms(mut self, value: usize) -> Self {
        self.mlt.max_query_terms = Some(value);
        self
    }

    /// Sets the minimum word length.
    ///
    /// The resulting query will ignore words shorter than this length.
    pub fn with_min_word_length(mut self, value: usize) -> Self {
        self.mlt.min_word_length = Some(value);
        self
    }

    /// Sets the maximum word length.
    ///
    /// The resulting query will ignore words longer than this length.
    pub fn with_max_word_length(mut self, value: usize) -> Self {
        self.mlt.max_word_length = Some(value);
        self
    }

    /// Sets the boost factor
    ///
    /// The boost factor used by the resulting query for boosting terms.
    pub fn with_boost_factor(mut self, value: f32) -> Self {
        self.mlt.boost_factor = Some(value);
        self
    }

    /// Sets the set of stop words
    ///
    /// The resulting query will ignore these set of words.
    pub fn with_stop_words(mut self, value: Vec<String>) -> Self {
        self.mlt.stop_words = value;
        self
    }

    /// Sets the document address
    /// Returns the constructed [`MoreLikeThisQuery`]
    ///
    /// This document will be used to collect field values, extract frequent terms
    /// needed for composing the query.
    ///
    /// Note that field values will only be collected from stored fields in the index.
    /// You can construct your own field values from any source.
    pub fn with_document(self, doc_address: DocAddress) -> MoreLikeThisQuery {
        MoreLikeThisQuery {
            mlt: self.mlt,
            target: TargetDocument::DocumentAdress(doc_address),
        }
    }

    /// Sets the document fields
    /// Returns the constructed [`MoreLikeThisQuery`]
    ///
    /// This represents the list field values possibly collected from multiple documents
    /// that will be used to compose the resulting query.
    /// This interface is meant to be used when you want to provide your own set of fields
    /// not necessarily from a specific document.
    pub fn with_document_fields(
        self,
        doc_fields: Vec<(Field, Vec<FieldValue>)>,
    ) -> MoreLikeThisQuery {
        MoreLikeThisQuery {
            mlt: self.mlt,
            target: TargetDocument::DocumentFields(doc_fields),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MoreLikeThisQuery;
    use super::TargetDocument;
    use crate::collector::TopDocs;
    use crate::schema::{Schema, STORED, TEXT};
    use crate::DocAddress;
    use crate::Index;

    fn create_test_index() -> Index {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let body = schema_builder.add_text_field("body", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(title => "aaa", body => "the old man and the sea"));
        index_writer.add_document(doc!(title => "bbb", body => "an old man sailing on the sea"));
        index_writer.add_document(doc!(title => "ccc", body=> "send this message to alice"));
        index_writer.add_document(doc!(title => "ddd", body=> "a lady was riding and old bike"));
        index_writer.add_document(doc!(title => "eee", body=> "Yes, my lady."));
        index_writer.commit().unwrap();
        index
    }

    #[test]
    fn test_more_like_this_query_builder() {
        // default settings
        let query = MoreLikeThisQuery::builder().with_document_fields(vec![]);

        assert_eq!(query.mlt.min_doc_frequency, Some(5));
        assert_eq!(query.mlt.max_doc_frequency, None);
        assert_eq!(query.mlt.min_term_frequency, Some(2));
        assert_eq!(query.mlt.max_query_terms, Some(25));
        assert_eq!(query.mlt.min_word_length, None);
        assert_eq!(query.mlt.max_word_length, None);
        assert_eq!(query.mlt.boost_factor, Some(1.0));
        assert_eq!(query.mlt.stop_words, Vec::<String>::new());
        assert_eq!(query.target, TargetDocument::DocumentFields(vec![]));

        // custom settings
        let query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(2)
            .with_max_doc_frequency(5)
            .with_min_term_frequency(2)
            .with_min_word_length(2)
            .with_max_word_length(4)
            .with_boost_factor(0.5)
            .with_stop_words(vec!["all".to_string(), "for".to_string()])
            .with_document(DocAddress::new(1, 2));

        assert_eq!(query.mlt.min_doc_frequency, Some(2));
        assert_eq!(query.mlt.max_doc_frequency, Some(5));
        assert_eq!(query.mlt.min_term_frequency, Some(2));
        assert_eq!(query.mlt.min_word_length, Some(2));
        assert_eq!(query.mlt.max_word_length, Some(4));
        assert_eq!(query.mlt.boost_factor, Some(0.5));
        assert_eq!(
            query.mlt.stop_words,
            vec!["all".to_string(), "for".to_string()]
        );
        assert_eq!(
            query.target,
            TargetDocument::DocumentAdress(DocAddress::new(1, 2))
        );
    }

    #[test]
    fn test_more_like_this_query() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // search base 1st doc with words [sea, and] skipping [old]
        let query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(1)
            .with_max_doc_frequency(10)
            .with_min_term_frequency(1)
            .with_min_word_length(2)
            .with_max_word_length(5)
            .with_boost_factor(1.0)
            .with_stop_words(vec!["old".to_string()])
            .with_document(DocAddress::new(0, 0));
        let top_docs = searcher.search(&query, &TopDocs::with_limit(5)).unwrap();
        let mut doc_ids: Vec<_> = top_docs.iter().map(|item| item.1.doc_id).collect();
        doc_ids.sort();

        assert_eq!(doc_ids.len(), 3);
        assert_eq!(doc_ids, vec![0, 1, 3]);

        // search base 5th doc with words [lady]
        let query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(1)
            .with_max_doc_frequency(10)
            .with_min_term_frequency(1)
            .with_min_word_length(2)
            .with_max_word_length(5)
            .with_boost_factor(1.0)
            .with_document(DocAddress::new(0, 4));
        let top_docs = searcher.search(&query, &TopDocs::with_limit(5)).unwrap();
        let mut doc_ids: Vec<_> = top_docs.iter().map(|item| item.1.doc_id).collect();
        doc_ids.sort();

        assert_eq!(doc_ids.len(), 2);
        assert_eq!(doc_ids, vec![3, 4]);
    }
}
