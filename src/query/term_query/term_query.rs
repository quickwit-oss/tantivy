use super::term_weight::TermWeight;
use crate::query::bm25::BM25Weight;
use crate::query::Query;
use crate::query::Weight;
use crate::schema::IndexRecordOption;
use crate::Result;
use crate::Searcher;
use crate::Term;
use std::collections::BTreeSet;
use std::fmt;

/// A Term query matches all of the documents
/// containing a specific term.
///
/// The score associated is defined as
/// `idf` *  sqrt(`term_freq` / `field norm`)
/// in which :
/// * `idf`        - inverse document frequency.
/// * `term_freq`  - number of occurrences of the term in the field
/// * `field norm` - number of tokens in the field.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{Schema, TEXT, IndexRecordOption};
/// use tantivy::{Index, Result, Term};
/// use tantivy::collector::{Count, TopDocs};
/// use tantivy::query::TermQuery;
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
///         index_writer.commit()?;
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     let query = TermQuery::new(
///         Term::from_field_text(title, "diary"),
///         IndexRecordOption::Basic,
///     );
///     let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count)).unwrap();
///     assert_eq!(count, 2);
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct TermQuery {
    term: Term,
    index_record_option: IndexRecordOption,
}

impl fmt::Debug for TermQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TermQuery({:?})", self.term)
    }
}

impl TermQuery {
    /// Creates a new term query.
    pub fn new(term: Term, segment_postings_options: IndexRecordOption) -> TermQuery {
        TermQuery {
            term,
            index_record_option: segment_postings_options,
        }
    }

    /// The `Term` this query is built out of.
    pub fn term(&self) -> &Term {
        &self.term
    }

    /// Returns a weight object.
    ///
    /// While `.weight(...)` returns a boxed trait object,
    /// this method return a specific implementation.
    /// This is useful for optimization purpose.
    pub fn specialized_weight(&self, searcher: &Searcher, scoring_enabled: bool) -> TermWeight {
        let term = self.term.clone();
        let bm25_weight = BM25Weight::for_terms(searcher, &[term]);
        let index_record_option = if scoring_enabled {
            self.index_record_option
        } else {
            IndexRecordOption::Basic
        };
        TermWeight::new(self.term.clone(), index_record_option, bm25_weight)
    }
}

impl Query for TermQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight(searcher, scoring_enabled)))
    }
    fn query_terms(&self, term_set: &mut BTreeSet<Term>) {
        term_set.insert(self.term.clone());
    }
}
