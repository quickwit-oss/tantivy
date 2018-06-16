use super::term_weight::TermWeight;
use query::bm25::BM25Weight;
use query::Query;
use query::Weight;
use schema::IndexRecordOption;
use Result;
use Searcher;
use Term;

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
/// use tantivy::schema::{SchemaBuilder, TEXT, IndexRecordOption};
/// use tantivy::{Index, Result, Term};
/// use tantivy::collector::{CountCollector, TopCollector, chain};
/// use tantivy::query::TermQuery;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
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
///
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     {
///         let mut top_collector = TopCollector::with_limit(2);
///         let mut count_collector = CountCollector::default();
///         {
///             let mut collectors = chain().push(&mut top_collector).push(&mut count_collector);
///             let query = TermQuery::new(
///                 Term::from_field_text(title, "diary"),
///                 IndexRecordOption::Basic,
///             );
///             searcher.search(&query, &mut collectors).unwrap();
///         }
///         assert_eq!(count_collector.count(), 2);
///         assert!(top_collector.at_capacity());
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct TermQuery {
    term: Term,
    index_record_option: IndexRecordOption,
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
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        Ok(Box::new(self.specialized_weight(searcher, scoring_enabled)))
    }
}
