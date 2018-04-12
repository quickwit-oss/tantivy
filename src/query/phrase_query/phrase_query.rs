use schema::Term;
use query::Query;
use core::searcher::Searcher;
use super::PhraseWeight;
use query::Weight;
use Result;
use query::bm25::BM25Weight;

/// `PhraseQuery` matches a specific sequence of words.
///
/// For instance the phrase query for `"part time"` will match
/// the sentence
///
/// **Alan just got a part time job.**
///
/// On the other hand it will not match the sentence.
///
/// **This is my favorite part of the job.**
///
/// Using a `PhraseQuery` on a field requires positions
/// to be indexed for this field.
///
#[derive(Debug)]
pub struct PhraseQuery {
    phrase_terms: Vec<Term>,
}

impl PhraseQuery {

    /// Creates a new `PhraseQuery` given a list of terms.
    ///
    /// There must be at least two terms, and all terms
    /// must belong to the same field.
    pub fn new(terms: Vec<Term>) -> PhraseQuery {
        assert!(terms.len() > 1, "A phrase query is required to have strictly more than one term.");
        assert!(terms[1..].iter().all(|term| term.field() == terms[0].field()), "All terms from a phrase query must belong to the same field");
        PhraseQuery {
            phrase_terms: terms
        }
    }
}

impl Query for PhraseQuery {
    /// Create the weight associated to a query.
    ///
    /// See [`Weight`](./trait.Weight.html).
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        let terms = self.phrase_terms.clone();
        if scoring_enabled {
            let bm25_weight = BM25Weight::for_terms(searcher, &terms);
            Ok(Box::new(PhraseWeight::new(
                terms,
                bm25_weight,
                true
            )))
        } else {
            Ok(Box::new(PhraseWeight::new(terms, BM25Weight::null(), false)))
        }

    }
}
