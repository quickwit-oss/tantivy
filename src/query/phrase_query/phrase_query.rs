use schema::Term;
use query::Query;
use core::searcher::Searcher;
use super::PhraseWeight;
use query::Weight;
use Result;

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

impl Query for PhraseQuery {
    /// Create the weight associated to a query.
    ///
    /// See [`Weight`](./trait.Weight.html).
    fn weight(&self, _searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        Ok(box PhraseWeight::new(self.phrase_terms.clone(), scoring_enabled))
    }
}

impl From<Vec<Term>> for PhraseQuery {
    fn from(phrase_terms: Vec<Term>) -> PhraseQuery {
        assert!(phrase_terms.len() > 1);
        PhraseQuery { phrase_terms }
    }
}
