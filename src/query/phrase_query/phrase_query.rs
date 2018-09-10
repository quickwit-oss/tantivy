use super::PhraseWeight;
use core::searcher::Searcher;
use error::TantivyError;
use query::bm25::{self, BM25Weight};
use query::Query;
use query::Weight;
use schema::{Field, Term};
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
#[derive(Clone, Debug)]
pub struct PhraseQuery {
    field: Field,
    phrase_terms: Vec<(usize, Term)>,
    bm25_k1: f32,
    bm25_b: f32,
}

impl PhraseQuery {
    /// Creates a new `PhraseQuery` given a list of terms.
    ///
    /// There must be at least two terms, and all terms
    /// must belong to the same field.
    /// Offset for each term will be same as index in the Vector
    pub fn new(terms: Vec<Term>) -> PhraseQuery {
        let terms_with_offset = terms.into_iter().enumerate().collect();
        PhraseQuery::new_with_offset(terms_with_offset)
    }

    /// Creates a new `PhraseQuery` given a list of terms and there offsets.
    ///
    /// Can be used to provide custom offset for each term.
    pub fn new_with_offset(terms: Vec<(usize, Term)>) -> PhraseQuery {
        PhraseQuery::new_with_settings(terms,
            bm25::DEFAULT_K1, bm25::DEFAULT_B)
    }

    /// Creates a new `PhraseQuery` with custom bm25 coefficients
    pub fn new_with_settings(mut terms: Vec<(usize, Term)>,
        bm25_k1: f32, bm25_b: f32)
        -> PhraseQuery
    {
        assert!(
            terms.len() > 1,
            "A phrase query is required to have strictly more than one term."
        );
        terms.sort_by_key(|&(offset, _)| offset);
        let field = terms[0].1.field();
        assert!(
            terms[1..].iter().all(|term| term.1.field() == field),
            "All terms from a phrase query must belong to the same field"
        );
        PhraseQuery {
            field,
            phrase_terms: terms,
            bm25_k1,
            bm25_b,
        }
    }

    /// The `Field` this `PhraseQuery` is targeting.
    pub fn field(&self) -> Field {
        self.field
    }

    /// `Term`s in the phrase without the associated offsets.
    pub fn phrase_terms(&self) -> Vec<Term> {
        self.phrase_terms
            .iter()
            .map(|(_, term)| term.clone())
            .collect::<Vec<Term>>()
    }
}

impl Query for PhraseQuery {
    /// Create the weight associated to a query.
    ///
    /// See [`Weight`](./trait.Weight.html).
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        let schema = searcher.schema();
        let field_entry = schema.get_field_entry(self.field);
        let has_positions = field_entry
            .field_type()
            .get_index_record_option()
            .map(|index_record_option| index_record_option.has_positions())
            .unwrap_or(false);
        if !has_positions {
            let field_name = field_entry.name();
            return Err(TantivyError::SchemaError(format!(
                "Applied phrase query on field {:?}, which does not have positions indexed",
                field_name
            )));
        }
        if scoring_enabled {
            let terms = self.phrase_terms();
            let bm25_weight = BM25Weight::for_terms(searcher, &terms,
                self.bm25_k1, self.bm25_b);
            Ok(Box::new(PhraseWeight::new(
                self.phrase_terms.clone(),
                bm25_weight,
                true,
            )))
        } else {
            Ok(Box::new(PhraseWeight::new(
                self.phrase_terms.clone(),
                BM25Weight::null(),
                false,
            )))
        }
    }
}
