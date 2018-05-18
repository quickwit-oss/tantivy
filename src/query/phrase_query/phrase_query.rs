use super::PhraseWeight;
use core::searcher::Searcher;
use error::ErrorKind;
use query::bm25::BM25Weight;
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
    phrase_terms: Vec<Term>,
}

impl PhraseQuery {
    /// Creates a new `PhraseQuery` given a list of terms.
    ///
    /// There must be at least two terms, and all terms
    /// must belong to the same field.
    pub fn new(terms: Vec<Term>) -> PhraseQuery {
        assert!(
            terms.len() > 1,
            "A phrase query is required to have strictly more than one term."
        );
        let field = terms[0].field();
        assert!(
            terms[1..].iter().all(|term| term.field() == field),
            "All terms from a phrase query must belong to the same field"
        );
        PhraseQuery {
            field,
            phrase_terms: terms,
        }
    }

    /// The `Field` this `PhraseQuery` is targeting.
    pub fn field(&self) -> Field {
        self.field
    }

    /// The `Term`s in the phrase making up this `PhraseQuery`.
    pub fn phrase_terms(&self) -> &[Term] {
        &self.phrase_terms[..]
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
            bail!(ErrorKind::SchemaError(format!(
                "Applied phrase query on field {:?}, which does not have positions indexed",
                field_name
            )))
        }
        let terms = self.phrase_terms.clone();
        if scoring_enabled {
            let bm25_weight = BM25Weight::for_terms(searcher, &terms);
            Ok(Box::new(PhraseWeight::new(terms, bm25_weight, true)))
        } else {
            Ok(Box::new(PhraseWeight::new(
                terms,
                BM25Weight::null(),
                false,
            )))
        }
    }
}
