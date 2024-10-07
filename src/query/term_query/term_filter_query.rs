use std::fmt;

use super::term_weight::TermWeight;
use crate::query::bm25::Bm25Weight;
use crate::query::{EnableScoring, Explanation, Query, Weight};
use crate::schema::{IndexRecordOption, Schema};
use crate::Term;

#[derive(Clone)]
pub struct TermFilterQuery {
    term: Term,
}

impl fmt::Debug for TermFilterQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TermFilterQuery({:?})", self.term)
    }
}

impl TermFilterQuery {
    /// Creates a new term filter query.
    pub fn new(term: Term) -> TermFilterQuery {
        TermFilterQuery {
            term
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
    pub fn specialized_weight(&self, schema: &Schema) -> crate::Result<TermWeight> {
        let field_entry = schema.get_field_entry(self.term.field());
        if !field_entry.is_indexed() {
            let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
            return Err(crate::TantivyError::SchemaError(error_msg));
        }
        let bm25_weight = Bm25Weight::new(Explanation::new("<no score>", 1.0f32), 1.0f32);
        let index_record_option = IndexRecordOption::Basic;

        Ok(TermWeight::new(
            self.term.clone(),
            index_record_option,
            bm25_weight,
            false,
        ))
    }
}

impl Query for TermFilterQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight(enable_scoring.schema())?))
    }
    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        visitor(&self.term, false);
    }
}
