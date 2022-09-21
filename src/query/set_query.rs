use std::collections::BTreeSet;

use tantivy_fst::raw::CompiledAddr;
use tantivy_fst::{Automaton, Map};

use crate::query::{AutomatonWeight, Query, Weight};
use crate::schema::Field;
use crate::{Searcher, Term};

/// A Term Set Query matches all of the documents containing any of the Term provided
///
/// Terms not using the right Field are discared.
#[derive(Debug, Clone)]
pub struct TermSetQuery {
    field: Field,
    terms: BTreeSet<Term>,
}

impl TermSetQuery {
    /// Create a Term Set Query
    pub fn new(field: Field, terms: BTreeSet<Term>) -> Self {
        TermSetQuery { field, terms }
    }

    fn specialized_weight(&self) -> crate::Result<AutomatonWeight<SetDfaWrapper>> {
        // BTreeSet are iterated in order and we write to memory, in practice no error can happen.
        let map = Map::from_iter(
            self.terms
                .iter()
                .filter(|key| key.field() == self.field)
                .map(|key| (key.value_bytes(), 0)),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(AutomatonWeight::new(self.field, SetDfaWrapper(map)))
    }
}

impl Query for TermSetQuery {
    fn weight(
        &self,
        _searcher: &Searcher,
        _scoring_enabled: bool,
    ) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}

struct SetDfaWrapper(Map<Vec<u8>>);

impl Automaton for SetDfaWrapper {
    type State = Option<CompiledAddr>;

    fn start(&self) -> Self::State {
        Some(self.0.as_ref().root().addr())
    }
    fn is_match(&self, state: &Self::State) -> bool {
        state
            .map(|s| self.0.as_ref().node(s).is_final())
            .unwrap_or(false)
    }
    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        state.and_then(|state| {
            let state = self.0.as_ref().node(state);
            let transition = state.find_input(byte)?;
            Some(state.transition_addr(transition))
        })
    }

    fn can_match(&self, state: &Self::State) -> bool {
        state.is_some()
    }
}
