use super::Scorer;
use core::SegmentReader;
use Result;
use DocId;
use std::collections::HashSet;
use Term;
use std::collections::BTreeMap;

pub struct MatchingTerms {
    doc_to_terms: BTreeMap<DocId, HashSet<Term>>
}

impl MatchingTerms {
    pub fn from_doc_ids(doc_ids: &[DocId]) -> MatchingTerms {
        MatchingTerms {
            doc_to_terms: doc_ids
                .iter()
                .cloned()
                .map(|doc_id| (doc_id, HashSet::default()))
                .collect()
        }
    }

    pub fn sorted_doc_ids(&self) -> Vec<DocId> {
        self.doc_to_terms.keys().cloned().collect()
    }

    pub fn add_term(&mut self, doc_id: DocId, term: Term) {
        if let Some(terms) = self.doc_to_terms.get_mut(&doc_id) {
            terms.insert(term);
        }
    }
}

/// A Weight is the specialization of a Query
/// for a given set of segments.
///
/// See [`Query`](./trait.Query.html).
pub trait Weight {
    /// Returns the scorer for the given segment.
    /// See [`Query`](./trait.Query.html).
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>>;

    fn matching_terms(&self, reader: &SegmentReader, matching_terms: &mut MatchingTerms) -> Result<()> {
        Ok(())
    }

    /// Returns the number documents within the given `SegmentReader`.
    fn count(&self, reader: &SegmentReader) -> Result<u32> {
        Ok(self.scorer(reader)?.count())
    }
}
