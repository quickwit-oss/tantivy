use crate::fastfield::DeleteBitSet;
use crate::query::explanation::does_not_match;
use crate::query::{Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, Searcher, SegmentReader, Term};
use std::collections::BTreeSet;
use std::fmt;

/// `BoostQuery` is a wrapper over a query used to boost its score.
///
/// The document set matched by the `BoostQuery` is strictly the same as the underlying query.
/// The score of each document, is the score of the underlying query multiplied by the `boost`
/// factor.
pub struct BoostQuery {
    query: Box<dyn Query>,
    boost: Score,
}

impl BoostQuery {
    /// Builds a boost query.
    pub fn new(query: Box<dyn Query>, boost: Score) -> BoostQuery {
        BoostQuery { query, boost }
    }
}

impl Clone for BoostQuery {
    fn clone(&self) -> Self {
        BoostQuery {
            query: self.query.box_clone(),
            boost: self.boost,
        }
    }
}

impl fmt::Debug for BoostQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Boost(query={:?}, boost={})", self.query, self.boost)
    }
}

impl Query for BoostQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> crate::Result<Box<dyn Weight>> {
        let weight_without_boost = self.query.weight(searcher, scoring_enabled)?;
        let boosted_weight = if scoring_enabled {
            Box::new(BoostWeight::new(weight_without_boost, self.boost))
        } else {
            weight_without_boost
        };
        Ok(boosted_weight)
    }

    fn query_terms(&self, term_set: &mut BTreeSet<Term>) {
        self.query.query_terms(term_set)
    }
}

pub(crate) struct BoostWeight {
    weight: Box<dyn Weight>,
    boost: Score,
}

impl BoostWeight {
    pub fn new(weight: Box<dyn Weight>, boost: Score) -> Self {
        BoostWeight { weight, boost }
    }
}

impl Weight for BoostWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        self.weight.scorer(reader, boost * self.boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        let mut explanation =
            Explanation::new(format!("Boost x{} of ...", self.boost), scorer.score());
        let underlying_explanation = self.weight.explain(reader, doc)?;
        explanation.add_detail(underlying_explanation);
        Ok(explanation)
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        self.weight.count(reader)
    }
}

pub(crate) struct BoostScorer<S: Scorer> {
    underlying: S,
    boost: Score,
}

impl<S: Scorer> BoostScorer<S> {
    pub fn new(underlying: S, boost: Score) -> BoostScorer<S> {
        BoostScorer { underlying, boost }
    }
}

impl<S: Scorer> DocSet for BoostScorer<S> {
    fn advance(&mut self) -> DocId {
        self.underlying.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.underlying.seek(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId]) -> usize {
        self.underlying.fill_buffer(buffer)
    }

    fn doc(&self) -> u32 {
        self.underlying.doc()
    }

    fn size_hint(&self) -> u32 {
        self.underlying.size_hint()
    }

    fn count(&mut self, delete_bitset: &DeleteBitSet) -> u32 {
        self.underlying.count(delete_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        self.underlying.count_including_deleted()
    }
}

impl<S: Scorer> Scorer for BoostScorer<S> {
    fn score(&mut self) -> Score {
        self.underlying.score() * self.boost
    }
}

#[cfg(test)]
mod tests {
    use super::BoostQuery;
    use crate::query::{AllQuery, Query};
    use crate::schema::Schema;
    use crate::{DocAddress, Document, Index};

    #[test]
    fn test_boost_query_explain() {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(Document::new());
        assert!(index_writer.commit().is_ok());
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query = BoostQuery::new(Box::new(AllQuery), 0.2);
        let explanation = query.explain(&searcher, DocAddress::new(0, 0u32)).unwrap();
        assert_eq!(
            explanation.to_pretty_json(),
            "{\n  \"value\": 0.2,\n  \"description\": \"Boost x0.2 of ...\",\n  \"details\": [\n    {\n      \"value\": 1.0,\n      \"description\": \"AllQuery\",\n      \"context\": []\n    }\n  ],\n  \"context\": []\n}"
        )
    }
}
