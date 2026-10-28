use crate::docset::{DocSet, TERMINATED};
use crate::postings::Postings;
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::Scorer;
use crate::{DocId, Score};

/// A `SimpleUnion` is a `DocSet` that is the union of multiple `DocSet`.
/// Unlike `BufferedUnion`, it doesn't do any horizon precomputation.
///
/// For that reason SimpleUnion is a good choice for queries that skip a lot.
pub struct SimpleUnion<TDocSet, TScoreCombiner = DoNothingCombiner> {
    docsets: Vec<TDocSet>,
    score_combiner: TScoreCombiner,
    doc: DocId,
}

impl<TDocSet: DocSet> SimpleUnion<TDocSet, DoNothingCombiner> {
    pub(crate) fn build(docsets: Vec<TDocSet>) -> Self {
        Self::build_with_combiner(docsets, DoNothingCombiner)
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> SimpleUnion<TScorer, TScoreCombiner> {
    pub(crate) fn build_scored(
        scorers: Vec<TScorer>,
        score_combiner_fn: impl FnOnce() -> TScoreCombiner,
    ) -> Self {
        Self::build_with_combiner(scorers, score_combiner_fn())
    }
}

impl<TDocSet: DocSet, TScoreCombiner> SimpleUnion<TDocSet, TScoreCombiner> {
    fn build_with_combiner(mut docsets: Vec<TDocSet>, score_combiner: TScoreCombiner) -> Self {
        docsets.retain(|docset| docset.doc() != TERMINATED);
        let mut docset = SimpleUnion {
            docsets,
            score_combiner,
            doc: 0,
        };
        docset.initialize_first_doc_id();
        docset
    }

    fn initialize_first_doc_id(&mut self) {
        let mut next_doc = TERMINATED;

        for docset in &self.docsets {
            next_doc = next_doc.min(docset.doc());
        }
        self.doc = next_doc;
    }

    fn advance_to_next(&mut self) -> DocId {
        let mut next_doc = TERMINATED;

        for docset in &mut self.docsets {
            if docset.doc() <= self.doc {
                docset.advance();
            }
            next_doc = next_doc.min(docset.doc());
        }
        self.doc = next_doc;
        self.doc
    }
}

impl<TDocSet: Postings, TScoreCombiner: Send + 'static> Postings
    for SimpleUnion<TDocSet, TScoreCombiner>
{
    fn term_freq(&self) -> u32 {
        let mut term_freq = 0;
        for docset in &self.docsets {
            let doc = docset.doc();
            if doc == self.doc {
                term_freq += docset.term_freq();
            }
        }
        term_freq
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        for docset in &mut self.docsets {
            let doc = docset.doc();
            if doc == self.doc {
                docset.append_positions_with_offset(offset, output);
            }
        }
        output.sort_unstable();
        output.dedup();
    }
}

impl<TDocSet: DocSet, TScoreCombiner: Send> DocSet for SimpleUnion<TDocSet, TScoreCombiner> {
    fn advance(&mut self) -> DocId {
        self.advance_to_next();
        self.doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.doc = TERMINATED;
        for docset in &mut self.docsets {
            if docset.doc() < target {
                docset.seek(target);
            }
            if docset.doc() < self.doc {
                self.doc = docset.doc();
            }
        }
        self.doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        // TODO: use estimate_union
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .max()
            .unwrap_or(0u32)
    }

    fn cost(&self) -> u64 {
        self.docsets.iter().map(|docset| docset.cost()).sum()
    }

    fn count_including_deleted(&mut self) -> u32 {
        if self.doc == TERMINATED {
            return 0u32;
        }
        let mut count = 1u32;
        while self.advance_to_next() != TERMINATED {
            count += 1;
        }
        count
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> Scorer
    for SimpleUnion<TScorer, TScoreCombiner>
{
    fn score(&mut self) -> Score {
        self.score_combiner.clear();
        // Child order determines floating-point accumulation order, as in BufferedUnionScorer.
        for scorer in &mut self.docsets {
            if scorer.doc() == self.doc {
                self.score_combiner.update(scorer);
            }
        }
        self.score_combiner.score()
    }
}
