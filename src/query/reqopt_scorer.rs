use crate::docset::{DocSet, SkipResult};
use crate::query::score_combiner::ScoreCombiner;
use crate::query::Scorer;
use crate::DocId;
use crate::Score;
use std::cmp::Ordering;
use std::marker::PhantomData;

/// Given a required scorer and an optional scorer
/// matches all document from the required scorer
/// and complements the score using the optional scorer.
///
/// This is useful for queries like `+somethingrequired somethingoptional`.
///
/// Note that `somethingoptional` has no impact on the `DocSet`.
pub struct RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner> {
    req_scorer: TReqScorer,
    opt_scorer: TOptScorer,
    score_cache: Option<Score>,
    opt_finished: bool,
    _phantom: PhantomData<TScoreCombiner>,
}

impl<TReqScorer, TOptScorer, TScoreCombiner>
    RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner>
where
    TOptScorer: DocSet,
{
    /// Creates a new `RequiredOptionalScorer`.
    pub fn new(
        req_scorer: TReqScorer,
        mut opt_scorer: TOptScorer,
    ) -> RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner> {
        let opt_finished = !opt_scorer.advance();
        RequiredOptionalScorer {
            req_scorer,
            opt_scorer,
            score_cache: None,
            opt_finished,
            _phantom: PhantomData,
        }
    }
}

impl<TReqScorer, TOptScorer, TScoreCombiner> DocSet
    for RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner>
where
    TReqScorer: DocSet,
    TOptScorer: DocSet,
{
    fn advance(&mut self) -> bool {
        self.score_cache = None;
        self.req_scorer.advance()
    }

    fn doc(&self) -> DocId {
        self.req_scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.req_scorer.size_hint()
    }
}

impl<TReqScorer, TOptScorer, TScoreCombiner> Scorer
    for RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner>
where
    TReqScorer: Scorer,
    TOptScorer: Scorer,
    TScoreCombiner: ScoreCombiner,
{
    fn score(&mut self) -> Score {
        if let Some(score) = self.score_cache {
            return score;
        }
        let doc = self.doc();
        let mut score_combiner = TScoreCombiner::default();
        score_combiner.update(&mut self.req_scorer);
        if !self.opt_finished {
            match self.opt_scorer.doc().cmp(&doc) {
                Ordering::Greater => {}
                Ordering::Equal => {
                    score_combiner.update(&mut self.opt_scorer);
                }
                Ordering::Less => match self.opt_scorer.skip_next(doc) {
                    SkipResult::Reached => {
                        score_combiner.update(&mut self.opt_scorer);
                    }
                    SkipResult::End => {
                        self.opt_finished = true;
                    }
                    SkipResult::OverStep => {}
                },
            }
        }
        let score = score_combiner.score();
        self.score_cache = Some(score);
        score
    }
}

#[cfg(test)]
mod tests {
    use super::RequiredOptionalScorer;
    use crate::docset::DocSet;
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::score_combiner::{DoNothingCombiner, SumCombiner};
    use crate::query::ConstScorer;
    use crate::query::Scorer;
    use crate::query::VecDocSet;
    use crate::tests::sample_with_seed;

    #[test]
    fn test_reqopt_scorer_empty() {
        let req = vec![1, 3, 7];
        let mut reqoptscorer: RequiredOptionalScorer<_, _, SumCombiner> =
            RequiredOptionalScorer::new(
                ConstScorer::new(VecDocSet::from(req.clone())),
                ConstScorer::new(VecDocSet::from(vec![])),
            );
        let mut docs = vec![];
        while reqoptscorer.advance() {
            docs.push(reqoptscorer.doc());
        }
        assert_eq!(docs, req);
    }

    #[test]
    fn test_reqopt_scorer() {
        let mut reqoptscorer: RequiredOptionalScorer<_, _, SumCombiner> =
            RequiredOptionalScorer::new(
                ConstScorer::new(VecDocSet::from(vec![1, 3, 7, 8, 9, 10, 13, 15])),
                ConstScorer::new(VecDocSet::from(vec![1, 2, 7, 11, 12, 15])),
            );
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 1);
            assert_eq!(reqoptscorer.score(), 2f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 3);
            assert_eq!(reqoptscorer.score(), 1f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 7);
            assert_eq!(reqoptscorer.score(), 2f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 8);
            assert_eq!(reqoptscorer.score(), 1f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 9);
            assert_eq!(reqoptscorer.score(), 1f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 10);
            assert_eq!(reqoptscorer.score(), 1f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 13);
            assert_eq!(reqoptscorer.score(), 1f32);
        }
        {
            assert!(reqoptscorer.advance());
            assert_eq!(reqoptscorer.doc(), 15);
            assert_eq!(reqoptscorer.score(), 2f32);
        }
        assert!(!reqoptscorer.advance());
    }

    #[test]
    fn test_reqopt_scorer_skip() {
        let req_docs = sample_with_seed(10_000, 0.02, 1);
        let opt_docs = sample_with_seed(10_000, 0.02, 2);
        let skip_docs = sample_with_seed(10_000, 0.001, 3);
        test_skip_against_unoptimized(
            || {
                Box::new(RequiredOptionalScorer::<_, _, DoNothingCombiner>::new(
                    ConstScorer::new(VecDocSet::from(req_docs.clone())),
                    ConstScorer::new(VecDocSet::from(opt_docs.clone())),
                ))
            },
            skip_docs,
        );
    }

}
