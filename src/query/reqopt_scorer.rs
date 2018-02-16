use DocId;
use DocSet;
use query::Scorer;
use Score;
use postings::SkipResult;
use std::cmp::Ordering;

/// Given a required scorer and an optional scorer
/// matches all document from the required scorer
/// and complements the score using the optional scorer.
///
/// This is useful for queries like `+somethingrequired somethingoptional`.
///
/// Note that `somethingoptional` has no impact on the `DocSet`.
pub struct RequiredOptionalScorer<TReqScorer, TOptScorer> {
    req_scorer: TReqScorer,
    opt_scorer: TOptScorer,
    score_cache: Option<Score>,
    opt_finished: bool,
}

impl<TReqScorer, TOptScorer> RequiredOptionalScorer<TReqScorer, TOptScorer>
    where TOptScorer: DocSet {
    fn new(req_scorer: TReqScorer, mut opt_scorer: TOptScorer) -> RequiredOptionalScorer<TReqScorer, TOptScorer> {
        let opt_finished = !opt_scorer.advance();
        RequiredOptionalScorer {
            req_scorer,
            opt_scorer,
            score_cache: None,
            opt_finished
        }
    }
}

impl<TReqScorer, TOptScorer> DocSet for RequiredOptionalScorer<TReqScorer, TOptScorer>
    where TReqScorer: DocSet, TOptScorer: DocSet
{
    fn advance(&mut self) -> bool {
        self.score_cache = None;
        if self.req_scorer.advance() {
            true
        } else {
            false
        }
    }

    fn doc(&self) -> DocId {
        self.req_scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.req_scorer.size_hint()
    }
}


impl<TReqScorer, TOptScorer> Scorer for RequiredOptionalScorer<TReqScorer, TOptScorer>
    where TReqScorer: Scorer, TOptScorer: Scorer {

    fn score(&mut self) -> Score {
        if let Some(score) = self.score_cache {
            return score;
        }
        let doc = self.doc();
        let mut score = self.req_scorer.score();
        if self.opt_finished {
            return score;
        }
        match self.opt_scorer.doc().cmp(&doc) {
            Ordering::Greater => {}
            Ordering::Equal => {
                score += self.opt_scorer.score();
            }
            Ordering::Less => {
                match self.opt_scorer.skip_next(doc) {
                    SkipResult::Reached => {
                        score += self.opt_scorer.score();
                    }
                    SkipResult::End => {
                        self.opt_finished = true;
                    }
                    SkipResult::OverStep => {}
                }
            }
        }
        self.score_cache = Some(score);
        score
    }
}


#[cfg(test)]
mod tests {
    use tests::sample_with_seed;
    use super::RequiredOptionalScorer;
    use postings::VecPostings;
    use query::ConstScorer;
    use DocSet;
    use postings::tests::test_skip_against_unoptimized;
    use query::Scorer;


    #[test]
    fn test_reqopt_scorer_empty() {
        let req = vec![1, 3, 7];
        let mut reqoptscorer = RequiredOptionalScorer::new(
            ConstScorer::new(VecPostings::from(req.clone())),
            ConstScorer::new(VecPostings::from(vec![]))
        );
        let mut docs = vec![];
        while reqoptscorer.advance() {
            docs.push(reqoptscorer.doc());
        }
        assert_eq!(docs, req);
    }

    #[test]
    fn test_reqopt_scorer() {
        let mut reqoptscorer = RequiredOptionalScorer::new(
            ConstScorer::new(VecPostings::from(vec![1,3,7,8,9,10,13,15])),
            ConstScorer::new(VecPostings::from(vec![1,2,7,11,12,15]))
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
        test_skip_against_unoptimized(||
            box RequiredOptionalScorer::new(
                ConstScorer::new(VecPostings::from(req_docs.clone())),
                ConstScorer::new(VecPostings::from(opt_docs.clone()))
            ), skip_docs);
    }


}