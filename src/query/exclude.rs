use crate::docset::{DocSet, SkipResult};
use crate::query::Scorer;
use crate::DocId;
use crate::Score;

#[derive(Clone, Copy, Debug)]
enum State {
    ExcludeOne(DocId),
    Finished,
}

/// Filters a given `DocSet` by removing the docs from a given `DocSet`.
///
/// The excluding docset has no impact on scoring.
pub struct Exclude<TDocSet, TDocSetExclude> {
    underlying_docset: TDocSet,
    excluding_docset: TDocSetExclude,
    excluding_state: State,
}

impl<TDocSet, TDocSetExclude> Exclude<TDocSet, TDocSetExclude>
where
    TDocSetExclude: DocSet,
{
    /// Creates a new `ExcludeScorer`
    pub fn new(
        underlying_docset: TDocSet,
        mut excluding_docset: TDocSetExclude,
    ) -> Exclude<TDocSet, TDocSetExclude> {
        let state = if excluding_docset.advance() {
            State::ExcludeOne(excluding_docset.doc())
        } else {
            State::Finished
        };
        Exclude {
            underlying_docset,
            excluding_docset,
            excluding_state: state,
        }
    }
}

impl<TDocSet, TDocSetExclude> Exclude<TDocSet, TDocSetExclude>
where
    TDocSet: DocSet,
    TDocSetExclude: DocSet,
{
    /// Returns true iff the doc is not removed.
    ///
    /// The method has to be called with non strictly
    /// increasing `doc`.
    fn accept(&mut self) -> bool {
        let doc = self.underlying_docset.doc();
        match self.excluding_state {
            State::ExcludeOne(excluded_doc) => {
                if doc == excluded_doc {
                    false
                } else if excluded_doc > doc {
                    true
                } else {
                    match self.excluding_docset.skip_next(doc) {
                        SkipResult::OverStep => {
                            self.excluding_state = State::ExcludeOne(self.excluding_docset.doc());
                            true
                        }
                        SkipResult::End => {
                            self.excluding_state = State::Finished;
                            true
                        }
                        SkipResult::Reached => false,
                    }
                }
            }
            State::Finished => true,
        }
    }
}

impl<TDocSet, TDocSetExclude> DocSet for Exclude<TDocSet, TDocSetExclude>
where
    TDocSet: DocSet,
    TDocSetExclude: DocSet,
{
    fn advance(&mut self) -> bool {
        while self.underlying_docset.advance() {
            if self.accept() {
                return true;
            }
        }
        false
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let underlying_skip_result = self.underlying_docset.skip_next(target);
        if underlying_skip_result == SkipResult::End {
            return SkipResult::End;
        }
        if self.accept() {
            underlying_skip_result
        } else if self.advance() {
            SkipResult::OverStep
        } else {
            SkipResult::End
        }
    }

    fn doc(&self) -> DocId {
        self.underlying_docset.doc()
    }

    /// `.size_hint()` directly returns the size
    /// of the underlying docset without taking in account
    /// the fact that docs might be deleted.
    fn size_hint(&self) -> u32 {
        self.underlying_docset.size_hint()
    }
}

impl<TScorer, TDocSetExclude> Scorer for Exclude<TScorer, TDocSetExclude>
where
    TScorer: Scorer,
    TDocSetExclude: DocSet + 'static,
{
    fn score(&mut self) -> Score {
        self.underlying_docset.score()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::VecDocSet;
    use crate::tests::sample_with_seed;

    #[test]
    fn test_exclude() {
        let mut exclude_scorer = Exclude::new(
            VecDocSet::from(vec![1, 2, 5, 8, 10, 15, 24]),
            VecDocSet::from(vec![1, 2, 3, 10, 16, 24]),
        );
        let mut els = vec![];
        while exclude_scorer.advance() {
            els.push(exclude_scorer.doc());
        }
        assert_eq!(els, vec![5, 8, 15]);
    }

    #[test]
    fn test_exclude_skip() {
        test_skip_against_unoptimized(
            || {
                Box::new(Exclude::new(
                    VecDocSet::from(vec![1, 2, 5, 8, 10, 15, 24]),
                    VecDocSet::from(vec![1, 2, 3, 10, 16, 24]),
                ))
            },
            vec![1, 2, 5, 8, 10, 15, 24],
        );
    }

    #[test]
    fn test_exclude_skip_random() {
        let sample_include = sample_with_seed(10_000, 0.1, 1);
        let sample_exclude = sample_with_seed(10_000, 0.05, 2);
        let sample_skip = sample_with_seed(10_000, 0.005, 3);
        test_skip_against_unoptimized(
            || {
                Box::new(Exclude::new(
                    VecDocSet::from(sample_include.clone()),
                    VecDocSet::from(sample_exclude.clone()),
                ))
            },
            sample_skip,
        );
    }

}
