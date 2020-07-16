use crate::docset::{DocSet, TERMINATED};
use crate::query::Scorer;
use crate::DocId;
use crate::Score;

#[inline(always)]
fn is_within<TDocSetExclude: DocSet>(docset: &mut TDocSetExclude, doc: DocId) -> bool {
    docset.doc() <= doc && docset.seek(doc) == doc
}

/// Filters a given `DocSet` by removing the docs from a given `DocSet`.
///
/// The excluding docset has no impact on scoring.
pub struct Exclude<TDocSet, TDocSetExclude> {
    underlying_docset: TDocSet,
    excluding_docset: TDocSetExclude,
}

impl<TDocSet, TDocSetExclude> Exclude<TDocSet, TDocSetExclude>
where
    TDocSet: DocSet,
    TDocSetExclude: DocSet,
{
    /// Creates a new `ExcludeScorer`
    pub fn new(
        mut underlying_docset: TDocSet,
        mut excluding_docset: TDocSetExclude,
    ) -> Exclude<TDocSet, TDocSetExclude> {
        while underlying_docset.doc() != TERMINATED {
            let target = underlying_docset.doc();
            if !is_within(&mut excluding_docset, target) {
                break;
            }
            underlying_docset.advance();
        }
        Exclude {
            underlying_docset,
            excluding_docset,
        }
    }
}

impl<TDocSet, TDocSetExclude> DocSet for Exclude<TDocSet, TDocSetExclude>
where
    TDocSet: DocSet,
    TDocSetExclude: DocSet,
{
    fn advance(&mut self) -> DocId {
        loop {
            let candidate = self.underlying_docset.advance();
            if candidate == TERMINATED {
                return TERMINATED;
            }
            if !is_within(&mut self.excluding_docset, candidate) {
                return candidate;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let candidate = self.underlying_docset.seek(target);
        if candidate == TERMINATED {
            return TERMINATED;
        }
        if !is_within(&mut self.excluding_docset, candidate) {
            return candidate;
        }
        self.advance()
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
        while exclude_scorer.doc() != TERMINATED {
            els.push(exclude_scorer.doc());
            exclude_scorer.advance();
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
            vec![5, 8, 10, 15, 24],
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
