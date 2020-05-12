use crate::docset::{DocSet, SkipResult};
use crate::query::EmptyScorer;
use crate::query::Scorer;
use crate::DocId;
use crate::Score;

/// Returns the intersection scorer.
///
/// The score associated to the documents is the sum of the
/// score of the `Scorer`s given in argument.
///
/// For better performance, the function uses a
/// specialized implementation if the two
/// shortest scorers are `TermScorer`s.
pub fn intersect_scorers(mut scorers: Vec<Box<dyn Scorer>>) -> Box<dyn Scorer> {
    if scorers.is_empty() {
        return Box::new(EmptyScorer);
    }
    if scorers.len() == 1 {
        return scorers.pop().unwrap();
    }
    // We know that we have at least 2 elements.
    Box::new(Intersection::new(scorers))
}

/// Creates a `DocSet` that iterate through the intersection of two or more `DocSet`s.
pub struct Intersection<TDocSet: DocSet> {
    docsets: Vec<TDocSet>,
}

impl<TDocSet: DocSet> Intersection<TDocSet> {
    pub(crate) fn new(mut docsets: Vec<TDocSet>) -> Intersection<TDocSet> {
        assert!(docsets.len() >= 2);
        docsets.sort_by_key(|scorer| scorer.size_hint());
        Intersection { docsets }
    }

    pub(crate) fn docset_mut(&mut self, ord: usize) -> &mut TDocSet {
        &mut self.docsets[ord]
    }
}

impl<TDocSet: DocSet> DocSet for Intersection<TDocSet> {
    fn advance(&mut self) -> bool {
        if !self.docsets[0].advance() {
            return false;
        }
        let mut candidate_emitter = 0;
        let mut candidate = self.docsets[0].doc();
        'outer: loop {
            for (i, docset) in self.docsets.iter_mut().enumerate() {
                if i == candidate_emitter {
                    continue;
                }
                match docset.skip_next(candidate) {
                    SkipResult::End => {
                        return false;
                    }
                    SkipResult::OverStep => {
                        candidate = docset.doc();
                        candidate_emitter = i;
                        continue 'outer;
                    }
                    SkipResult::Reached => {}
                }
            }
            return true;
        }
    }

    // TODO implement skip_next

    fn doc(&self) -> DocId {
        self.docsets[0].doc()
    }

    fn size_hint(&self) -> u32 {
        self.docsets[0].size_hint()
    }

}

impl<TDocSet: Scorer + DocSet> Scorer for Intersection<TDocSet> {
    fn score(&mut self) -> Score {
        self.docsets.iter_mut().map(Scorer::score).sum::<Score>()
    }


    fn for_each(&mut self, callback: &mut dyn FnMut(DocId, Score)) {
        if !self.docsets[0].advance() {
            return;
        }
        let mut candidate_emitter = 0;
        let mut candidate = self.docsets[0].doc();
        'outer: loop {
            for (i, docset) in self.docsets.iter_mut().enumerate() {
                if i == candidate_emitter {
                    continue;
                }
                match docset.skip_next(candidate) {
                    SkipResult::End => {
                        return;
                    }
                    SkipResult::OverStep => {
                        candidate = docset.doc();
                        candidate_emitter = i;
                        continue 'outer;
                    }
                    SkipResult::Reached => {}
                }
            }

            callback(candidate, self.score());
            if !self.docsets[0].advance() {
                return;
            }
            candidate_emitter = 0;
            candidate = self.docsets[0].doc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Intersection;
    use crate::docset::{DocSet, SkipResult};
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::VecDocSet;

    #[test]
    fn test_intersection() {
        {
            let left = VecDocSet::from(vec![1, 3, 9]);
            let right = VecDocSet::from(vec![3, 4, 9, 18]);
            let mut intersection = Intersection::new(vec![Box::new(left), Box::new(right)]);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 3);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
        {
            let a = Box::new(VecDocSet::from(vec![1, 3, 9]));
            let b = Box::new(VecDocSet::from(vec![3, 4, 9, 18]));
            let c = Box::new(VecDocSet::from(vec![1, 5, 9, 111]));
            let mut intersection = Intersection::new(vec![a, b, c]);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
    }

    #[test]
    fn test_intersection_zero() {
        let left = Box::new(VecDocSet::from(vec![0]));
        let right = Box::new(VecDocSet::from(vec![0]));
        let mut intersection = Intersection::new(vec![left, right]);
        assert!(intersection.advance());
        assert_eq!(intersection.doc(), 0);
    }

    #[test]
    fn test_intersection_skip() {
        let left = Box::new(VecDocSet::from(vec![0, 1, 2, 4]));
        let right = Box::new(VecDocSet::from(vec![2, 5]));
        let mut intersection = Intersection::new(vec![left, right]);
        assert_eq!(intersection.skip_next(2), SkipResult::Reached);
        assert_eq!(intersection.doc(), 2);
    }

    #[test]
    fn test_intersection_skip_against_unoptimized() {
        test_skip_against_unoptimized(
            || {
                let left = VecDocSet::from(vec![4]);
                let right = VecDocSet::from(vec![2, 5]);
                Box::new(Intersection::new(vec![left, right]))
            },
            vec![0, 2, 4, 5, 6],
        );
        test_skip_against_unoptimized(
            || {
                let mut left = VecDocSet::from(vec![1, 4, 5, 6]);
                let mut right = VecDocSet::from(vec![2, 5, 10]);
                left.advance();
                right.advance();
                Box::new(Intersection::new(vec![left, right]))
            },
            vec![0, 1, 2, 3, 4, 5, 6, 7, 10, 11],
        );
        test_skip_against_unoptimized(
            || {
                Box::new(Intersection::new(vec![
                    VecDocSet::from(vec![1, 4, 5, 6]),
                    VecDocSet::from(vec![1, 2, 5, 6]),
                    VecDocSet::from(vec![1, 4, 5, 6]),
                    VecDocSet::from(vec![1, 5, 6]),
                    VecDocSet::from(vec![2, 4, 5, 7, 8]),
                ]))
            },
            vec![0, 1, 2, 3, 4, 5, 6, 7, 10, 11],
        );
    }

    #[test]
    fn test_intersection_empty() {
        let a = VecDocSet::from(vec![1, 3]);
        let b = VecDocSet::from(vec![1, 4]);
        let c = VecDocSet::from(vec![3, 9]);
        let mut intersection = Intersection::new(vec![a, b, c]);
        assert!(!intersection.advance());
    }
}
