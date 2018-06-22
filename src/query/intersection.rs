use docset::{DocSet, SkipResult};
use downcast::Downcast;
use query::term_query::TermScorer;
use query::EmptyScorer;
use query::Scorer;
use std::borrow::Borrow;
use DocId;
use Score;

/// Returns the intersection scorer.
///
/// The score associated to the documents is the sum of the
/// score of the `Scorer`s given in argument.
///
/// For better performance, the function uses a
/// specialized implementation if the two
/// shortest scorers are `TermScorer`s.
pub fn intersect_scorers(mut scorers: Vec<Box<Scorer>>) -> Box<Scorer> {
    let num_docsets = scorers.len();
    scorers.sort_by(|left, right| right.size_hint().cmp(&left.size_hint()));
    let rarest_opt = scorers.pop();
    let second_rarest_opt = scorers.pop();
    scorers.reverse();
    match (rarest_opt, second_rarest_opt) {
        (None, None) => Box::new(EmptyScorer),
        (Some(single_docset), None) => single_docset,
        (Some(left), Some(right)) => {
            {
                if [&left, &right].into_iter().all(|scorer| {
                    let scorer_ref: &Scorer = (*scorer).borrow();
                    Downcast::<TermScorer>::is_type(scorer_ref)
                }) {
                    let left = *Downcast::<TermScorer>::downcast(left).unwrap();
                    let right = *Downcast::<TermScorer>::downcast(right).unwrap();
                    return Box::new(Intersection {
                        left,
                        right,
                        others: scorers,
                        num_docsets,
                    });
                }
            }
            return Box::new(Intersection {
                left,
                right,
                others: scorers,
                num_docsets,
            });
        }
        _ => {
            unreachable!();
        }
    }
}

/// Creates a `DocSet` that iterator through the intersection of two `DocSet`s.
pub struct Intersection<TDocSet: DocSet, TOtherDocSet: DocSet = Box<Scorer>> {
    left: TDocSet,
    right: TDocSet,
    others: Vec<TOtherDocSet>,
    num_docsets: usize,
}

impl<TDocSet: DocSet> Intersection<TDocSet, TDocSet> {
    pub(crate) fn new(mut docsets: Vec<TDocSet>) -> Intersection<TDocSet, TDocSet> {
        let num_docsets = docsets.len();
        assert!(num_docsets >= 2);
        docsets.sort_by(|left, right| right.size_hint().cmp(&left.size_hint()));
        let left = docsets.pop().unwrap();
        let right = docsets.pop().unwrap();
        docsets.reverse();
        Intersection {
            left,
            right,
            others: docsets,
            num_docsets,
        }
    }
}

impl<TDocSet: DocSet> Intersection<TDocSet, TDocSet> {
    pub(crate) fn docset_mut_specialized(&mut self, ord: usize) -> &mut TDocSet {
        match ord {
            0 => &mut self.left,
            1 => &mut self.right,
            n => &mut self.others[n - 2],
        }
    }
}

impl<TDocSet: DocSet, TOtherDocSet: DocSet> Intersection<TDocSet, TOtherDocSet> {
    pub(crate) fn docset_mut(&mut self, ord: usize) -> &mut DocSet {
        match ord {
            0 => &mut self.left,
            1 => &mut self.right,
            n => &mut self.others[n - 2],
        }
    }
}

impl<TDocSet: DocSet, TOtherDocSet: DocSet> DocSet for Intersection<TDocSet, TOtherDocSet> {
    #[allow(never_loop)]
    fn advance(&mut self) -> bool {
        let (left, right) = (&mut self.left, &mut self.right);

        if !left.advance() {
            return false;
        }

        let mut candidate = left.doc();
        let mut other_candidate_ord: usize = usize::max_value();

        'outer: loop {
            // In the first part we look for a document in the intersection
            // of the two rarest `DocSet` in the intersection.
            loop {
                match right.skip_next(candidate) {
                    SkipResult::Reached => {
                        break;
                    }
                    SkipResult::OverStep => {
                        candidate = right.doc();
                        other_candidate_ord = usize::max_value();
                    }
                    SkipResult::End => {
                        return false;
                    }
                }

                match left.skip_next(candidate) {
                    SkipResult::Reached => {
                        break;
                    }
                    SkipResult::OverStep => {
                        candidate = left.doc();
                        other_candidate_ord = usize::max_value();
                    }
                    SkipResult::End => {
                        return false;
                    }
                }
            }
            // test the remaining scorers;
            for (ord, docset) in self.others.iter_mut().enumerate() {
                if ord != other_candidate_ord {
                    // `candidate_ord` is already at the
                    // right position.
                    //
                    // Calling `skip_next` would advance this docset
                    // and miss it.
                    match docset.skip_next(candidate) {
                        SkipResult::Reached => {}
                        SkipResult::OverStep => {
                            // this is not in the intersection,
                            // let's update our candidate.
                            candidate = docset.doc();
                            match left.skip_next(candidate) {
                                SkipResult::Reached => {
                                    other_candidate_ord = ord;
                                }
                                SkipResult::OverStep => {
                                    candidate = left.doc();
                                    other_candidate_ord = usize::max_value();
                                }
                                SkipResult::End => {
                                    return false;
                                }
                            }
                            continue 'outer;
                        }
                        SkipResult::End => {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        // We optimize skipping by skipping every single member
        // of the intersection to target.
        let mut current_target: DocId = target;
        let mut current_ord = self.num_docsets;

        'outer: loop {
            for ord in 0..self.num_docsets {
                let docset = self.docset_mut(ord);
                if ord == current_ord {
                    continue;
                }
                match docset.skip_next(current_target) {
                    SkipResult::End => {
                        return SkipResult::End;
                    }
                    SkipResult::OverStep => {
                        // update the target
                        // for the remaining members of the intersection.
                        current_target = docset.doc();
                        current_ord = ord;
                        continue 'outer;
                    }
                    SkipResult::Reached => {}
                }
            }
            if target == current_target {
                return SkipResult::Reached;
            } else {
                assert!(current_target > target);
                return SkipResult::OverStep;
            }
        }
    }

    fn doc(&self) -> DocId {
        self.left.doc()
    }

    fn size_hint(&self) -> u32 {
        self.left.size_hint()
    }
}

impl<TScorer, TOtherScorer> Scorer for Intersection<TScorer, TOtherScorer>
where
    TScorer: Scorer,
    TOtherScorer: Scorer,
{
    fn score(&mut self) -> Score {
        self.left.score()
            + self.right.score()
            + self.others.iter_mut().map(Scorer::score).sum::<Score>()
    }
}

#[cfg(test)]
mod tests {
    use super::Intersection;
    use docset::{DocSet, SkipResult};
    use postings::tests::test_skip_against_unoptimized;
    use query::VecDocSet;

    #[test]
    fn test_intersection() {
        {
            let left = VecDocSet::from(vec![1, 3, 9]);
            let right = VecDocSet::from(vec![3, 4, 9, 18]);
            let mut intersection = Intersection::new(vec![left, right]);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 3);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
        {
            let a = VecDocSet::from(vec![1, 3, 9]);
            let b = VecDocSet::from(vec![3, 4, 9, 18]);
            let c = VecDocSet::from(vec![1, 5, 9, 111]);
            let mut intersection = Intersection::new(vec![a, b, c]);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
    }

    #[test]
    fn test_intersection_zero() {
        let left = VecDocSet::from(vec![0]);
        let right = VecDocSet::from(vec![0]);
        let mut intersection = Intersection::new(vec![left, right]);
        assert!(intersection.advance());
        assert_eq!(intersection.doc(), 0);
    }

    #[test]
    fn test_intersection_skip() {
        let left = VecDocSet::from(vec![0, 1, 2, 4]);
        let right = VecDocSet::from(vec![2, 5]);
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
