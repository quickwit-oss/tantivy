use crate::docset::{DocSet, TERMINATED};
use crate::query::term_query::TermScorer;
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
    scorers.sort_by_key(|scorer| scorer.size_hint());
    let doc = go_to_first_doc(&mut scorers[..]);
    if doc == TERMINATED {
        return Box::new(EmptyScorer);
    }
    // We know that we have at least 2 elements.
    let left = scorers.remove(0);
    let right = scorers.remove(0);
    let all_term_scorers = [&left, &right]
        .iter()
        .all(|&scorer| scorer.is::<TermScorer>());
    if all_term_scorers {
        return Box::new(Intersection {
            left: *(left.downcast::<TermScorer>().map_err(|_| ()).unwrap()),
            right: *(right.downcast::<TermScorer>().map_err(|_| ()).unwrap()),
            others: scorers,
        });
    }
    Box::new(Intersection {
        left,
        right,
        others: scorers,
    })
}

/// Creates a `DocSet` that iterate through the intersection of two or more `DocSet`s.
pub struct Intersection<TDocSet: DocSet, TOtherDocSet: DocSet = Box<dyn Scorer>> {
    left: TDocSet,
    right: TDocSet,
    others: Vec<TOtherDocSet>,
}

fn go_to_first_doc<TDocSet: DocSet>(docsets: &mut [TDocSet]) -> DocId {
    assert!(!docsets.is_empty());
    let mut candidate = docsets.iter().map(TDocSet::doc).max().unwrap();
    'outer: loop {
        for docset in docsets.iter_mut() {
            let seek_doc = docset.seek(candidate);
            if seek_doc > candidate {
                candidate = docset.doc();
                continue 'outer;
            }
        }
        return candidate;
    }
}

impl<TDocSet: DocSet> Intersection<TDocSet, TDocSet> {
    pub(crate) fn new(mut docsets: Vec<TDocSet>) -> Intersection<TDocSet, TDocSet> {
        let num_docsets = docsets.len();
        assert!(num_docsets >= 2);
        docsets.sort_by_key(|docset| docset.size_hint());
        go_to_first_doc(&mut docsets);
        let left = docsets.remove(0);
        let right = docsets.remove(0);
        Intersection {
            left,
            right,
            others: docsets,
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

impl<TDocSet: DocSet, TOtherDocSet: DocSet> DocSet for Intersection<TDocSet, TOtherDocSet> {
    fn advance(&mut self) -> DocId {
        let (left, right) = (&mut self.left, &mut self.right);
        let mut candidate = left.advance();

        'outer: loop {
            // In the first part we look for a document in the intersection
            // of the two rarest `DocSet` in the intersection.

            loop {
                let right_doc = right.seek(candidate);
                candidate = left.seek(right_doc);
                if candidate == right_doc {
                    break;
                }
            }

            debug_assert_eq!(left.doc(), right.doc());
            // test the remaining scorers;
            for docset in self.others.iter_mut() {
                let seek_doc = docset.seek(candidate);
                if seek_doc > candidate {
                    candidate = left.seek(seek_doc);
                    continue 'outer;
                }
            }
            debug_assert_eq!(candidate, self.left.doc());
            debug_assert_eq!(candidate, self.right.doc());
            debug_assert!(self.others.iter().all(|docset| docset.doc() == candidate));
            return candidate;
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.left.seek(target);
        let mut docsets: Vec<&mut dyn DocSet> = vec![&mut self.left, &mut self.right];
        for docset in &mut self.others {
            docsets.push(docset);
        }
        let doc = go_to_first_doc(&mut docsets[..]);
        debug_assert!(docsets.iter().all(|docset| docset.doc() == doc));
        debug_assert!(doc >= target);
        doc
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
    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::VecDocSet;

    #[test]
    fn test_intersection() {
        {
            let left = VecDocSet::from(vec![1, 3, 9]);
            let right = VecDocSet::from(vec![3, 4, 9, 18]);
            let mut intersection = Intersection::new(vec![left, right]);
            assert_eq!(intersection.doc(), 3);
            assert_eq!(intersection.advance(), 9);
            assert_eq!(intersection.doc(), 9);
            assert_eq!(intersection.advance(), TERMINATED);
        }
        {
            let a = VecDocSet::from(vec![1, 3, 9]);
            let b = VecDocSet::from(vec![3, 4, 9, 18]);
            let c = VecDocSet::from(vec![1, 5, 9, 111]);
            let mut intersection = Intersection::new(vec![a, b, c]);
            assert_eq!(intersection.doc(), 9);
            assert_eq!(intersection.advance(), TERMINATED);
        }
    }

    #[test]
    fn test_intersection_zero() {
        let left = VecDocSet::from(vec![0]);
        let right = VecDocSet::from(vec![0]);
        let mut intersection = Intersection::new(vec![left, right]);
        assert_eq!(intersection.doc(), 0);
        assert_eq!(intersection.advance(), TERMINATED);
    }

    #[test]
    fn test_intersection_skip() {
        let left = VecDocSet::from(vec![0, 1, 2, 4]);
        let right = VecDocSet::from(vec![2, 5]);
        let mut intersection = Intersection::new(vec![left, right]);
        assert_eq!(intersection.seek(2), 2);
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
        let intersection = Intersection::new(vec![a, b, c]);
        assert_eq!(intersection.doc(), TERMINATED);
    }
}
