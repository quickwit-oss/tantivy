use super::size_hint::estimate_intersection;
use crate::docset::{DocSet, TERMINATED};
use crate::query::term_query::TermScorer;
use crate::query::{EmptyScorer, Scorer};
use crate::{DocId, Score};

/// Returns the intersection scorer.
///
/// The score associated with the documents is the sum of the
/// score of the `Scorer`s given in argument.
///
/// For better performance, the function uses a
/// specialized implementation if the two
/// shortest scorers are `TermScorer`s.
///
/// num_docs_segment is the number of documents in the segment. It is used for estimating the
/// `size_hint` of the intersection.
pub fn intersect_scorers(
    mut scorers: Vec<Box<dyn Scorer>>,
    num_docs_segment: u32,
) -> Box<dyn Scorer> {
    if scorers.is_empty() {
        return Box::new(EmptyScorer);
    }
    if scorers.len() == 1 {
        return scorers.pop().unwrap();
    }
    // Order by estimated cost to drive each scorer.
    scorers.sort_by_key(|scorer| scorer.cost());
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
            num_docs: num_docs_segment,
        });
    }
    Box::new(Intersection {
        left,
        right,
        others: scorers,
        num_docs: num_docs_segment,
    })
}

/// Creates a `DocSet` that iterate through the intersection of two or more `DocSet`s.
pub struct Intersection<TDocSet: DocSet, TOtherDocSet: DocSet = Box<dyn Scorer>> {
    left: TDocSet,
    right: TDocSet,
    others: Vec<TOtherDocSet>,
    num_docs: u32,
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
    /// num_docs is the number of documents in the segment.
    pub(crate) fn new(mut docsets: Vec<TDocSet>, num_docs: u32) -> Intersection<TDocSet, TDocSet> {
        let num_docsets = docsets.len();
        assert!(num_docsets >= 2);
        docsets.sort_by_key(|docset| docset.cost());
        go_to_first_doc(&mut docsets);
        let left = docsets.remove(0);
        let right = docsets.remove(0);
        Intersection {
            left,
            right,
            others: docsets,
            num_docs,
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
    #[inline]
    fn advance(&mut self) -> DocId {
        let (left, right) = (&mut self.left, &mut self.right);
        let mut candidate = left.advance();
        if candidate == TERMINATED {
            return TERMINATED;
        }

        loop {
            // In the first part we look for a document in the intersection
            // of the two rarest `DocSet` in the intersection.

            loop {
                if right.seek_into_the_danger_zone(candidate) {
                    break;
                }
                let right_doc = right.doc();
                // TODO: Think about which value would make sense here
                // It depends on the DocSet implementation, when a seek would outweigh an advance.
                if right_doc > candidate.wrapping_add(100) {
                    candidate = left.seek(right_doc);
                } else {
                    candidate = left.advance();
                }
                if candidate == TERMINATED {
                    return TERMINATED;
                }
            }

            debug_assert_eq!(left.doc(), right.doc());
            // test the remaining scorers
            if self
                .others
                .iter_mut()
                .all(|docset| docset.seek_into_the_danger_zone(candidate))
            {
                debug_assert_eq!(candidate, self.left.doc());
                debug_assert_eq!(candidate, self.right.doc());
                debug_assert!(self.others.iter().all(|docset| docset.doc() == candidate));
                return candidate;
            }
            candidate = left.advance();
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

    /// Seeks to the target if necessary and checks if the target is an exact match.
    ///
    /// Some implementations may choose to advance past the target if beneficial for performance.
    /// The return value is `true` if the target is in the docset, and `false` otherwise.
    fn seek_into_the_danger_zone(&mut self, target: DocId) -> bool {
        self.left.seek_into_the_danger_zone(target)
            && self.right.seek_into_the_danger_zone(target)
            && self
                .others
                .iter_mut()
                .all(|docset| docset.seek_into_the_danger_zone(target))
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.left.doc()
    }

    fn size_hint(&self) -> u32 {
        estimate_intersection(
            [self.left.size_hint(), self.right.size_hint()]
                .into_iter()
                .chain(self.others.iter().map(DocSet::size_hint)),
            self.num_docs,
        )
    }

    fn cost(&self) -> u64 {
        // What's the best way to compute the cost of an intersection?
        // For now we take the cost of the docset driver, which is the first docset.
        // If there are docsets that are bad at skipping, they should also influence the cost.
        self.left.cost()
    }
}

impl<TScorer, TOtherScorer> Scorer for Intersection<TScorer, TOtherScorer>
where
    TScorer: Scorer,
    TOtherScorer: Scorer,
{
    #[inline]
    fn score(&mut self) -> Score {
        self.left.score()
            + self.right.score()
            + self.others.iter_mut().map(Scorer::score).sum::<Score>()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::Intersection;
    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::VecDocSet;

    #[test]
    fn test_intersection() {
        {
            let left = VecDocSet::from(vec![1, 3, 9]);
            let right = VecDocSet::from(vec![3, 4, 9, 18]);
            let mut intersection = Intersection::new(vec![left, right], 10);
            assert_eq!(intersection.doc(), 3);
            assert_eq!(intersection.advance(), 9);
            assert_eq!(intersection.doc(), 9);
            assert_eq!(intersection.advance(), TERMINATED);
        }
        {
            let a = VecDocSet::from(vec![1, 3, 9]);
            let b = VecDocSet::from(vec![3, 4, 9, 18]);
            let c = VecDocSet::from(vec![1, 5, 9, 111]);
            let mut intersection = Intersection::new(vec![a, b, c], 10);
            assert_eq!(intersection.doc(), 9);
            assert_eq!(intersection.advance(), TERMINATED);
        }
    }

    #[test]
    fn test_intersection_zero() {
        let left = VecDocSet::from(vec![0]);
        let right = VecDocSet::from(vec![0]);
        let mut intersection = Intersection::new(vec![left, right], 10);
        assert_eq!(intersection.doc(), 0);
        assert_eq!(intersection.advance(), TERMINATED);
    }

    #[test]
    fn test_intersection_skip() {
        let left = VecDocSet::from(vec![0, 1, 2, 4]);
        let right = VecDocSet::from(vec![2, 5]);
        let mut intersection = Intersection::new(vec![left, right], 10);
        assert_eq!(intersection.seek(2), 2);
        assert_eq!(intersection.doc(), 2);
    }

    #[test]
    fn test_intersection_skip_against_unoptimized() {
        test_skip_against_unoptimized(
            || {
                let left = VecDocSet::from(vec![4]);
                let right = VecDocSet::from(vec![2, 5]);
                Box::new(Intersection::new(vec![left, right], 10))
            },
            vec![0, 2, 4, 5, 6],
        );
        test_skip_against_unoptimized(
            || {
                let mut left = VecDocSet::from(vec![1, 4, 5, 6]);
                let mut right = VecDocSet::from(vec![2, 5, 10]);
                left.advance();
                right.advance();
                Box::new(Intersection::new(vec![left, right], 10))
            },
            vec![0, 1, 2, 3, 4, 5, 6, 7, 10, 11],
        );
        test_skip_against_unoptimized(
            || {
                Box::new(Intersection::new(
                    vec![
                        VecDocSet::from(vec![1, 4, 5, 6]),
                        VecDocSet::from(vec![1, 2, 5, 6]),
                        VecDocSet::from(vec![1, 4, 5, 6]),
                        VecDocSet::from(vec![1, 5, 6]),
                        VecDocSet::from(vec![2, 4, 5, 7, 8]),
                    ],
                    10,
                ))
            },
            vec![0, 1, 2, 3, 4, 5, 6, 7, 10, 11],
        );
    }

    #[test]
    fn test_intersection_empty() {
        let a = VecDocSet::from(vec![1, 3]);
        let b = VecDocSet::from(vec![1, 4]);
        let c = VecDocSet::from(vec![3, 9]);
        let intersection = Intersection::new(vec![a, b, c], 10);
        assert_eq!(intersection.doc(), TERMINATED);
    }

    // Strategy to generate sorted and deduplicated vectors of u32 document IDs
    fn sorted_deduped_vec(max_val: u32, max_size: usize) -> impl Strategy<Value = Vec<u32>> {
        prop::collection::vec(0..max_val, 0..max_size).prop_map(|mut vec| {
            vec.sort();
            vec.dedup();
            vec
        })
    }

    proptest! {
        #[test]
        fn prop_test_intersection_consistency(
            a in sorted_deduped_vec(100, 10),
            b in sorted_deduped_vec(100, 10),
            num_docs in 100u32..500u32
        ) {
            let left = VecDocSet::from(a.clone());
            let right = VecDocSet::from(b.clone());
            let mut intersection = Intersection::new(vec![left, right], num_docs);

            let expected: Vec<u32> = a.iter()
                .cloned()
                .filter(|doc| b.contains(doc))
                .collect();

            for expected_doc in expected {
                assert_eq!(intersection.doc(), expected_doc);
                intersection.advance();
            }
            assert_eq!(intersection.doc(), TERMINATED);
        }

    }
}
