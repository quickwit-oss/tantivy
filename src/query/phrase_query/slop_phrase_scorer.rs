use super::phrase_scorer::{PhraseScorer, PostingsWithOffset};
use crate::docset::{DocSet, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::phrase_query::phrase_scorer::get_postings_with_offset;
use crate::query::{Intersection, Scorer};
use crate::{DocId, Score};

pub struct SlopPhraseScorer<TPostings: Postings> {
    intersection_docset: Intersection<PostingsWithOffset<TPostings>, PostingsWithOffset<TPostings>>,
    num_terms: usize,
    left: Vec<u32>,
    right: Vec<u32>,
    phrase_count: usize,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: Bm25Weight,
    scoring_enabled: bool,
    slop: u32,
    used_slop: u32,
}

impl<TPostings: Postings> PhraseScorer for SlopPhraseScorer<TPostings> {
    fn phrase_count(&self) -> u32 {
        self.phrase_count as u32
    }

    fn phrase_match(&mut self) -> bool {
        if self.scoring_enabled {
            let intersection_counters = self.compute_phrase_match();
            (self.phrase_count, self.used_slop) = intersection_count_with_slop(
                &self.left[..intersection_counters.0],
                &self.right[..],
                self.slop,
                intersection_counters.1,
            );
            self.phrase_count > 0
        } else {
            self.phrase_exists()
        }
    }

    fn phrase_exists(&mut self) -> bool {
        let intersection_counters = self.compute_phrase_match();
        intersection_exists_with_slop(
            &self.left[..intersection_counters.0],
            &self.right[..],
            self.slop,
        )
    }
}

impl<TPostings: Postings> SlopPhraseScorer<TPostings> {
    pub fn new(
        term_postings: Vec<(usize, TPostings)>,
        similarity_weight: Bm25Weight,
        fieldnorm_reader: FieldNormReader,
        scoring_enabled: bool,
        slop: u32,
    ) -> SlopPhraseScorer<TPostings> {
        let num_terms = term_postings.len();
        let mut scorer = SlopPhraseScorer {
            intersection_docset: Intersection::new(get_postings_with_offset(term_postings)),
            num_terms,
            left: Vec::with_capacity(100),
            right: Vec::with_capacity(100),
            phrase_count: 0,
            similarity_weight,
            fieldnorm_reader,
            scoring_enabled,
            slop,
            used_slop: 0u32,
        };
        if scorer.doc() != TERMINATED && !scorer.phrase_match() {
            scorer.advance();
        }
        scorer
    }

    fn compute_phrase_match(&mut self) -> (usize, u32) {
        {
            self.intersection_docset
                .docset_mut_specialized(0)
                .positions(&mut self.left);
        }
        let mut intersection_counters = (self.left.len(), 0);
        for i in 1..self.num_terms - 1 {
            {
                self.intersection_docset
                    .docset_mut_specialized(i)
                    .positions(&mut self.right);
            }
            intersection_counters = intersection_with_slop(
                &mut self.left[..intersection_counters.0],
                &self.right[..],
                self.slop,
                intersection_counters.1,
            );
            if intersection_counters.0 == 0 {
                return (0, 0);
            }
        }
        self.intersection_docset
            .docset_mut_specialized(self.num_terms - 1)
            .positions(&mut self.right);
        intersection_counters
    }
}

impl<TPostings: Postings> DocSet for SlopPhraseScorer<TPostings> {
    fn advance(&mut self) -> DocId {
        loop {
            let doc = self.intersection_docset.advance();
            if doc == TERMINATED || self.phrase_match() {
                return doc;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc());
        let doc = self.intersection_docset.seek(target);
        if doc == TERMINATED || self.phrase_match() {
            return doc;
        }
        self.advance()
    }

    fn doc(&self) -> DocId {
        self.intersection_docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.intersection_docset.size_hint()
    }
}

impl<TPostings: Postings> Scorer for SlopPhraseScorer<TPostings> {
    fn score(&mut self) -> Score {
        let doc = self.doc();
        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);
        // we use slop+1 rather than slop to garantee the resulting ratio is > 0
        let allowed_slop = (self.slop + 1) as f32 * self.phrase_count as f32;
        let spare_slop_ratio = 1f32 - self.used_slop as f32 / allowed_slop;
        return self
            .similarity_weight
            .score(fieldnorm_id, self.phrase_count as u32)
            * spare_slop_ratio;
    }
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in left.
///
/// Condition for match is that the value stored in left is less than or equal to
/// the value in right and that the distance to the previous token is lte to the slop.
///
/// Returns the length of the intersection and the accumulated used slop
fn intersection_with_slop(
    left: &mut [u32],
    right: &[u32],
    slop: u32,
    used_slop: u32,
) -> (usize, u32) {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left.len();
    let right_len = right.len();
    let mut used_slop = used_slop;
    while left_index < left_len && right_index < right_len {
        let left_val = left[left_index];
        let right_val = right[right_index];

        // The three conditions are:
        // left_val < right_slop -> left index increment.
        // right_slop <= left_val <= right -> find the best match.
        // left_val > right -> right index increment.
        let right_slop = if right_val >= slop {
            right_val - slop
        } else {
            0
        };

        if left_val < right_slop {
            left_index += 1;
        } else if right_slop <= left_val && left_val <= right_val {
            while left_index + 1 < left_len {
                // there could be a better match
                let next_left_val = left[left_index + 1];
                if next_left_val > right_val {
                    // the next value is outside the range, so current one is the best.
                    break;
                }
                // the next value is better.
                left_index += 1;
            }
            used_slop += right_val - left[left_index];
            // store the match in left.
            left[count] = right_val;
            count += 1;
            left_index += 1;
            right_index += 1;
        } else if left_val > right_val {
            right_index += 1;
        }
    }
    (count, used_slop)
}

fn intersection_count_with_slop(
    left: &[u32],
    right: &[u32],
    slop: u32,
    used_slop: u32,
) -> (usize, u32) {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left.len();
    let right_len = right.len();
    let mut used_slop = used_slop;
    while left_index < left_len && right_index < right_len {
        let left_val = left[left_index];
        let right_val = right[right_index];
        let right_slop = if right_val >= slop {
            right_val - slop
        } else {
            0
        };

        if left_val < right_slop {
            left_index += 1;
        } else if right_slop <= left_val && left_val <= right_val {
            while left_index + 1 < left_len {
                let next_left_val = left[left_index + 1];
                if next_left_val > right_val {
                    break;
                }
                left_index += 1;
            }
            used_slop += right_val - left[left_index];
            count += 1;
            left_index += 1;
            right_index += 1;
        } else if left_val > right_val {
            right_index += 1;
        }
    }
    (count, used_slop)
}

fn intersection_exists_with_slop(left: &[u32], right: &[u32], slop: u32) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    let left_len = left.len();
    let right_len = right.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left[left_index];
        let right_val = right[right_index];
        let right_slop = if right_val >= slop {
            right_val - slop
        } else {
            0
        };

        if left_val < right_slop {
            left_index += 1;
        } else if right_slop <= left_val && left_val <= right_val {
            return true;
        } else if left_val > right_val {
            right_index += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        intersection_count_with_slop, intersection_exists_with_slop, intersection_with_slop,
    };

    fn test_intersection_aux(left: &[u32], right: &[u32], expected: &[u32], slop: u32) {
        let mut left_vec = Vec::from(left);
        let left_mut = &mut left_vec[..];
        assert_eq!(
            intersection_count_with_slop(left_mut, right, slop, 0).0,
            expected.len()
        );
        let count = intersection_with_slop(left_mut, right, slop, 0);
        assert_eq!(&left_mut[..count.0], expected);
        assert_eq!(
            intersection_exists_with_slop(left, right, slop),
            expected.len() != 0
        )
    }

    #[test]
    fn test_slop() {
        // The slop is not symetric. It does not allow for the phrase to be out of order.
        // The slop is not symetric. It does not allow for the phrase to be out of order.
        test_intersection_aux(&[1], &[2], &[2], 1);
        test_intersection_aux(&[1], &[2], &[2], 1);
        test_intersection_aux(&[1], &[3], &[], 1);
        test_intersection_aux(&[1], &[3], &[], 1);
        test_intersection_aux(&[1], &[3], &[3], 2);
        test_intersection_aux(&[1], &[3], &[3], 2);
        test_intersection_aux(&[], &[2], &[], 100000);
        test_intersection_aux(&[], &[2], &[], 100000);
        test_intersection_aux(&[5, 7, 11], &[1, 5, 10, 12], &[5, 12], 1);
        test_intersection_aux(&[5, 7, 11], &[1, 5, 10, 12], &[5, 12], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12], 10);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12], 10);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[2, 4, 6], 1);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[2, 4, 6], 1);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[], 0);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[], 0);
    }

    fn test_merge(
        left: &[u32],
        right: &[u32],
        expected_left: &[u32],
        expected_used: u32,
        slop: u32,
    ) {
        let mut left_vec = Vec::from(left);
        let left_mut = &mut left_vec[..];
        let mut right_vec = Vec::from(right);
        let right_mut = &mut right_vec[..];
        let count = intersection_with_slop(left_mut, right_mut, slop, 0);
        assert_eq!(&left_mut[..count.0], expected_left);
        assert_eq!(count.1, expected_used);
    }

    #[test]
    fn test_merge_slop() {
        test_merge(&[1, 2], &[1], &[1], 0, 1);
        test_merge(&[3], &[4], &[4], 1, 2);
        test_merge(&[3], &[4], &[4], 1, 2);
        test_merge(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12], 0, 0);
        test_merge(&[1, 5, 6], &[2, 6, 7], &[2, 6], 1, 10); // we don't answer 2,6,7
    }
}
