use std::cmp::Ordering;

use crate::docset::{DocSet, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::{Intersection, Scorer};
use crate::{DocId, Score};

struct PostingsWithOffset<TPostings> {
    offset: u32,
    postings: TPostings,
}

impl<TPostings: Postings> PostingsWithOffset<TPostings> {
    pub fn new(segment_postings: TPostings, offset: u32) -> PostingsWithOffset<TPostings> {
        PostingsWithOffset {
            offset,
            postings: segment_postings,
        }
    }

    pub fn positions(&mut self, output: &mut Vec<u32>) {
        self.postings.positions_with_offset(self.offset, output)
    }
}

impl<TPostings: Postings> DocSet for PostingsWithOffset<TPostings> {
    fn advance(&mut self) -> DocId {
        self.postings.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.postings.seek(target)
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

pub struct PhraseScorer<TPostings: Postings> {
    intersection_docset: Intersection<PostingsWithOffset<TPostings>, PostingsWithOffset<TPostings>>,
    num_terms: usize,
    left_positions: Vec<u32>,
    right_positions: Vec<u32>,
    phrase_count: u32,
    fieldnorm_reader: FieldNormReader,
    similarity_weight_opt: Option<Bm25Weight>,
    slop: u32,
}

/// Returns true if and only if the two sorted arrays contain a common element
fn intersection_exists(left: &[u32], right: &[u32]) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        let left_val = left[left_index];
        let right_val = right[right_index];
        match left_val.cmp(&right_val) {
            Ordering::Less => {
                left_index += 1;
            }
            Ordering::Equal => {
                return true;
            }
            Ordering::Greater => {
                right_index += 1;
            }
        }
    }
    false
}

pub(crate) fn intersection_count(left: &[u32], right: &[u32]) -> usize {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    while left_index < left.len() && right_index < right.len() {
        let left_val = left[left_index];
        let right_val = right[right_index];
        match left_val.cmp(&right_val) {
            Ordering::Less => {
                left_index += 1;
            }
            Ordering::Equal => {
                count += 1;
                left_index += 1;
                right_index += 1;
            }
            Ordering::Greater => {
                right_index += 1;
            }
        }
    }
    count
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in left.
///
/// Returns the length of the intersection
#[inline]
fn intersection(left: &mut Vec<u32>, right: &[u32]) -> usize {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left.len();
    let right_len = right.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left[left_index];
        let right_val = right[right_index];
        match left_val.cmp(&right_val) {
            Ordering::Less => {
                left_index += 1;
            }
            Ordering::Equal => {
                left[count] = left_val;
                count += 1;
                left_index += 1;
                right_index += 1;
            }
            Ordering::Greater => {
                right_index += 1;
            }
        }
    }
    left.truncate(count);
    count
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in left_positions if update_left is true.
///
/// Condition for match is that the distance between left and right is less than or equal to `slop`.
///
/// Returns the length of the intersection
#[inline]
fn intersection_count_with_slop(
    left_positions: &mut Vec<u32>,
    right_positions: &[u32],
    slop: u32,
    update_left: bool,
) -> usize {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left_positions.len();
    let right_len = right_positions.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left_positions[left_index];
        let right_val = right_positions[right_index];

        let distance = left_val.abs_diff(right_val);
        if distance <= slop {
            while left_index + 1 < left_len {
                // there could be a better match
                let next_left_val = left_positions[left_index + 1];
                if next_left_val > right_val {
                    // the next value is outside the range, so current one is the best.
                    break;
                }
                // the next value is better.
                left_index += 1;
            }

            // store the match in left.
            if update_left {
                left_positions[count] = right_val;
            }
            count += 1;
            left_index += 1;
            right_index += 1;
        } else if left_val < right_val {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    if update_left {
        left_positions.truncate(count);
    }

    count
}

fn intersection_exists_with_slop(
    left_positions: &[u32],
    right_positions: &[u32],
    slop: u32,
) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    let left_len = left_positions.len();
    let right_len = right_positions.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left_positions[left_index];
        let right_val = right_positions[right_index];
        let distance = left_val.abs_diff(right_val);
        if distance <= slop {
            return true;
        } else if left_val < right_val {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    false
}

impl<TPostings: Postings> PhraseScorer<TPostings> {
    // If similarity_weight is None, then scoring is disabled.
    pub fn new(
        term_postings: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
    ) -> PhraseScorer<TPostings> {
        Self::new_with_offset(
            term_postings,
            similarity_weight_opt,
            fieldnorm_reader,
            slop,
            0,
        )
    }

    pub(crate) fn new_with_offset(
        term_postings_with_offset: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        offset: usize,
    ) -> PhraseScorer<TPostings> {
        let max_offset = term_postings_with_offset
            .iter()
            .map(|&(offset, _)| offset)
            .max()
            .unwrap_or(0)
            + offset;
        let num_docsets = term_postings_with_offset.len();
        let postings_with_offsets = term_postings_with_offset
            .into_iter()
            .map(|(offset, postings)| {
                PostingsWithOffset::new(postings, (max_offset - offset) as u32)
            })
            .collect::<Vec<_>>();
        let mut scorer = PhraseScorer {
            intersection_docset: Intersection::new(postings_with_offsets),
            num_terms: num_docsets,
            left_positions: Vec::with_capacity(100),
            right_positions: Vec::with_capacity(100),
            phrase_count: 0u32,
            similarity_weight_opt,
            fieldnorm_reader,
            slop,
        };
        if scorer.doc() != TERMINATED && !scorer.phrase_match() {
            scorer.advance();
        }
        scorer
    }

    pub fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    pub(crate) fn get_intersection(&mut self) -> &[u32] {
        let len = intersection(&mut self.left_positions, &self.right_positions);
        &self.left_positions[..len]
    }

    fn phrase_match(&mut self) -> bool {
        if self.similarity_weight_opt.is_some() {
            let count = self.compute_phrase_count();
            self.phrase_count = count;
            count > 0u32
        } else {
            self.phrase_exists()
        }
    }

    fn phrase_exists(&mut self) -> bool {
        self.compute_phrase_match();
        if self.has_slop() {
            intersection_exists_with_slop(
                &self.left_positions,
                &self.right_positions[..],
                self.slop,
            )
        } else {
            intersection_exists(&self.left_positions, &self.right_positions[..])
        }
    }

    fn compute_phrase_count(&mut self) -> u32 {
        self.compute_phrase_match();
        if self.has_slop() {
            intersection_count_with_slop(
                &mut self.left_positions,
                &self.right_positions[..],
                self.slop,
                false,
            ) as u32
        } else {
            intersection_count(&self.left_positions, &self.right_positions[..]) as u32
        }
    }

    fn compute_phrase_match(&mut self) {
        {
            self.intersection_docset
                .docset_mut_specialized(0)
                .positions(&mut self.left_positions);
        }
        for i in 1..self.num_terms - 1 {
            {
                self.intersection_docset
                    .docset_mut_specialized(i)
                    .positions(&mut self.right_positions);
            }
            if self.has_slop() {
                intersection_count_with_slop(
                    &mut self.left_positions,
                    &self.right_positions[..],
                    self.slop,
                    true,
                )
            } else {
                intersection(&mut self.left_positions, &self.right_positions[..])
            };
            if self.left_positions.is_empty() {
                return;
            }
        }
        self.intersection_docset
            .docset_mut_specialized(self.num_terms - 1)
            .positions(&mut self.right_positions);
    }

    fn has_slop(&self) -> bool {
        self.slop > 0
    }
}

impl<TPostings: Postings> DocSet for PhraseScorer<TPostings> {
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

impl<TPostings: Postings> Scorer for PhraseScorer<TPostings> {
    fn score(&mut self) -> Score {
        let doc = self.doc();
        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);
        if let Some(similarity_weight) = self.similarity_weight_opt.as_ref() {
            similarity_weight.score(fieldnorm_id, self.phrase_count)
        } else {
            1.0f32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{intersection, intersection_count, intersection_count_with_slop};

    fn test_intersection_sym(left: &[u32], right: &[u32], expected: &[u32]) {
        test_intersection_aux(left, right, expected, 0);
        test_intersection_aux(right, left, expected, 0);
    }

    fn test_intersection_aux(left: &[u32], right: &[u32], expected: &[u32], slop: u32) {
        let mut left_vec = Vec::from(left);
        if slop == 0 {
            assert_eq!(intersection_count(&left_vec, right), expected.len());
            intersection(&mut left_vec, right);
            assert_eq!(&left_vec, expected);
        } else {
            let mut right_vec = Vec::from(right);
            let right_mut = &mut right_vec[..];
            intersection_count_with_slop(&mut left_vec, right_mut, slop, true);
            assert_eq!(&left_vec, expected);
        }
    }

    #[test]
    fn test_intersection() {
        test_intersection_sym(&[1], &[1], &[1]);
        test_intersection_sym(&[1], &[2], &[]);
        test_intersection_sym(&[], &[2], &[]);
        test_intersection_sym(&[5, 7], &[1, 5, 10, 12], &[5]);
        test_intersection_sym(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12]);
    }
    #[test]
    fn test_slop() {
        // The slop is not symmetric. It does not allow for the phrase to be out of order.
        test_intersection_aux(&[1], &[2], &[2], 1);
        test_intersection_aux(&[1], &[3], &[], 1);
        test_intersection_aux(&[1], &[3], &[3], 2);
        test_intersection_aux(&[], &[2], &[], 100000);
        test_intersection_aux(&[5, 7, 11], &[1, 5, 10, 12], &[5, 10], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 8, 9, 12], 1);
        test_intersection_aux(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 8, 9, 12], 10);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[2, 4, 6], 1);
        test_intersection_aux(&[1, 3, 5], &[2, 4, 6], &[], 0);
    }

    fn test_merge(left: &[u32], right: &[u32], expected_left: &[u32], slop: u32) {
        let mut left_vec = Vec::from(left);
        let mut right_vec = Vec::from(right);
        let right_mut = &mut right_vec[..];
        intersection_count_with_slop(&mut left_vec, right_mut, slop, true);
        assert_eq!(&left_vec, expected_left);
    }

    #[test]
    fn test_merge_slop() {
        test_merge(&[1, 2], &[1], &[1], 1);
        test_merge(&[3], &[4], &[4], 2);
        test_merge(&[3], &[4], &[4], 2);
        test_merge(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 8, 9, 12], 10);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use test::Bencher;

    use super::{intersection, intersection_count};

    #[bench]
    fn bench_intersection_short(b: &mut Bencher) {
        let mut left = Vec::new();
        b.iter(|| {
            left.clear();
            left.extend_from_slice(&[1, 5, 10, 12]);
            let right = [5, 7];
            intersection(&mut left, &right);
        });
    }

    #[bench]
    fn bench_intersection_count_short(b: &mut Bencher) {
        b.iter(|| {
            let left = [1, 5, 10, 12];
            let right = [5, 7];
            intersection_count(&left, &right);
        });
    }
}
