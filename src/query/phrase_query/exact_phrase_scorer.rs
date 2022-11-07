use std::cmp::Ordering;

use super::phrase_scorer::{PhraseScorer, PostingsWithOffset};
use crate::docset::{DocSet, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::phrase_query::phrase_scorer::get_postings_with_offset;
use crate::query::{Intersection, Scorer};
use crate::{DocId, Score};

pub struct ExactPhraseScorer<TPostings: Postings> {
    intersection_docset: Intersection<PostingsWithOffset<TPostings>, PostingsWithOffset<TPostings>>,
    num_terms: usize,
    left: Vec<u32>,
    right: Vec<u32>,
    phrase_count: u32,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: Bm25Weight,
    scoring_enabled: bool,
}

impl<TPostings: Postings> PhraseScorer for ExactPhraseScorer<TPostings> {
    fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    fn phrase_match(&mut self) -> bool {
        if self.scoring_enabled {
            let intersection_len = self.compute_phrase_match();
            let count = intersection_count(&self.left[..intersection_len], &self.right[..]) as u32;
            self.phrase_count = count;
            count > 0u32
        } else {
            self.phrase_exists()
        }
    }

    fn phrase_exists(&mut self) -> bool {
        let intersection_len = self.compute_phrase_match();
        intersection_exists(&self.left[..intersection_len], &self.right[..])
    }
}

impl<TPostings: Postings> ExactPhraseScorer<TPostings> {
    pub fn new(
        term_postings: Vec<(usize, TPostings)>,
        similarity_weight: Bm25Weight,
        fieldnorm_reader: FieldNormReader,
        scoring_enabled: bool,
    ) -> ExactPhraseScorer<TPostings> {
        let num_terms = term_postings.len();
        let mut scorer = ExactPhraseScorer {
            intersection_docset: Intersection::new(get_postings_with_offset(term_postings)),
            num_terms,
            left: Vec::with_capacity(100),
            right: Vec::with_capacity(100),
            phrase_count: 0u32,
            similarity_weight,
            fieldnorm_reader,
            scoring_enabled,
        };
        if scorer.doc() != TERMINATED && !scorer.phrase_match() {
            scorer.advance();
        }
        scorer
    }

    fn compute_phrase_match(&mut self) -> usize {
        {
            self.intersection_docset
                .docset_mut_specialized(0)
                .positions(&mut self.left);
        }
        let mut intersection_len = self.left.len();
        for i in 1..self.num_terms - 1 {
            {
                self.intersection_docset
                    .docset_mut_specialized(i)
                    .positions(&mut self.right);
            }
            intersection_len = intersection(&mut self.left[..intersection_len], &self.right[..]);
            if intersection_len == 0 {
                return 0;
            }
        }
        self.intersection_docset
            .docset_mut_specialized(self.num_terms - 1)
            .positions(&mut self.right);
        intersection_len
    }
}

impl<TPostings: Postings> DocSet for ExactPhraseScorer<TPostings> {
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

impl<TPostings: Postings> Scorer for ExactPhraseScorer<TPostings> {
    fn score(&mut self) -> Score {
        let doc = self.doc();
        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);
        self.similarity_weight
            .score(fieldnorm_id, self.phrase_count)
    }
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

fn intersection_count(left: &[u32], right: &[u32]) -> usize {
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
fn intersection(left: &mut [u32], right: &[u32]) -> usize {
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
    count
}

#[cfg(test)]
mod tests {
    use super::{intersection, intersection_count, intersection_exists};

    fn test_intersection_sym(left: &[u32], right: &[u32], expected: &[u32]) {
        test_intersection_aux(left, right, expected);
        test_intersection_aux(right, left, expected);
    }

    fn test_intersection_aux(left: &[u32], right: &[u32], expected: &[u32]) {
        let mut left_vec = Vec::from(left);
        let left_mut = &mut left_vec[..];
        assert_eq!(intersection_count(left_mut, right), expected.len());
        let count = intersection(left_mut, right);
        assert_eq!(&left_mut[..count], expected);
        assert_eq!(intersection_exists(left, right), expected.len() != 0);
    }

    #[test]
    fn test_intersection() {
        test_intersection_sym(&[1], &[1], &[1]);
        test_intersection_sym(&[1], &[2], &[]);
        test_intersection_sym(&[], &[2], &[]);
        test_intersection_sym(&[5, 7], &[1, 5, 10, 12], &[5]);
        test_intersection_sym(&[1, 5, 6, 9, 10, 12], &[6, 8, 9, 12], &[6, 9, 12]);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use test::Bencher;

    use super::{intersection, intersection_count};

    #[bench]
    fn bench_intersection_short(b: &mut Bencher) {
        b.iter(|| {
            let mut left = [1, 5, 10, 12];
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
