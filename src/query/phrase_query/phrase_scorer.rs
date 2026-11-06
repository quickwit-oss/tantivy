use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::{Intersection, Scorer};
use crate::{DocId, Score};

struct PostingsWithOffset<TPostings> {
    offset: u32,
    postings: TPostings,
    phrase_index: usize,
}

impl<TPostings: Postings> PostingsWithOffset<TPostings> {
    pub fn new(
        segment_postings: TPostings,
        offset: u32,
        phrase_index: usize,
    ) -> PostingsWithOffset<TPostings> {
        PostingsWithOffset {
            offset,
            postings: segment_postings,
            phrase_index,
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

/// Scores the documents where the terms of a phrase occur at their offsets
/// (within `slop`), with BM25 over the phrase count when a similarity weight
/// is given. Each position is any [`Postings`], e.g. a union of several terms.
pub struct PhraseScorer<TPostings: Postings> {
    intersection_docset: Intersection<PostingsWithOffset<TPostings>, PostingsWithOffset<TPostings>>,
    num_terms: usize,
    left_positions: Vec<u32>,
    right_positions: Vec<u32>,
    phrase_count: u32,
    fieldnorm_reader: FieldNormReader,
    similarity_weight_opt: Option<Bm25Weight>,
    slop: u32,
    left_slops: Vec<u32>,
    positions_buffer: Vec<u32>,
    slops_buffer: Vec<u32>,
    slop_positions: Vec<Vec<u32>>,
    position_order: Vec<usize>,
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
fn intersection(left: &mut Vec<u32>, right: &[u32]) {
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

fn positions_match_with_slop(position_lists: &[Vec<u32>], slop: u32) -> bool {
    if position_lists.iter().any(Vec::is_empty) {
        return false;
    }
    let mut indices = vec![0; position_lists.len()];
    let mut current_positions = BinaryHeap::new();
    let mut current_max = 0;
    for (list_index, positions) in position_lists.iter().enumerate() {
        let position = positions[0];
        current_max = current_max.max(position);
        current_positions.push(Reverse((position, list_index)));
    }

    loop {
        let Reverse((current_min, list_index)) = current_positions.pop().unwrap();
        if current_max - current_min <= slop {
            return true;
        }
        let next_index = &mut indices[list_index];
        *next_index += 1;
        let Some(&next_position) = position_lists[list_index].get(*next_index) else {
            return false;
        };
        current_max = current_max.max(next_position);
        current_positions.push(Reverse((next_position, list_index)));
    }
}

/// Approximate phrase count used for scoring. Exact match existence is checked separately with
/// `positions_match_with_slop`. Distances are widened before summing so large slops cannot wrap.
///
/// left_slops is allowed to be empty, which equals to a slop of 0 so far.
#[inline]
fn intersection_count_with_carrying_slop(
    left_positions: &mut Vec<u32>,
    left_slops: &mut Vec<u32>,
    right_positions: &[u32],
    max_slop: u32,
    update_left: bool,
    positions_buffer: &mut Vec<u32>,
    slops_buffer: &mut Vec<u32>,
) -> u32 {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;

    if left_positions.is_empty() || right_positions.is_empty() {
        if update_left {
            left_positions.clear();
            left_slops.clear();
        }
        return 0;
    }

    let add_val = |val: (u32, u32), new_left: &mut Vec<u32>, new_slops: &mut Vec<u32>| {
        if update_left {
            let pos_exists = new_left.last().map(|v| *v == val.1).unwrap_or(false);
            if pos_exists {
                let last_slop = new_slops.last_mut().unwrap();
                *last_slop = (*last_slop).min(val.0);
            } else {
                new_left.push(val.1);
                new_slops.push(val.0);
            }
        }
    };
    loop {
        let left_val = left_positions[left_index];
        let slop_so_far = left_slops.get(left_index).cloned().unwrap_or(0);
        let right_val = right_positions[right_index];

        let distance = slop_so_far as u64 + left_val.abs_diff(right_val) as u64;
        if distance <= max_slop as u64 {
            let (smaller_val, larger_val, mut smaller_val_idx, smaller_val_positions) =
                if left_val < right_val {
                    (left_val, right_val, left_index, left_positions.as_slice())
                } else {
                    (right_val, left_val, right_index, right_positions)
                };

            let mut new_slop = distance as u32;
            add_val((new_slop, smaller_val), positions_buffer, slops_buffer);
            while smaller_val_idx + 1 < smaller_val_positions.len() {
                // there could be a better match
                let next_val = smaller_val_positions[smaller_val_idx + 1];
                if next_val > larger_val {
                    // the next value is outside the range, so current one is the best.
                    break;
                }
                let distance = slop_so_far as u64 + next_val.abs_diff(larger_val) as u64;

                // the next value is better.
                smaller_val_idx += 1;
                if distance > max_slop as u64 {
                    continue;
                }
                // better slop
                new_slop = distance as u32;
                add_val((new_slop, next_val), positions_buffer, slops_buffer);
            }

            add_val((new_slop, larger_val), positions_buffer, slops_buffer);
            count += 1;
            left_index += 1;
            right_index += 1;
        } else if left_val < right_val {
            left_index += 1;
        } else {
            right_index += 1;
        }

        if left_index >= left_positions.len() || right_index >= right_positions.len() {
            // finish rest
            if left_index >= left_positions.len() {
                let left_val = *left_positions.last().unwrap();
                let slop_so_far: u32 = *left_slops.last().unwrap_or(&0);
                for right_val in &right_positions[right_index..] {
                    let new_slop = left_val.abs_diff(*right_val) as u64 + slop_so_far as u64;
                    if new_slop <= max_slop as u64 {
                        add_val(
                            (new_slop as u32, *right_val),
                            positions_buffer,
                            slops_buffer,
                        );
                    }
                }
            } else {
                let right_val = *right_positions.last().unwrap();
                for left_idx in left_index..left_positions.len() {
                    let left_val = left_positions[left_idx];
                    let slop_so_far = *left_slops.get(left_idx).unwrap_or(&0);
                    let new_slop = left_val.abs_diff(right_val) as u64 + slop_so_far as u64;
                    if new_slop <= max_slop as u64 {
                        add_val((new_slop as u32, left_val), positions_buffer, slops_buffer);
                    }
                }
            };

            break;
        }
    }
    if update_left {
        std::mem::swap(left_positions, positions_buffer);
        std::mem::swap(left_slops, slops_buffer);
        positions_buffer.clear();
        slops_buffer.clear();
    }

    count
}

impl<TPostings: Postings> PhraseScorer<TPostings> {
    /// A scorer over `term_postings` (each with its offset in the phrase),
    /// positioned on the first match. If `similarity_weight_opt` is None,
    /// then scoring is disabled (every match scores 1).
    ///
    /// # Panics
    ///
    /// If `term_postings` has fewer than two postings. A phrase of one term is a term query.
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
        let (seek_result, mut scorer) = Self::new_danger(
            term_postings_with_offset,
            similarity_weight_opt,
            fieldnorm_reader,
            slop,
            offset,
            0,
        );
        if let SeekDangerResult::SeekLowerBound(target) = seek_result {
            if target < TERMINATED {
                scorer.seek(target);
            }
        }
        scorer
    }

    /// Creates a phrase scorer near `target` without scanning forward to the next phrase match.
    ///
    /// On a miss, the scorer follows the danger-state contract: it must only receive further
    /// `seek_danger` calls until one returns [`SeekDangerResult::Found`].
    pub(crate) fn new_danger(
        mut term_postings_with_offset: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        offset: usize,
        target: DocId,
    ) -> (SeekDangerResult, PhraseScorer<TPostings>) {
        for (_, postings) in &mut term_postings_with_offset {
            if postings.doc() < target {
                // We do not optimize for that seek.
                // This would require a constructor Intersection::new that
                // accepts postings in the danger zone and we prefer to avoid that.
                postings.seek(target);
            }
        }
        let num_docs = fieldnorm_reader.num_docs();
        let max_offset = term_postings_with_offset
            .iter()
            .map(|&(offset, _)| offset)
            .max()
            .unwrap_or(0)
            + offset;
        let num_docsets = term_postings_with_offset.len();
        let postings_with_offsets = term_postings_with_offset
            .into_iter()
            .enumerate()
            .map(|(phrase_index, (offset, postings))| {
                PostingsWithOffset::new(postings, (max_offset - offset) as u32, phrase_index)
            })
            .collect::<Vec<_>>();
        let mut intersection_docset = Intersection::new(postings_with_offsets, num_docs);
        let mut position_order = vec![0; num_docsets];
        for docset_index in 0..num_docsets {
            let phrase_index = intersection_docset
                .docset_mut_specialized(docset_index)
                .phrase_index;
            position_order[phrase_index] = docset_index;
        }
        let slop_positions = (0..num_docsets).map(|_| Vec::new()).collect();
        let mut scorer = PhraseScorer {
            intersection_docset,
            num_terms: num_docsets,
            left_positions: Vec::with_capacity(100),
            right_positions: Vec::with_capacity(100),
            phrase_count: 0u32,
            similarity_weight_opt,
            fieldnorm_reader,
            slop,
            left_slops: Vec::with_capacity(100),
            slops_buffer: Vec::with_capacity(100),
            positions_buffer: Vec::with_capacity(100),
            slop_positions,
            position_order,
        };
        let doc = scorer.doc();
        debug_assert!(doc >= target);
        let seek_result = if doc >= TERMINATED {
            SeekDangerResult::SeekLowerBound(TERMINATED)
        } else if doc > target {
            SeekDangerResult::SeekLowerBound(doc)
        } else if scorer.phrase_match() {
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        };
        (seek_result, scorer)
    }

    /// The number of phrase matches in the current document (computed when
    /// scoring is enabled).
    pub fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    pub(crate) fn get_intersection(&mut self) -> &[u32] {
        intersection(&mut self.left_positions, &self.right_positions);
        &self.left_positions
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
        if self.has_slop() {
            self.compute_slop_positions();
            positions_match_with_slop(&self.slop_positions, self.slop)
        } else {
            self.compute_phrase_match();
            intersection_exists(&self.left_positions, &self.right_positions[..])
        }
    }

    fn compute_phrase_count(&mut self) -> u32 {
        if self.has_slop() {
            self.compute_slop_positions();
            if !positions_match_with_slop(&self.slop_positions, self.slop) {
                return 0;
            }
            self.compute_slop_phrase_count().max(1)
        } else {
            self.compute_phrase_match();
            intersection_count(&self.left_positions, &self.right_positions[..]) as u32
        }
    }

    fn compute_slop_positions(&mut self) {
        for phrase_index in 0..self.num_terms {
            let docset_index = self.position_order[phrase_index];
            let positions = &mut self.slop_positions[phrase_index];
            positions.clear();
            self.intersection_docset
                .docset_mut_specialized(docset_index)
                .positions(positions);
        }
    }

    fn compute_slop_phrase_count(&mut self) -> u32 {
        let mut order = (0..self.num_terms).collect::<Vec<_>>();
        order.sort_unstable_by_key(|&phrase_index| {
            (self.slop_positions[phrase_index].len(), phrase_index)
        });
        self.left_positions
            .clone_from(&self.slop_positions[order[0]]);
        self.left_slops.clear();
        for &phrase_index in &order[1..self.num_terms - 1] {
            intersection_count_with_carrying_slop(
                &mut self.left_positions,
                &mut self.left_slops,
                &self.slop_positions[phrase_index],
                self.slop,
                true,
                &mut self.positions_buffer,
                &mut self.slops_buffer,
            );
            if self.left_positions.is_empty() {
                return 0;
            }
        }
        if self.num_terms > 2 {
            intersection_count_with_carrying_slop(
                &mut self.left_positions,
                &mut self.left_slops,
                &self.slop_positions[order[self.num_terms - 1]],
                self.slop,
                false,
                &mut self.positions_buffer,
                &mut self.slops_buffer,
            )
        } else {
            intersection_count_with_slop(
                &mut self.left_positions,
                &self.slop_positions[order[1]],
                self.slop,
                false,
            ) as u32
        }
    }

    fn compute_phrase_match(&mut self) {
        self.intersection_docset
            .docset_mut_specialized(0)
            .positions(&mut self.left_positions);
        for i in 1..self.num_terms - 1 {
            self.intersection_docset
                .docset_mut_specialized(i)
                .positions(&mut self.right_positions);
            intersection(&mut self.left_positions, &self.right_positions);
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

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        debug_assert!(
            target >= self.doc(),
            "target ({}) should be greater than or equal to doc ({})",
            target,
            self.doc()
        );
        let seek_res = self.intersection_docset.seek_danger(target);
        if seek_res != SeekDangerResult::Found {
            return seek_res;
        }
        // The intersection matched. Now let's see if we match the phrase.
        if self.phrase_match() {
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn doc(&self) -> DocId {
        self.intersection_docset.doc()
    }

    fn size_hint(&self) -> u32 {
        // We adjust the intersection estimate, since actual phrase hits are much lower than where
        // the all appear.
        // The estimate should depend on average field length, e.g. if the field is really short
        // a phrase hit is more likely
        self.intersection_docset.size_hint() / (10 * self.num_terms as u32)
    }

    /// Returns a best-effort hint of the
    /// cost to drive the docset.
    fn cost(&self) -> u64 {
        // While determing a potential hit is cheap for phrases, evaluating an actual hit is
        // expensive since it requires to load positions for a doc and check if they are next to
        // each other.
        // So the cost estimation would be the number of times we need to check if a doc is a hit *
        // 10 * self.num_terms.
        self.intersection_docset.size_hint() as u64 * 10 * self.num_terms as u64
    }
}

impl<TPostings: Postings> Scorer for PhraseScorer<TPostings> {
    #[inline]
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
    use super::*;

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

    fn test_carry_slop_intersection_aux(
        right: &[&[u32]],
        expected: &[(u32, u32)],
        slop: u32,
        expected_count: u32,
    ) {
        let mut left_vec = right[0].to_vec();
        let mut slops = vec![0; left_vec.len()];
        let mut count = 0;
        for right in &right[1..] {
            count = intersection_count_with_carrying_slop(
                &mut left_vec,
                &mut slops,
                right,
                slop,
                true,
                &mut Vec::new(),
                &mut Vec::new(),
            );
        }
        let out: Vec<(u32, u32)> = slops
            .iter()
            .cloned()
            .zip(left_vec.iter().cloned())
            .collect();
        assert_eq!(&out, expected);
        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_carry_slop_intersection() {
        test_carry_slop_intersection_aux(&[&[1], &[]], &[], 1, 0);
        test_carry_slop_intersection_aux(&[&[1], &[2]], &[(1, 1), (1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[1], &[3]], &[], 1, 0);
        test_carry_slop_intersection_aux(&[&[1], &[2]], &[(1, 1), (1, 2)], 1, 1);

        // The order may still matter
        test_carry_slop_intersection_aux(&[&[1], &[2], &[2]], &[(1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[2], &[1], &[2]], &[(1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[2], &[2], &[1]], &[(1, 1), (1, 2)], 1, 1);

        test_carry_slop_intersection_aux(&[&[2], &[2], &[1], &[2]], &[(1, 2)], 1, 1);
        test_carry_slop_intersection_aux(&[&[1], &[2], &[2], &[2]], &[(1, 2)], 1, 1);

        test_carry_slop_intersection_aux(&[&[1], &[2], &[1]], &[(1, 1)], 1, 1);

        test_carry_slop_intersection_aux(&[&[11], &[10, 12]], &[(1, 10), (1, 11), (1, 12)], 1, 1);
        test_carry_slop_intersection_aux(&[&[10, 12], &[11]], &[(1, 10), (1, 11), (1, 12)], 1, 1);

        test_carry_slop_intersection_aux(
            &[&[5, 7, 11], &[1, 5, 10, 12]],
            &[(0, 5), (1, 10), (1, 11), (1, 12)],
            1,
            2,
        );
        test_carry_slop_intersection_aux(&[&[0], &[256]], &[(256, 0), (256, 256)], 256, 1);
        test_carry_slop_intersection_aux(&[&[0], &[256], &[512]], &[], 256, 0);
        test_carry_slop_intersection_aux(&[&[0], &[u32::MAX], &[u32::MAX - 1]], &[], u32::MAX, 0);
    }

    #[test]
    fn test_carry_slop_bounds_candidates() {
        let mut left_positions = vec![0];
        let mut left_slops = Vec::new();
        let mut positions_buffer = Vec::new();
        let mut slops_buffer = Vec::new();
        let mut max_candidates = 0;

        for right_val in 1..30u32 {
            intersection_count_with_carrying_slop(
                &mut left_positions,
                &mut left_slops,
                &[right_val],
                29,
                true,
                &mut positions_buffer,
                &mut slops_buffer,
            );
            max_candidates = max_candidates.max(left_positions.len());
            assert_eq!(left_positions.len(), left_slops.len());
        }
        assert!(max_candidates <= 153, "{max_candidates} candidates");
    }

    #[test]
    fn test_positions_match_with_slop_brute_force() {
        fn positions(mask: u32) -> Vec<u32> {
            (0..5)
                .filter(|position| mask & (1 << position) != 0)
                .collect()
        }

        for first_mask in 0..32 {
            let first = positions(first_mask);
            for second_mask in 0..32 {
                let second = positions(second_mask);
                for third_mask in 0..32 {
                    let third = positions(third_mask);
                    for slop in 0..=4 {
                        let expected = first.iter().any(|&a| {
                            second.iter().any(|&b| {
                                third.iter().any(|&c| {
                                    let start = a.min(b).min(c);
                                    let end = a.max(b).max(c);
                                    end - start <= slop
                                })
                            })
                        });
                        let matches = positions_match_with_slop(
                            &[first.clone(), second.clone(), third.clone()],
                            slop,
                        );
                        assert_eq!(
                            matches, expected,
                            "{first:?} {second:?} {third:?} at slop {slop}"
                        );
                    }
                }
            }
        }
        assert!(positions_match_with_slop(
            &[vec![0], vec![u32::MAX], vec![u32::MAX - 1]],
            u32::MAX
        ));
        assert!(!positions_match_with_slop(
            &[vec![0], vec![256], vec![512]],
            256
        ));
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use test::Bencher;

    use super::*;

    #[bench]
    fn bench_intersection_short_slop_carrying(b: &mut Bencher) {
        let mut left = Vec::new();
        let mut left_slops = Vec::new();
        let mut buffer = Vec::new();
        let mut slop_buffer = Vec::new();
        b.iter(|| {
            left.clear();
            left.extend_from_slice(&[1, 5, 10, 12]);
            left_slops.clear();
            left_slops.extend_from_slice(&[0, 0, 0, 0]);
            let right = [5, 7];
            intersection(&mut left, &right);

            intersection_count_with_carrying_slop(
                &mut left,
                &mut left_slops,
                &right,
                2,
                true,
                &mut buffer,
                &mut slop_buffer,
            )
        });
    }

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
    fn bench_intersection_medium_slop_carrying(b: &mut Bencher) {
        let mut left = Vec::new();
        let mut left_slops: Vec<u32> = Vec::new();
        let mut buffer = Vec::new();
        let mut slop_buffer = Vec::new();
        let left_data: Vec<u32> = (0..100).collect();
        let left_slop_data: Vec<u32> = (0..100).map(|_| 0).collect();

        b.iter(|| {
            left.clear();
            left.extend_from_slice(&left_data);
            left_slops.clear();
            left_slops.extend_from_slice(&left_slop_data);
            let right = [5, 7, 55, 200];

            intersection_count_with_carrying_slop(
                &mut left,
                &mut left_slops,
                &right,
                2,
                true,
                &mut buffer,
                &mut slop_buffer,
            )
        });
    }

    #[bench]
    fn bench_intersection_medium_slop(b: &mut Bencher) {
        let mut left = Vec::new();
        let left_data: Vec<u32> = (0..100).collect();

        b.iter(|| {
            left.clear();
            left.extend_from_slice(&left_data);
            let right = [5, 7, 55, 200];
            intersection_count_with_slop(&mut left, &right[..], 2, true) as u32
        });
    }

    #[bench]
    fn bench_intersection_medium(b: &mut Bencher) {
        let mut left = Vec::new();
        let left_data: Vec<u32> = (0..100).collect();
        b.iter(|| {
            left.clear();
            left.extend_from_slice(&left_data);
            let right = [5, 7, 55, 200];
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
