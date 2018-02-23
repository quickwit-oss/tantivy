use DocId;
use docset::{DocSet, SkipResult};
use postings::Postings;
use query::{Intersection, Scorer};
use std::mem;


struct PostingsWithOffset<TPostings> {
    offset: u32,
    postings: TPostings
}

impl<TPostings: Postings> PostingsWithOffset<TPostings> {
    pub fn new(segment_postings: TPostings, offset: u32) -> PostingsWithOffset<TPostings> {
        PostingsWithOffset {
            offset,
            postings: segment_postings
        }
    }

    pub fn positions(&self) -> &[u32] {
        self.postings.positions_with_offset(self.offset)
    }
}

impl<TPostings: Postings> DocSet for PostingsWithOffset<TPostings> {
    fn advance(&mut self) -> bool {
        self.postings.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.postings.skip_next(target)
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
    num_docsets: usize,
    source: Vec<u32>,
    result: Vec<u32>
}

fn intersection_arr(left: &[u32], right: &[u32], output: &mut [u32]) -> usize {
    let mut left_i = 0;
    let mut right_i = 0;
    let mut count = 0;
    while left_i < left.len() && right_i < right.len() {
        if left[left_i] < right[right_i] {
            left_i += 1;
        } else if right[right_i] < left[left_i] {
            right_i += 1;
        } else {
            output[count] = left[left_i];
            count+=1;
            left_i += 1;
            right_i += 1;
        }
    }
    count
}

impl<TPostings: Postings> PhraseScorer<TPostings> {

    pub fn new(term_postings: Vec<TPostings>) -> PhraseScorer<TPostings> {
        let num_docsets = term_postings.len();
        let postings_with_offsets = term_postings
            .into_iter()
            .enumerate()
            .map(|(offset, postings)| PostingsWithOffset::new(postings, (num_docsets - offset) as u32))
            .collect::<Vec<_>>();
        PhraseScorer {
            intersection_docset: Intersection::new(postings_with_offsets),
            num_docsets,
            source: Vec::with_capacity(100),
            result: Vec::with_capacity(100)
        }
    }

    fn phrase_match(&mut self) -> bool {
        // TODO early exit when we don't care about th phrase frequency
        let mut intersection_len;
        {
            let left = self.intersection_docset.docset(0).positions();
            let right = self.intersection_docset.docset(1).positions();
            let max_intersection_len = left.len().min(right.len());
            if max_intersection_len > self.result.len() {
                self.result.resize(max_intersection_len, 0u32);
                self.source.resize(max_intersection_len, 0u32)
            }
            intersection_len = intersection_arr(left, right, &mut self.result[..]);
        }
        if intersection_len == 0 {
            return false;
        }
        for i in 2..self.num_docsets {
            mem::swap(&mut self.source, &mut self.result);
            let term_positions = self.intersection_docset.docset(i).positions();
            intersection_len = intersection_arr(
                &self.source[..intersection_len],
                term_positions,
                &mut self.result[..]);
            if intersection_len == 0 {
                return false;
            }
        }
        return true;
    }
}

impl<TPostings: Postings> DocSet for PhraseScorer<TPostings> {
    fn advance(&mut self) -> bool {
        while self.intersection_docset.advance() {
            if self.phrase_match() {
                return true;
            }
        }
        false
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if self.intersection_docset.skip_next(target) == SkipResult::End {
            return SkipResult::End;
        }
        if self.phrase_match() {
            if self.doc() == target {
                return SkipResult::Reached;
            } else {
                return SkipResult::OverStep;
            }
        }
        if self.advance() {
            SkipResult::OverStep
        } else {
            SkipResult::End
        }
    }

    fn doc(&self) -> DocId {
        self.intersection_docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.intersection_docset.size_hint()
    }
}

impl<TPostings: Postings> Scorer for PhraseScorer<TPostings> {
    fn score(&mut self) -> f32 {
        1f32
    }
}
