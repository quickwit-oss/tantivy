use DocId;
use docset::{DocSet, SkipResult};
use postings::Postings;
use query::{Intersection, Scorer};

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

    pub fn positions(&mut self, output: &mut Vec<u32>) {
        self.postings.positions_with_offset(self.offset, output)
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
    left: Vec<u32>,
    right: Vec<u32>
}


/// Computes the length of the intersection of two sorted arrays.
fn intersection_count(left: &[u32], right: &[u32]) -> usize {
    let mut left_i = 0;
    let mut right_i = 0;
    let mut count = 0;
    while left_i < left.len() && right_i < right.len() {
        let left_val = left[left_i];
        let right_val = right[right_i];
        if left_val < right_val {
            left_i += 1;
        } else if right_val < left_val {
            right_i += 1;
        } else {
            count += 1;
            left_i += 1;
            right_i += 1;
        }
    }
    count
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in left.
///
/// Returns the length of the intersection
fn intersection(left: &mut [u32], right: &[u32]) -> usize {
    let mut left_i = 0;
    let mut right_i = 0;
    let mut count = 0;
    let left_len = left.len();
    let right_len = right.len();
    while left_i < left_len && right_i < right_len {
        let left_val = left[left_i];
        let right_val = right[right_i];
        if left_val < right_val {
            left_i += 1;
        } else if right_val < left_val {
            right_i += 1;
        } else {
            left[count] = left_val;
            count += 1;
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
            left: Vec::with_capacity(100),
            right: Vec::with_capacity(100)
        }
    }

    fn phrase_match(&mut self) -> bool {
        // TODO early exit when we don't care about the phrase frequency
        {
            self.intersection_docset.docset_mut_specialized(0).positions(&mut self.left);
        }
        let mut intersection_len = self.left.len();
        for i in 1..self.num_docsets - 1 {
            {
                self.intersection_docset.docset_mut_specialized(i).positions(&mut self.right);
            }
            intersection_len = intersection(&mut self.left[..intersection_len], &self.right[..]);
            if intersection_len == 0 {
                return false;
            }
        }

        self.intersection_docset.docset_mut_specialized(self.num_docsets - 1).positions(&mut self.right);
        intersection_len = intersection_count(&mut self.left[..intersection_len], &self.right[..]);
        intersection_len > 0
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

#[cfg(test)]
mod tests {

    use test::Bencher;
    use super::{intersection_count, intersection};

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