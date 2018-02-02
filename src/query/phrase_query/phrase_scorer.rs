use query::Scorer;
use DocId;
use postings::{DocSet, IntersectionDocSet, Postings, SegmentPostings, SkipResult};

struct PostingsWithOffset {
    offset: u32,
    segment_postings: SegmentPostings,
}

impl PostingsWithOffset {
    pub fn new(segment_postings: SegmentPostings, offset: u32) -> PostingsWithOffset {
        PostingsWithOffset {
            offset,
            segment_postings,
        }
    }
}

impl Postings for PostingsWithOffset {
    fn term_freq(&self) -> u32 {
        self.segment_postings.term_freq()
    }

    fn positions(&self) -> &[u32] {
        self.segment_postings.positions()
    }
}

impl DocSet for PostingsWithOffset {
    fn advance(&mut self) -> bool {
        self.segment_postings.advance()
    }

    fn doc(&self) -> DocId {
        self.segment_postings.doc()
    }

    fn size_hint(&self) -> usize {
        self.segment_postings.size_hint()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.segment_postings.skip_next(target)
    }
}

pub struct PhraseScorer {
    intersection_docset: IntersectionDocSet<PostingsWithOffset>,
}

impl PhraseScorer {
    pub fn new(term_postings: Vec<SegmentPostings>) -> PhraseScorer {
        let postings_with_offsets: Vec<_> = term_postings
            .into_iter()
            .enumerate()
            .map(|(offset, postings)| PostingsWithOffset::new(postings, offset as u32))
            .collect();
        PhraseScorer {
            intersection_docset: IntersectionDocSet::from(postings_with_offsets),
        }
    }

    fn phrase_match(&self) -> bool {
        // TODO maybe we could avoid decoding positions lazily for all terms
        // when there is > 2 terms.
        //
        // For instance for the query "A B C", the position of "C" do not need
        // to be decoded if "A B" had no match.
        let docsets = self.intersection_docset.docsets();
        let mut positions_arr: Vec<&[u32]> = vec![&[]; docsets.len()];
        for docset in docsets {
            positions_arr[docset.offset as usize] = docset.positions();
        }

        let num_postings = positions_arr.len() as u32;

        let mut ord = 1u32;
        let mut pos_candidate = positions_arr[0][0];
        positions_arr[0] = &(positions_arr[0])[1..];
        let mut count_matching = 1;

        #[cfg_attr(feature = "cargo-clippy", allow(never_loop))]
        'outer: loop {
            let target = pos_candidate + ord;
            let positions = positions_arr[ord as usize];
            for (i, pos_i) in positions.iter().cloned().enumerate() {
                if pos_i < target {
                    continue;
                }
                if pos_i == target {
                    count_matching += 1;
                    if count_matching == num_postings {
                        return true;
                    }
                } else if pos_i > target {
                    count_matching = 1;
                    pos_candidate = positions[i] - ord;
                    positions_arr[ord as usize] = &(positions_arr[ord as usize])[(i + 1)..];
                }
                ord += 1;
                if ord == num_postings {
                    ord = 0;
                }
                continue 'outer;
            }
            return false;
        }
    }
}

impl DocSet for PhraseScorer {
    fn advance(&mut self) -> bool {
        while self.intersection_docset.advance() {
            if self.phrase_match() {
                return true;
            }
        }
        false
    }

    fn doc(&self) -> DocId {
        self.intersection_docset.doc()
    }

    fn size_hint(&self) -> usize {
        self.intersection_docset.size_hint()
    }
}

impl Scorer for PhraseScorer {
    fn score(&self) -> f32 {
        1f32
    }
}
