use query::Scorer;
use DocSet;
use postings::SegmentPostings;
use postings::Postings;
use postings::IntersectionDocSet;
use DocId;

pub struct PhraseScorer {
    pub intersection_docset: IntersectionDocSet<SegmentPostings>,
}

impl PhraseScorer {
    fn phrase_match(&self) -> bool {
        let mut positions_arr: Vec<&[u32]> = self.intersection_docset
            .docsets()
            .iter()
            .map(|posting| posting.positions())
            .collect();

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
