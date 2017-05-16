use query::Scorer;
use DocSet;
use postings::SegmentPostings;
use postings::Postings;
use postings::IntersectionDocSet;
use DocId;

pub struct PhraseScorer<'a> {
    pub intersection_docset: IntersectionDocSet<SegmentPostings<'a>>,
}


impl<'a> PhraseScorer<'a> {
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
            for i in 0..positions.len() {
                let pos_i = positions[i];
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

impl<'a> DocSet for PhraseScorer<'a> {
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
}


impl<'a> Scorer for PhraseScorer<'a> {
    fn score(&self) -> f32 {
        1f32
    }
}
