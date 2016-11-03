use query::Scorer;
use DocSet;
use query::term_query::TermScorer;
use query::boolean_query::BooleanScorer;
use postings::SegmentPostings;
use postings::Postings;
use DocId;

pub struct PhraseScorer<'a> {
    pub all_term_scorer: BooleanScorer<TermScorer<SegmentPostings<'a>>>,
    pub positions_offsets: Vec<u32>,
}

impl<'a> PhraseScorer<'a> {
    fn phrase_match(&self) -> bool {
        println!("phrase_match");
        let mut positions_arr: Vec<&[u32]> = self.all_term_scorer
            .scorers()
            .iter()
            .map(|scorer| {
                println!("{:?}", scorer.doc());
                scorer.postings().positions()
            })
            .collect();
        println!("positions arr {:?}", positions_arr);
        let mut cur = 0;
        'outer: loop {
            for i in 0..positions_arr.len() {
                let positions: &mut &[u32] = &mut positions_arr[i];
                println!("{} {:?} {:?}", i, positions, self.positions_offsets);
                if positions.len() == 0 {
                    return false;
                }
                let head_position = positions[0] + self.positions_offsets[i];
                println!("cur: {}, head_position {}", cur, head_position);
                while head_position < cur {
                    if positions.len() == 1 {
                        return false;
                    }
                    *positions = &(*positions)[1..];
                }
                if head_position != cur {
                    cur = head_position;
                    continue 'outer;
                }
            }
            return true;
        }
    }
}

impl<'a> DocSet for PhraseScorer<'a> {
    fn advance(&mut self,) -> bool {
        println!("docset advance");
        while self.all_term_scorer.advance() {
            if self.phrase_match() {
                return true;
            }
        }
        false
    }

    fn doc(&self,) -> DocId {
        self.all_term_scorer.doc()
    }
}


impl<'a> Scorer for PhraseScorer<'a> {
    fn score(&self,) -> f32 {
        self.all_term_scorer.score()
    }
    
}
