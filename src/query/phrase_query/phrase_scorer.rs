use query::Scorer;
use DocSet;
use postings::SegmentPostings;
use postings::Postings;
use postings::IntersectionDocSet;
use DocId;

pub struct PhraseScorer<'a> {
    pub intersection_docset: IntersectionDocSet<SegmentPostings<'a>>,
    pub positions_offsets: Vec<u32>,
}

impl<'a> PhraseScorer<'a> {
    fn phrase_match(&self) -> bool {
        let mut positions_arr: Vec<&[u32]> = self.intersection_docset
            .docsets()
            .iter()
            .map(|posting| {
                posting.positions()
            })
            .collect();
        println!("positions arr {:?}", positions_arr);
        
        
        let mut cur = 0;
        'outer: loop {
            for i in 0..positions_arr.len() {
                println!("i {}", i);
                let positions: &mut &[u32] = &mut positions_arr[i];
                if positions.len() == 0 {
                    println!("NOPE");
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
        while self.intersection_docset.advance() {
            println!("doc {}", self.intersection_docset.doc());
            if self.phrase_match() {
                println!("return {}", self.intersection_docset.doc());
                return true;
            }
        }
        false
    }

    fn doc(&self,) -> DocId {
        self.intersection_docset.doc()
    }
}


impl<'a> Scorer for PhraseScorer<'a> {
    fn score(&self,) -> f32 {
        1f32
    }
    
}
