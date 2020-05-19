use crate::{Score, DocId, TERMINATED, DocSet};
use crate::query::term_query::TermScorer;
use crate::postings::BlockSegmentPostings;
use futures::AsyncSeekExt;


struct BlockWAND {
    term_scorers : Vec<TermScorer>,
}


fn find_pivot_doc(term_scorers: &[TermScorer], threshold: f32) -> DocId {
    let mut max_score = 0.0f32;
    for term_scorer in term_scorers.iter() {
        max_score += term_scorer.max_score();
        if max_score > threshold {
            return term_scorer.doc();
        }
    }
    TERMINATED
}

fn shallow_advance(scorers: &mut Vec<TermScorer>, pivot: DocId) -> Score {
    let mut max_block_score = 0.0f32;
    let mut i = 0;
    while i < scorers.len() {
        if scorers[i].doc() > pivot {
            break;
        }
       while scorers[i].postings.block_cursor.skip_reader.doc() < pivot {
            if scorers[i].postings.block_cursor.skip_reader.advance() {
                max_block_score += scorers[i].postings.block_cursor.skip_reader.block_max_score();
                i += 1;
            } else {
                scorers.swap_remove(i);
            }
        }
    }
    max_block_score
}

pub fn block_wand(mut scorers: Vec<TermScorer>, mut threshold: f32, callback: &mut dyn FnMut(u32, Score) -> Score) {
    loop {
        scorers.sort_by_key(|scorer| scorer.doc());
        let pivot_doc = find_pivot_doc(&scorers, threshold);
        if pivot_doc == TERMINATED {
            return;
        }
        if shallow_advance(&mut scorers, pivot_doc) > threshold {
            if scorers[0].doc() == pivot_doc {
                // EvaluatePartial(d , p);
                // Move all pointers from lists[0] to lists[p] by calling
                // Next(list, d + 1)
            } else {
                let scorer_id = scorers.iter_mut()
                    .take_while(|term_scorer| term_scorer.doc() < pivot_doc)
                    .enumerate()
                    .min_by_key(|(scorer_id, scorer)| scorer.doc_freq())
                    .map(|(scorer_id, scorer)| scorer_id)
                    .unwrap();
                if scorers[scorer_id].seek(pivot_doc) == TERMINATED {
                    scorers.swap_remove(scorer_id);
                }
            }

        } else {
            //d = GetNewCandidate();
            //Choose one list from the lists before and including lists[p]
            //with the largest IDF, move it by calling Next(list, d)
        }
    }

}