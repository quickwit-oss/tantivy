use crate::query::term_query::TermScorer;
use crate::query::Scorer;
use crate::{DocId, DocSet, Score, TERMINATED};

/// Returns the lowest document that has a chance of exceeding the
/// threshold score.
///
/// term_scorers are assumed sorted by .doc().
fn find_pivot_doc(term_scorers: &[TermScorer], threshold: f32) -> Option<DocId> {
    let mut max_score = 0.0f32;
    for term_scorer in term_scorers {
        max_score += term_scorer.max_score();
        if max_score > threshold {
            return Some(term_scorer.doc());
        }
    }
    None
}

fn shallow_advance(scorers: &mut Vec<TermScorer>, pivot: DocId) -> Score {
    let mut block_max_score_upperbound = 0.0f32;
    for scorer in scorers {
        if scorer.doc() > pivot {
            break;
        }
        scorer.postings.block_cursor.seek(pivot);
        block_max_score_upperbound += scorer.postings.block_cursor.skip_reader.block_max_score();
    }
    block_max_score_upperbound
}

fn compute_score(scorers: &mut Vec<TermScorer>, doc: DocId) -> Score {
    let mut i = 0;
    let mut score = 0.0f32;
    while i < scorers.len() {
        if scorers[i].doc() > doc {
            break;
        }
        if scorers[i].seek(doc) == TERMINATED {
            scorers.swap_remove(i);
        } else {
            score += scorers[i].score();
            i += 1;
        }
    }
    score
}

fn advance_all_scorers(scorers: &mut Vec<TermScorer>, pivot: DocId) {
    let mut i = 0;
    while i < scorers.len() {
        if scorers[i].doc() == pivot {
            if scorers[i].advance() == TERMINATED {
                scorers.swap_remove(i);
                continue;
            }
        }
        i += 1;
    }
}

pub fn block_wand(
    mut scorers: Vec<TermScorer>,
    mut threshold: f32,
    callback: &mut dyn FnMut(u32, Score) -> Score,
) {
    loop {
        scorers.sort_by_key(|scorer| scorer.doc());
        let pivot_opt = find_pivot_doc(&scorers, threshold);
        if let Some(pivot_doc) = pivot_opt {
            let block_max_score_upperbound = shallow_advance(&mut scorers, pivot_doc);
            // TODO bug: more than one scorer can point on the pivot.
            if block_max_score_upperbound <= threshold {
                // TODO choose a better candidate.
                if scorers[0].seek(pivot_doc + 1) == TERMINATED {
                    scorers.swap_remove(0);
                }
                continue;
            }

            if scorers[0].doc() != pivot_doc {
                // all scorers are not aligned on pivot_doc.
                if let Some(scorer_ord) = scorers
                    .iter_mut()
                    .take_while(|scorer| scorer.doc() < pivot_doc)
                    .enumerate()
                    .min_by_key(|(_ord, scorer)| scorer.doc_freq())
                    .map(|(ord, _scorer)| ord)
                {
                    // TODOD FIX seek, right now the block will never get loaded.
                    if scorers[scorer_ord].seek(pivot_doc) == TERMINATED {
                        scorers.swap_remove(scorer_ord);
                    }
                    continue;
                }
            }
            // TODO no need to fully score?
            let score = compute_score(&mut scorers, pivot_doc);
            if score > threshold {
                threshold = callback(pivot_doc, score);
            }
            advance_all_scorers(&mut scorers, pivot_doc);
        } else {
            return;
        }
    }
}
