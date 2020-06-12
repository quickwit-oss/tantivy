use crate::query::term_query::TermScorer;
use crate::query::Scorer;
use crate::{DocId, DocSet, Score, TERMINATED};
use std::ops::DerefMut;
use std::ops::Deref;

/// Takes a term_scorers sorted by their current doc() and a threshold and returns
/// Returns (pivot_len, pivot_ord) defined as follows:
/// - `pivot_doc` lowest document that has a chance of exceeding (>) the threshold score.
/// - `before_pivot_len` number of term_scorers such that term_scorer.doc() < pivot.
/// - `pivot_len` number of term_scorers such that term_scorer.doc() <= pivot.
///
/// We always have `before_pivot_len` < `pivot_len`.
///
/// None is returned if we establish that no document can exceed the threshold.
fn find_pivot_doc(term_scorers: &[TermScorerWithMaxScore], threshold: f32) -> Option<(usize, usize, DocId)> {
    let mut max_score = 0.0f32;
    let mut before_pivot_len = 0;
    let mut pivot_doc = TERMINATED;
    while before_pivot_len < term_scorers.len() {
        let term_scorer = &term_scorers[before_pivot_len];
        max_score += term_scorer.max_score;
        if max_score > threshold {
            pivot_doc = term_scorer.doc();
            break;
        }
        before_pivot_len += 1;
    }
    if pivot_doc == TERMINATED {
        return None;
    }
    // Right now i is an ordinal, we want a len.
    let mut pivot_len = before_pivot_len + 1;
    // Some other term_scorer may be positioned on the same document.
    pivot_len += term_scorers[pivot_len..].iter()
        .take_while(|term_scorer| term_scorer.doc() == pivot_doc)
        .count();
    Some((before_pivot_len, pivot_len, pivot_doc))
}

// Shallow advance all of the scorers that are positioned before or on pivot,
// and returns their number as well as the sum of their block_max_score.
//
// This gives us a tighter upperbound of DocId score, without decoding
// any blocks.
//
// After calling this method, some TermScorer might be in a state in which
// their block is not loaded.
fn shallow_advance(scorers: &mut Vec<TermScorerWithMaxScore>, pivot: DocId) -> Score {
    scorers.iter_mut()
        .map(|scorer| {
            scorer.shallow_seek(pivot);
            scorer.block_max_score()
        })
        .sum()
}

struct TermScorerWithMaxScore<'a> {
    scorer: &'a mut TermScorer,
    max_score: f32,
}

impl<'a> From<&'a mut TermScorer> for TermScorerWithMaxScore<'a> {
    fn from(scorer: &'a mut TermScorer) -> Self {
        let max_score = scorer.max_score();
        TermScorerWithMaxScore {
            scorer,
            max_score
        }
    }
}

impl<'a> Deref for TermScorerWithMaxScore<'a> {
    type Target = TermScorer;

    fn deref(&self) -> &Self::Target {
        self.scorer
    }
}


impl<'a> DerefMut for TermScorerWithMaxScore<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scorer
    }
}

// Before and after calling this method, scorers need to be sorted by their `.doc()`.
fn block_max_was_too_low_advance_one_scorer(scorers: &mut Vec<TermScorerWithMaxScore>, pivot_len: usize) {
    let mut scorer_to_seek = pivot_len - 1;
    let mut doc_to_seek_after = scorers[scorer_to_seek].doc();
    for scorer_ord in (0..pivot_len - 1).rev() {
        let scorer = &scorers[scorer_ord];
        if scorer.last_doc_in_block() <= doc_to_seek_after {
            doc_to_seek_after = scorer.last_doc_in_block();
            scorer_to_seek = scorer_ord;
        }
    }
    for scorer in &scorers[pivot_len..] {
        if scorer.doc() <= doc_to_seek_after {
            doc_to_seek_after = scorer.doc();
        }
    }
    scorers[scorer_to_seek].seek(doc_to_seek_after + 1);
    restore_ordering(scorers, scorer_to_seek);
}


// Given a list of term_scorers and a `ord` and assuming that `term_scorers[ord]` is sorted
// except term_scorers[ord] that might be in advance compared to its ranks,
// bubble up term_scorers[ord] in order to restore the ordering.
fn restore_ordering(term_scorers: &mut Vec<TermScorerWithMaxScore>, ord: usize) {
    let doc = term_scorers[ord].doc();
    for i in ord + 1..term_scorers.len() {
        if term_scorers[i].doc() >= doc {
            break;
        }
        term_scorers.swap(i, i - 1);
    }
}


// Attempts to advance all term_scorers between `&term_scorers[0..before_len]` to the pivot.
// If this works, return true.
// If this fails (ie: one of the term_scorer does not contain `pivot_doc` and seek goes past the
// pivot), reorder the term_scorers to ensure the list is still sorted and returns `false`.
// If a term_scorer reach TERMINATED in the process return false remove the term_scorer and return.
fn align_scorers(term_scorers: &mut Vec<TermScorerWithMaxScore>, pivot_doc: DocId, before_pivot_len: usize) -> bool {
    debug_assert_ne!(pivot_doc, TERMINATED);
    for i in (0..before_pivot_len).rev() {
        let new_doc = term_scorers[i].seek(pivot_doc);
        if new_doc != pivot_doc {
            if new_doc == TERMINATED {
                term_scorers.swap_remove(i);
            }
            // We went past the pivot.
            // We just go through the outer loop mechanic (Note that pivot is
            // still a possible candidate).
            //
            // Termination is still guaranteed since we can only consider the same
            // pivot at most term_scorers.len() - 1 times.
            restore_ordering(term_scorers, i);
            return false;
        }
    }
    return true;
}

// Assumes terms_scorers[..pivot_len] are positioned on the same doc (pivot_doc).
// Advance term_scorers[..pivot_len] and out of these removes the terminated scores.
// Restores the ordering of term_scorers.
fn advance_all_scorers_on_pivot(term_scorers: &mut Vec<TermScorerWithMaxScore>, pivot_len: usize) {
    let mut i = 0;
    for _ in 0..pivot_len {
        if term_scorers[i].advance() == TERMINATED {
            term_scorers.swap_remove(i);
        } else {
            i += 1;
        }
    }
    term_scorers.sort_by_key(|scorer| scorer.doc());
}

pub fn block_wand(
    mut scorers: Vec<TermScorer>,
    mut threshold: f32,
    callback: &mut dyn FnMut(u32, Score) -> Score,
) {
    let mut scorers: Vec<TermScorerWithMaxScore> = scorers.iter_mut().map(TermScorerWithMaxScore::from).collect();
    scorers.sort_by_key(|scorer| scorer.doc());
    loop {
        // At this point we need to ensure that the scorers are sorted!
        if let Some((before_pivot_len, pivot_len, pivot_doc)) = find_pivot_doc(&scorers[..], threshold) {
            debug_assert_ne!(pivot_doc, TERMINATED);
            debug_assert!(before_pivot_len < pivot_len);
            let block_max_score_upperbound: Score =  scorers[..pivot_len].iter_mut()
                .map(|scorer| {
                    scorer.shallow_seek(pivot_doc);
                    scorer.block_max_score()
                })
                .sum();

            // Beware after shallow advance, skip readers can be in advance compared to
            // the segment posting lists.
            //
            // `block_segment_postings.load_block()` need to be called separately.
            if block_max_score_upperbound <= threshold {
                // Block max condition was not reached.
                // We could get away by simply advancing the scorers to DocId + 1 but it would
                // be inefficient. The optimization requires proper explanation and was
                // isolated in a different function.
                block_max_was_too_low_advance_one_scorer(&mut scorers, pivot_len);
                continue;
            }

            // Block max condition is observed.
            //
            // Let's try and advance all scorers before the pivot to the pivot.
            if !align_scorers(&mut scorers, pivot_doc, before_pivot_len) {
                // At least of the scorer does not contain the pivot.
                //
                // Let's stop scoring this pivot and go through the pivot selection again.
                // Note that the current pivot is not necessarily a bad candidate and it
                // may be picked again.
                continue;
            }

            // At this point, all scorers are positioned on the doc.
            let score = scorers[..pivot_len]
                .iter_mut()
                .map(|scorer| scorer.score())
                .sum();
            if score > threshold {
                threshold = callback(pivot_doc, score);
            }
            // let's advance all of the scorers that are currently positioned on the pivot.
            advance_all_scorers_on_pivot(&mut scorers, pivot_len);

        } else {
            return;
        }
    }
}
