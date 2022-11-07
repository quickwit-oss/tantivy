use crate::docset::DocSet;
use crate::postings::Postings;
use crate::query::Scorer;
use crate::DocId;

pub trait PhraseScorer: Scorer {
    fn phrase_count(&self) -> u32;
    fn phrase_match(&mut self) -> bool;
    fn phrase_exists(&mut self) -> bool;
}
pub(crate) struct PostingsWithOffset<TPostings> {
    offset: u32,
    postings: TPostings,
}

impl<TPostings: Postings> PostingsWithOffset<TPostings> {
    pub fn new(segment_postings: TPostings, offset: u32) -> PostingsWithOffset<TPostings> {
        PostingsWithOffset {
            offset,
            postings: segment_postings,
        }
    }

    pub fn positions(&mut self, output: &mut Vec<u32>) {
        self.postings.positions_with_offset(self.offset, output)
    }
}

impl<TPostings: Postings> DocSet for PostingsWithOffset<TPostings> {
    fn advance(&mut self) -> DocId {
        self.postings.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.postings.seek(target)
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

/// In order to check if terms are adjacent, we offset the terms' positions in the field relatively
/// to their position in the phrase. It then becomes a list intersection problem.
pub(crate) fn get_postings_with_offset<TPostings: Postings>(
    term_postings: Vec<(usize, TPostings)>,
) -> Vec<PostingsWithOffset<TPostings>> {
    let max_offset = term_postings
        .iter()
        .map(|&(offset, _)| offset)
        .max()
        .unwrap_or(0);
    term_postings
        .into_iter()
        .map(|(offset, postings)| PostingsWithOffset::new(postings, (max_offset - offset) as u32))
        .collect::<Vec<_>>()
}
