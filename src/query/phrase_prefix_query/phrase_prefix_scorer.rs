use crate::docset::{DocSet, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::phrase_query::{intersection_count, intersection_count_with_slop, PhraseScorer};
use crate::query::Scorer;
use crate::{DocId, Score};

pub struct PhrasePrefixScorer<TPostings: Postings> {
    phrase_scorer: PhraseScorer<TPostings>,
    suffixes: Vec<TPostings>,
    suffix_offset: u32,
    phrase_count: u32,
    slop: u32,
}

impl<TPostings: Postings> PhrasePrefixScorer<TPostings> {
    // If similarity_weight is None, then scoring is disabled.
    pub fn new(
        term_postings: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        suffixes: Vec<TPostings>,
        suffixe_pos: usize,
    ) -> PhrasePrefixScorer<TPostings> {
        // correct indices so we can merge with our suffix term the PhraseScorer doesn't know about

        let max_offset = term_postings
            .iter()
            .map(|(pos, _)| *pos)
            .chain(std::iter::once(suffixe_pos))
            .max()
            .unwrap();

        let mut res = PhrasePrefixScorer {
            phrase_scorer: PhraseScorer::new_with_offset(
                term_postings,
                similarity_weight_opt,
                fieldnorm_reader,
                slop,
                1,
            ),
            suffixes,
            suffix_offset: (max_offset - suffixe_pos) as u32,
            phrase_count: 0,
            slop,
        };
        if !res.matches_prefix() {
            res.advance();
        }
        res
    }

    pub fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    fn matches_prefix(&mut self) -> bool {
        let mut count = 0;
        let mut positions = Vec::new();
        let current_doc = self.doc();
        let pos_matching = self.phrase_scorer.get_intersection();
        for suffix in &mut self.suffixes {
            if suffix.doc() > current_doc {
                continue;
            }
            let doc = suffix.seek(current_doc);
            if doc == current_doc {
                suffix.positions_with_offset(self.suffix_offset, &mut positions);
                if self.slop == 0 {
                    count += intersection_count(pos_matching, &positions);
                } else {
                    count += intersection_count_with_slop(pos_matching, &positions, self.slop);
                }
            }
        }
        self.phrase_count = count as u32;
        count != 0
    }
}

impl<TPostings: Postings> DocSet for PhrasePrefixScorer<TPostings> {
    fn advance(&mut self) -> DocId {
        loop {
            let doc = self.phrase_scorer.advance();
            if doc == TERMINATED || self.matches_prefix() {
                return doc;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.phrase_scorer.seek(target);
        let doc = self.phrase_scorer.seek(target);
        if doc == TERMINATED || self.matches_prefix() {
            return doc;
        }
        self.advance()
    }

    fn doc(&self) -> DocId {
        self.phrase_scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.phrase_scorer.size_hint()
    }
}

impl<TPostings: Postings> Scorer for PhrasePrefixScorer<TPostings> {
    fn score(&mut self) -> Score {
        // TODO modify score??
        self.phrase_scorer.score()
    }
}
