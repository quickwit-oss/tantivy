use crate::docset::{DocSet, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::phrase_query::{intersection_count, intersection_count_with_slop, PhraseScorer};
use crate::query::Scorer;
use crate::{DocId, Score};

enum PhraseKind<TPostings: Postings> {
    SinglePrefix(u32, TPostings, Vec<u32>),
    MultiPrefix(PhraseScorer<TPostings>),
}

impl<TPostings: Postings> PhraseKind<TPostings> {
    fn get_intersection(&mut self) -> &[u32] {
        match self {
            PhraseKind::SinglePrefix(offset, postings, indexes) => {
                if indexes.is_empty() {
                    postings.positions_with_offset(*offset, indexes);
                }
                indexes
            }
            PhraseKind::MultiPrefix(postings) => postings.get_intersection(),
        }
    }
}

impl<TPostings: Postings> DocSet for PhraseKind<TPostings> {
    fn advance(&mut self) -> DocId {
        match self {
            PhraseKind::SinglePrefix(_, postings, indexes) => {
                indexes.clear();
                postings.advance()
            }
            PhraseKind::MultiPrefix(postings) => postings.advance(),
        }
    }

    fn doc(&self) -> DocId {
        match self {
            PhraseKind::SinglePrefix(_, postings, _) => postings.doc(),
            PhraseKind::MultiPrefix(postings) => postings.doc(),
        }
    }

    fn size_hint(&self) -> u32 {
        match self {
            PhraseKind::SinglePrefix(_, postings, _) => postings.size_hint(),
            PhraseKind::MultiPrefix(postings) => postings.size_hint(),
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        match self {
            PhraseKind::SinglePrefix(_, postings, indexes) => {
                indexes.clear();
                postings.seek(target)
            }
            PhraseKind::MultiPrefix(postings) => postings.seek(target),
        }
    }
}

impl<TPostings: Postings> Scorer for PhraseKind<TPostings> {
    fn score(&mut self) -> Score {
        match self {
            PhraseKind::SinglePrefix(_, _postings, indexes) => {
                if indexes.is_empty() {
                    0.0
                } else {
                    1.0
                }
            }
            PhraseKind::MultiPrefix(postings) => postings.score(),
        }
    }
}

pub struct PhrasePrefixScorer<TPostings: Postings> {
    phrase_scorer: PhraseKind<TPostings>,
    suffixes: Vec<TPostings>,
    suffix_offset: u32,
    phrase_count: u32,
    slop: u32,
}

impl<TPostings: Postings> PhrasePrefixScorer<TPostings> {
    // If similarity_weight is None, then scoring is disabled.
    pub fn new(
        mut term_postings: Vec<(usize, TPostings)>,
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

        let phrase_scorer = if term_postings.len() > 1 {
            PhraseKind::MultiPrefix(PhraseScorer::new_with_offset(
                term_postings,
                similarity_weight_opt,
                fieldnorm_reader,
                slop,
                1,
            ))
        } else {
            let (pos, posting) = term_postings
                .pop()
                .expect("PhrasePrefixScorer must have at least two terms");
            let offset = suffixe_pos - pos;
            PhraseKind::SinglePrefix(offset as u32, posting, Vec::with_capacity(100))
        };
        let mut res = PhrasePrefixScorer {
            phrase_scorer,
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
                    count += intersection_count(dbg!(pos_matching), dbg!(&positions));
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
