use Score;
use DocId;
use docset::{DocSet, SkipResult};
use query::Scorer;

use postings::Postings;
use fieldnorm::FieldNormReader;

pub struct TermScorer<TPostings: Postings> {
    pub fieldnorm_reader_opt: Option<FieldNormReader>,
    pub postings: TPostings,
    pub weight: f32,
    pub cache: [f32; 256],
}

impl<TPostings: Postings> DocSet for TermScorer<TPostings> {
    fn advance(&mut self) -> bool {
        self.postings.advance()
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.postings.skip_next(target)
    }
}

impl<TPostings: Postings> Scorer for TermScorer<TPostings> {
    fn score(&mut self) -> Score {
        let doc = self.doc();
        let fieldnorm_id = self.fieldnorm_reader_opt
            .as_ref()
            .map(|fieldnorm_reader| {
                fieldnorm_reader.fieldnorm_id(doc)
            })
            .unwrap_or(0u8);
        let norm = self.cache[fieldnorm_id as usize];
        let term_freq = self.postings.term_freq() as f32;
        self.weight * term_freq / (term_freq + norm)
    }
}

