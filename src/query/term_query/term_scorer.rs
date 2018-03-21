use Score;
use DocId;
use docset::{DocSet, SkipResult};
use query::Scorer;

use postings::Postings;
use fieldnorm::FieldNormReader;

pub struct TermScorer<TPostings: Postings> {
    pub idf: Score,
    pub fieldnorm_reader_opt: Option<FieldNormReader>,
    pub postings: TPostings,
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
        let doc = self.postings.doc();
        let tf = match self.fieldnorm_reader_opt {
            Some(ref fieldnorm_reader) => {
                let field_norm = fieldnorm_reader.fieldnorm(doc);
                (self.postings.term_freq() as f32 / field_norm as f32)
            }
            None => self.postings.term_freq() as f32,
        };
        self.idf * tf.sqrt()
    }
}
