use Score;
use DocId;
use docset::{DocSet, SkipResult};
use postings::SegmentPostings;
use query::Scorer;
use postings::Postings;
use fastfield::FastFieldReader;
use postings::{NoDelete, DeleteSet};

pub struct TermScorer<TDeleteSet: DeleteSet=NoDelete> {
    pub idf: Score,
    pub fieldnorm_reader_opt: Option<FastFieldReader<u64>>,
    pub postings: SegmentPostings<TDeleteSet>,
}

impl<TDeleteSet: DeleteSet> TermScorer<TDeleteSet> {
    pub fn postings(&self) -> &SegmentPostings<TDeleteSet> {
        &self.postings
    }
}

impl<TDeleteSet: DeleteSet> DocSet for TermScorer<TDeleteSet> {
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

impl<TDeleteSet: DeleteSet> Scorer for TermScorer<TDeleteSet> {
    fn score(&mut self) -> Score {
        let doc = self.postings.doc();
        let tf = match self.fieldnorm_reader_opt {
            Some(ref fieldnorm_reader) => {
                let field_norm = fieldnorm_reader.get(doc);
                (self.postings.term_freq() as f32 / field_norm as f32)
            }
            None => self.postings.term_freq() as f32,
        };
        self.idf * tf.sqrt()
    }
}
