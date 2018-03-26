use Score;
use DocId;
use docset::{DocSet, SkipResult};
use query::Scorer;

use postings::Postings;
use fieldnorm::FieldNormReader;
use query::bm25::BM25Weight;

pub struct TermScorer<TPostings: Postings> {
    postings: TPostings,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: BM25Weight,
}


impl<TPostings: Postings> TermScorer<TPostings> {
    pub fn new(postings: TPostings,
               fieldnorm_reader: FieldNormReader,
               similarity_weight: BM25Weight) -> TermScorer<TPostings> {
        TermScorer {
            postings,
            fieldnorm_reader,
            similarity_weight,
        }
    }
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
        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);
        self.similarity_weight.score(fieldnorm_id, self.postings.term_freq())
    }
}

