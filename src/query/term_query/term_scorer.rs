use docset::{DocSet, SkipResult};
use query::{Explanation, Scorer};
use DocId;
use Score;

use fieldnorm::FieldNormReader;
use postings::Postings;
use postings::SegmentPostings;
use query::bm25::BM25Weight;

pub struct TermScorer {
    postings: SegmentPostings,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: BM25Weight,
}

impl TermScorer {
    pub fn new(
        postings: SegmentPostings,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: BM25Weight,
    ) -> TermScorer {
        TermScorer {
            postings,
            fieldnorm_reader,
            similarity_weight,
        }
    }
}

impl TermScorer {
    pub fn term_freq(&self) -> u32 {
        self.postings.term_freq()
    }

    pub fn fieldnorm_id(&self) -> u8 {
        self.fieldnorm_reader.fieldnorm_id(self.doc())
    }

    pub fn explain(&self) -> Explanation {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.explain(fieldnorm_id, term_freq)
    }
}

impl DocSet for TermScorer {
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

impl Scorer for TermScorer {
    fn score(&mut self) -> Score {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.score(fieldnorm_id, term_freq)
    }
}
