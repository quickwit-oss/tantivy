use crate::docset::DocSet;
use crate::query::{Explanation, Scorer};
use crate::DocId;
use crate::Score;

use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::postings::SegmentPostings;
use crate::query::bm25::BM25Weight;

pub struct TermScorer {
    pub(crate) postings: SegmentPostings,
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

    pub fn doc_freq(&self,) -> usize {
        self.postings.doc_freq()
    }

    pub fn fieldnorm_id(&self) -> u8 {
        self.fieldnorm_reader.fieldnorm_id(self.doc())
    }

    pub fn explain(&self) -> Explanation {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.explain(fieldnorm_id, term_freq)
    }

    pub fn max_score(&self, ) -> f32 {
        unimplemented!();
    }
}

impl DocSet for TermScorer {
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

impl Scorer for TermScorer {
    fn score(&mut self) -> Score {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.score(fieldnorm_id, term_freq)
    }
}
