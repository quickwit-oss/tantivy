use crate::docset::{DocSet, SkipResult};
use crate::query::{Explanation, Scorer};
use crate::DocId;
use crate::Score;

use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::postings::{BlockMaxPostings, BlockMaxSegmentPostings};
use crate::query::bm25::BM25Weight;
use crate::query::BlockMaxScorer;

pub struct BlockMaxTermScorer {
    postings: BlockMaxSegmentPostings,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: BM25Weight,
}

impl BlockMaxTermScorer {
    pub fn new(
        postings: BlockMaxSegmentPostings,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: BM25Weight,
    ) -> Self {
        Self {
            postings,
            fieldnorm_reader,
            similarity_weight,
        }
    }
}

impl BlockMaxTermScorer {
    fn _score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        self.similarity_weight.score(fieldnorm_id, term_freq)
    }

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

impl DocSet for BlockMaxTermScorer {
    fn advance(&mut self) -> bool {
        self.postings.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.postings.skip_next(target)
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

impl Scorer for BlockMaxTermScorer {
    fn score(&mut self) -> Score {
        self._score(
            self.fieldnorm_reader.fieldnorm_id(self.doc()),
            self.postings.term_freq(),
        )
    }
}

impl BlockMaxScorer for BlockMaxTermScorer {
    fn block_max_score(&mut self) -> Score {
        self._score(
            self.fieldnorm_reader
                .fieldnorm_id(self.postings.block_max_doc()),
            self.postings.term_freq(),
        )
    }

    fn block_max_doc(&mut self) -> DocId {
        self.postings.block_max_doc()
    }

    fn max_score(&self) -> Score {
        self._score(
            self.fieldnorm_reader.fieldnorm_id(self.postings.max_doc()),
            self.postings.max_term_freq(),
        )
    }
}
