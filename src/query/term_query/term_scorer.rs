use crate::docset::DocSet;
use crate::query::{Explanation, Scorer};
use crate::DocId;
use crate::Score;

use crate::fieldnorm::FieldNormReader;
use crate::postings::SegmentPostings;
use crate::postings::{FreqReadingOption, Postings};
use crate::query::bm25::BM25Weight;

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

    pub(crate) fn shallow_seek(&mut self, target_doc: DocId) {
        self.postings.block_cursor.shallow_seek(target_doc)
    }


    #[cfg(test)]
    pub fn create_for_test(
        doc_and_tfs: &[(DocId, u32)],
        fieldnorm_vals: &[u32],
        similarity_weight: BM25Weight,
    ) -> crate::Result<TermScorer> {
        assert!(!doc_and_tfs.is_empty());
        assert!(doc_and_tfs.len() <= fieldnorm_vals.len());

        let doc_freq = doc_and_tfs.len();
        let max_doc = doc_and_tfs.last().unwrap().0 + 1;
        let mut fieldnorms: Vec<u32> = std::iter::repeat(1).take(max_doc as usize).collect();

        for i in 0..doc_freq {
            let doc = doc_and_tfs[i].0;
            let fieldnorm = fieldnorm_vals[i];
            fieldnorms[doc as usize] = fieldnorm;
        }
        let fieldnorm_reader = FieldNormReader::from(&fieldnorms[..]);

        let segment_postings =
            SegmentPostings::create_from_docs_and_tfs(doc_and_tfs, Some(fieldnorm_reader.clone()))?;

        Ok(TermScorer::new(segment_postings, fieldnorm_reader, similarity_weight))
    }

    /// See `FreqReadingOption`.
    pub(crate) fn freq_reading_option(&self) -> FreqReadingOption {
        self.postings.block_cursor.freq_reading_option()
    }

    /// Returns the maximum score for the current block.
    ///
    /// In some rare case, the result may not be exact. In this case a lower value is returned,
    /// (and may lead us to return a lesser document).
    ///
    /// At index time, we store the (fieldnorm_id, term frequency) pair that maximizes the
    /// score assuming the average fieldnorm computed on this segment.
    ///
    /// Though extremely rare, it is theoretically possible that the actual average fieldnorm
    /// is different enough from the current segment average fieldnorm that the maximum over a
    /// specific is achieved on a different document.
    ///
    /// (The result is on the other hand guaranteed to be correct if there is only one segment).
    pub fn block_max_score(&mut self) -> Score {
        self.postings
            .block_cursor
            .block_max_score(&self.fieldnorm_reader, &self.similarity_weight)
    }

    pub fn term_freq(&self) -> u32 {
        self.postings.term_freq()
    }

    pub fn doc_freq(&self) -> usize {
        self.postings.doc_freq() as usize
    }

    pub fn fieldnorm_id(&self) -> u8 {
        self.fieldnorm_reader.fieldnorm_id(self.doc())
    }

    pub fn explain(&self) -> Explanation {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.explain(fieldnorm_id, term_freq)
    }

    pub fn max_score(&self) -> f32 {
        self.similarity_weight.max_score()
    }

    pub fn last_doc_in_block(&self) -> DocId {
        self.postings.block_cursor.skip_reader.last_doc_in_block()
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

#[cfg(test)]
mod tests {
    use crate::assert_nearly_equals;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
    use crate::query::term_query::TermScorer;
    use crate::query::{BM25Weight, Scorer};
    use crate::{DocId, DocSet, TERMINATED};
    use proptest::prelude::*;

    #[test]
    fn test_term_scorer_max_score() -> crate::Result<()> {
        let bm25_weight = BM25Weight::for_one_term(3, 6, 10f32);
        let mut term_scorer =
            TermScorer::create_for_test(&[(2, 3), (3, 12), (7, 8)], &[10, 12, 100], bm25_weight)?;
        let max_scorer = term_scorer.max_score();
        assert_eq!(max_scorer, 1.3990127f32);
        assert_eq!(term_scorer.doc(), 2);
        assert_eq!(term_scorer.term_freq(), 3);
        assert_nearly_equals!(term_scorer.block_max_score(), 1.3676447f32);
        assert_nearly_equals!(term_scorer.score(), 1.0892314f32);
        assert_eq!(term_scorer.advance(), 3);
        assert_eq!(term_scorer.doc(), 3);
        assert_eq!(term_scorer.term_freq(), 12);
        assert_nearly_equals!(term_scorer.score(), 1.3676447f32);
        assert_eq!(term_scorer.advance(), 7);
        assert_eq!(term_scorer.doc(), 7);
        assert_eq!(term_scorer.term_freq(), 8);
        assert_nearly_equals!(term_scorer.score(), 0.72015285f32);
        assert_eq!(term_scorer.advance(), TERMINATED);
        Ok(())
    }


    #[test]
    fn test_term_scorer_shallow_advance() -> crate::Result<()> {
        let bm25_weight = BM25Weight::for_one_term(300, 1024, 10f32);
        let mut doc_and_tfs = vec![];
        for i in 0u32..300u32 {
            let doc = i * 10;
            doc_and_tfs.push((doc, 1u32 + doc % 3u32));
        }
        let fieldnorms: Vec<u32> = std::iter::repeat(10u32).take(1024).collect();
        let mut term_scorer =
            TermScorer::create_for_test(&doc_and_tfs, &fieldnorms, bm25_weight)?;
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.shallow_seek(1289);
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.seek(1289);
        assert_eq!(term_scorer.doc(), 1290);
        Ok(())
    }

    proptest! {
        #[test]
        fn test_term_scorer_block_max_score(term_freqs_fieldnorms in proptest::collection::vec((1u32..10u32, 0u32..100u32), 80..300)) {
        let term_doc_freq = term_freqs_fieldnorms.len();
         let doc_tfs: Vec<(u32, u32)> = term_freqs_fieldnorms.iter()
                   .cloned()
                  .enumerate()
                  .map(|(doc, (tf, _))| (doc as u32, tf))
                  .collect();

         let mut fieldnorms: Vec<u32> = vec![];
         for i in 0..term_doc_freq {
             let (tf, num_extra_terms) = term_freqs_fieldnorms[i];
             fieldnorms.push(tf + num_extra_terms);
         }
         let average_fieldnorm = fieldnorms
             .iter()
             .cloned()
             .sum::<u32>() as f32 / term_doc_freq as f32;
             // Average fieldnorm is over the entire index,
             // not necessarily the docs that are in the posting list.
             // For this reason we multiply by 1.1 to make a realistic value.
         let bm25_weight = BM25Weight::for_one_term(term_doc_freq as u64,
            term_doc_freq as u64 * 10u64,
            average_fieldnorm);

         let mut term_scorer =
              TermScorer::create_for_test(&doc_tfs[..], &fieldnorms[..], bm25_weight).unwrap();

         let docs: Vec<DocId> = (0..term_doc_freq).map(|doc| doc as DocId).collect();
         for block in docs.chunks(COMPRESSION_BLOCK_SIZE) {
             let block_max_score = term_scorer.block_max_score();
             let mut block_max_score_computed = 0.0f32;
             for &doc in block {
                assert_eq!(term_scorer.doc(), doc);
                block_max_score_computed = block_max_score_computed.max(term_scorer.score());
                term_scorer.advance();
             }
             assert_nearly_equals!(block_max_score_computed, block_max_score);
         }
        }
    }
}
