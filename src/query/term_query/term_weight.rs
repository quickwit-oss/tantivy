use super::term_scorer::TermScorer;
use crate::core::SegmentReader;
use crate::docset::DocSet;
use crate::postings::SegmentPostings;
use crate::query::bm25::BM25Weight;
use crate::query::explanation::does_not_match;
use crate::query::Weight;
use crate::query::{Explanation, Scorer};
use crate::schema::IndexRecordOption;
use crate::DocId;
use crate::Term;
use crate::{Result, SkipResult};

pub struct TermWeight {
    term: Term,
    index_record_option: IndexRecordOption,
    similarity_weight: BM25Weight,
}

impl Weight for TermWeight {
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> Result<Box<dyn Scorer>> {
        let term_scorer = self.scorer_specialized(reader, boost)?;
        Ok(Box::new(term_scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let mut scorer = self.scorer_specialized(reader, 1.0f32)?;
        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err(does_not_match(doc));
        }
        Ok(scorer.explain())
    }

    fn count(&self, reader: &SegmentReader) -> Result<u32> {
        if let Some(delete_bitset) = reader.delete_bitset() {
            Ok(self.scorer(reader, 1.0f32)?.count(delete_bitset))
        } else {
            let field = self.term.field();
            Ok(reader
                .inverted_index(field)
                .get_term_info(&self.term)
                .map(|term_info| term_info.doc_freq)
                .unwrap_or(0))
        }
    }
}

impl TermWeight {
    pub fn new(
        term: Term,
        index_record_option: IndexRecordOption,
        similarity_weight: BM25Weight,
    ) -> TermWeight {
        TermWeight {
            term,
            index_record_option,
            similarity_weight,
        }
    }

    fn scorer_specialized(&self, reader: &SegmentReader, boost: f32) -> Result<TermScorer> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field);
        let fieldnorm_reader = reader.get_fieldnorms_reader(field);
        let similarity_weight = self.similarity_weight.boost_by(boost);
        let postings_opt: Option<SegmentPostings> =
            inverted_index.read_postings(&self.term, self.index_record_option);
        if let Some(segment_postings) = postings_opt {
            Ok(TermScorer::new(
                segment_postings,
                fieldnorm_reader,
                similarity_weight,
            ))
        } else {
            Ok(TermScorer::new(
                SegmentPostings::empty(),
                fieldnorm_reader,
                similarity_weight,
            ))
        }
    }
}
