use super::PhraseScorer;
use crate::core::SegmentReader;
use crate::fieldnorm::FieldNormReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::BM25Weight;
use crate::query::explanation::does_not_match;
use crate::query::Scorer;
use crate::query::Weight;
use crate::query::{EmptyScorer, Explanation};
use crate::schema::IndexRecordOption;
use crate::schema::Term;
use crate::{DocId, DocSet};
use crate::{Result, SkipResult};

pub struct PhraseWeight {
    phrase_terms: Vec<(usize, Term)>,
    similarity_weight: BM25Weight,
    score_needed: bool,
}

impl PhraseWeight {
    /// Creates a new phrase weight.
    pub fn new(
        phrase_terms: Vec<(usize, Term)>,
        similarity_weight: BM25Weight,
        score_needed: bool,
    ) -> PhraseWeight {
        PhraseWeight {
            phrase_terms,
            similarity_weight,
            score_needed,
        }
    }

    fn fieldnorm_reader(&self, reader: &SegmentReader) -> FieldNormReader {
        let field = self.phrase_terms[0].1.field();
        reader.get_fieldnorms_reader(field)
    }

    pub fn phrase_scorer(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<PhraseScorer<SegmentPostings>>> {
        let similarity_weight = self.similarity_weight.clone();
        let fieldnorm_reader = self.fieldnorm_reader(reader);
        if reader.has_deletes() {
            let mut term_postings_list = Vec::new();
            for &(offset, ref term) in &self.phrase_terms {
                if let Some(postings) = reader
                    .inverted_index(term.field())
                    .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
                {
                    term_postings_list.push((offset, postings));
                } else {
                    return Ok(None);
                }
            }
            Ok(Some(PhraseScorer::new(
                term_postings_list,
                similarity_weight,
                fieldnorm_reader,
                self.score_needed,
            )))
        } else {
            let mut term_postings_list = Vec::new();
            for &(offset, ref term) in &self.phrase_terms {
                if let Some(postings) = reader
                    .inverted_index(term.field())
                    .read_postings_no_deletes(&term, IndexRecordOption::WithFreqsAndPositions)
                {
                    term_postings_list.push((offset, postings));
                } else {
                    return Ok(None);
                }
            }
            Ok(Some(PhraseScorer::new(
                term_postings_list,
                similarity_weight,
                fieldnorm_reader,
                self.score_needed,
            )))
        }
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<dyn Scorer>> {
        if let Some(scorer) = self.phrase_scorer(reader)? {
            Ok(Box::new(scorer))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let scorer_opt = self.phrase_scorer(reader)?;
        if scorer_opt.is_none() {
            return Err(does_not_match(doc));
        }
        let mut scorer = scorer_opt.unwrap();
        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err(does_not_match(doc));
        }
        let mut fieldnorm_reader = self.fieldnorm_reader(reader);
        let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
        let phrase_count = scorer.phrase_count();
        let mut explanation = Explanation::new("Phrase Scorer", scorer.score());
        explanation.add_detail(self.similarity_weight.explain(fieldnorm_id, phrase_count));
        Ok(explanation)
    }
}
