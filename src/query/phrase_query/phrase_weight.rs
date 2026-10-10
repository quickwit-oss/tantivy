use super::PhraseScorer;
use crate::docset::SeekDangerResult;
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::{EmptyScorer, Explanation, Scorer, Weight};
use crate::schema::{IndexRecordOption, Term};
use crate::{DocId, DocSet, Score, TERMINATED};

pub struct PhraseWeight {
    phrase_terms: Vec<(usize, Term)>,
    similarity_weight_opt: Option<Bm25Weight>,
    slop: u32,
}

impl PhraseWeight {
    /// Creates a new phrase weight.
    /// If `similarity_weight_opt` is None, then scoring is disabled
    pub fn new(
        phrase_terms: Vec<(usize, Term)>,
        similarity_weight_opt: Option<Bm25Weight>,
    ) -> PhraseWeight {
        let slop = 0;
        PhraseWeight {
            phrase_terms,
            similarity_weight_opt,
            slop,
        }
    }

    fn fieldnorm_reader(&self, reader: &SegmentReader) -> crate::Result<FieldNormReader> {
        let field = self.phrase_terms[0].1.field();
        if self.similarity_weight_opt.is_some() {
            if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(field)? {
                return Ok(fieldnorm_reader);
            }
        }
        Ok(FieldNormReader::constant(reader.max_doc(), 1))
    }

    pub(crate) fn phrase_scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Option<PhraseScorer<SegmentPostings>>> {
        let Some((seek_result, mut scorer)) = self.phrase_scorer_danger(reader, 0, boost)? else {
            return Ok(None);
        };
        if let SeekDangerResult::SeekLowerBound(target) = seek_result {
            if target < TERMINATED {
                scorer.seek(target);
            }
        }
        Ok(Some(scorer))
    }

    fn phrase_scorer_danger(
        &self,
        reader: &SegmentReader,
        target: DocId,
        boost: Score,
    ) -> crate::Result<Option<(SeekDangerResult, PhraseScorer<SegmentPostings>)>> {
        let similarity_weight_opt = self
            .similarity_weight_opt
            .as_ref()
            .map(|similarity_weight| similarity_weight.boost_by(boost));
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let mut term_postings_list = Vec::new();
        for &(offset, ref term) in &self.phrase_terms {
            let Some(postings) = reader
                .inverted_index(term.field())?
                .read_postings(term, IndexRecordOption::WithFreqsAndPositions)?
            else {
                return Ok(None);
            };
            term_postings_list.push((offset, postings));
        }
        Ok(Some(PhraseScorer::new_danger(
            term_postings_list,
            similarity_weight_opt,
            fieldnorm_reader,
            self.slop,
            0,
            target,
        )))
    }

    pub fn slop(&mut self, slop: u32) {
        self.slop = slop;
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        if let Some(scorer) = self.phrase_scorer(reader, boost)? {
            Ok(Box::new(scorer))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn scorer_danger(
        &self,
        reader: &SegmentReader,
        target: DocId,
        boost: Score,
    ) -> crate::Result<(SeekDangerResult, Box<dyn Scorer>)> {
        let Some((seek_result, scorer)) = self.phrase_scorer_danger(reader, target, boost)? else {
            return Ok((
                SeekDangerResult::SeekLowerBound(TERMINATED),
                Box::new(EmptyScorer),
            ));
        };
        Ok((seek_result, Box::new(scorer)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let scorer_opt = self.phrase_scorer(reader, 1.0)?;
        if scorer_opt.is_none() {
            return Err(does_not_match(doc));
        }
        let mut scorer = scorer_opt.unwrap();
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
        let phrase_count = scorer.phrase_count();
        let mut explanation = Explanation::new("Phrase Scorer", scorer.score());
        if let Some(similarity_weight) = self.similarity_weight_opt.as_ref() {
            explanation.add_detail(similarity_weight.explain(fieldnorm_id, phrase_count));
        }
        Ok(explanation)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_index;
    use crate::docset::{SeekDangerResult, TERMINATED};
    use crate::query::{EnableScoring, PhraseQuery, Weight};
    use crate::{DocSet, Term};

    #[test]
    pub fn test_phrase_count() -> crate::Result<()> {
        let index = create_index(&["a c", "a a b d a b c", " a b"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhraseQuery::new(vec![
            Term::from_field_text(text_field, "a"),
            Term::from_field_text(text_field, "b"),
        ]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.phrase_weight(enable_scoring).unwrap();
        let mut phrase_scorer = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(phrase_scorer.doc(), 1);
        assert_eq!(phrase_scorer.phrase_count(), 2);
        assert_eq!(phrase_scorer.advance(), 2);
        assert_eq!(phrase_scorer.doc(), 2);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_phrase_weight_scorer_danger() -> crate::Result<()> {
        let index = create_index(&["a b", "a c b", "a b", "a c"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhraseQuery::new(vec![
            Term::from_field_text(text_field, "a"),
            Term::from_field_text(text_field, "b"),
        ]);
        let phrase_weight =
            phrase_query.phrase_weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let reader = searcher.segment_reader(0);

        let (seek_result, mut scorer) = phrase_weight.scorer_danger(reader, 1, 1.0)?;
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(2));
        assert_eq!(scorer.seek_danger(2), SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 2);

        let (seek_result, scorer) = phrase_weight.scorer_danger(reader, 2, 1.0)?;
        assert_eq!(seek_result, SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 2);

        let (seek_result, _) = phrase_weight.scorer_danger(reader, 3, 1.0)?;
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(TERMINATED));

        let scoring_phrase_weight =
            phrase_query.phrase_weight(EnableScoring::enabled_from_searcher(&searcher))?;
        let (seek_result, mut danger_scorer) =
            scoring_phrase_weight.scorer_danger(reader, 2, 2.0)?;
        assert_eq!(seek_result, SeekDangerResult::Found);
        let mut regular_scorer = scoring_phrase_weight.scorer(reader, 2.0)?;
        assert_eq!(regular_scorer.seek(2), 2);
        assert_eq!(danger_scorer.score(), regular_scorer.score());
        Ok(())
    }
}
