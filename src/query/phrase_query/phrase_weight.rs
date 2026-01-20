use super::PhraseScorer;
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::TermInfo;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::{box_scorer, EmptyScorer, Explanation, Scorer, Weight};
use crate::schema::{IndexRecordOption, Term};
use crate::{DocId, DocSet, Score};

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
        PhraseWeight {
            phrase_terms,
            similarity_weight_opt,
            slop: 0,
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
    ) -> crate::Result<Option<Box<dyn Scorer>>> {
        let similarity_weight_opt = self
            .similarity_weight_opt
            .as_ref()
            .map(|similarity_weight| similarity_weight.boost_by(boost));
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;

        if self.phrase_terms.is_empty() {
            return Ok(None);
        }
        let field = self.phrase_terms[0].1.field();

        if !self
            .phrase_terms
            .iter()
            .all(|(_offset, term)| term.field() == field)
        {
            return Err(crate::TantivyError::InvalidArgument(
                "All terms in a phrase query must belong to the same field".to_string(),
            ));
        }

        let inverted_index_reader = reader.inverted_index(field)?;

        let mut term_infos: Vec<(usize, TermInfo)> = Vec::with_capacity(self.phrase_terms.len());

        // TODO make it specialized
        for &(offset, ref term) in &self.phrase_terms {
            let Some(term_info) = inverted_index_reader.get_term_info(&term)? else {
                return Ok(None);
            };
            term_infos.push((offset, term_info));
        }

        let scorer = reader.codec.new_phrase_scorer_type_erased(
            &term_infos[..],
            similarity_weight_opt,
            fieldnorm_reader,
            self.slop,
            &*inverted_index_reader,
        )?;

        Ok(Some(scorer))
    }

    pub fn set_slop(&mut self, slop: u32) {
        self.slop = slop;
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        if let Some(scorer) = self.phrase_scorer(reader, boost)? {
            Ok(scorer)
        } else {
            Ok(box_scorer(EmptyScorer))
        }
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
        Ok(scorer.explain())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_index;
    use crate::docset::TERMINATED;
    use crate::query::phrase_query::PhraseScorer;
    use crate::query::{EnableScoring, PhraseQuery, Scorer};
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
        let phrase_scorer_boxed: Box<dyn Scorer> = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        let mut phrase_scorer: Box<PhraseScorer> =
            phrase_scorer_boxed.downcast::<PhraseScorer>().ok().unwrap();
        assert_eq!(phrase_scorer.doc(), 1);
        assert_eq!(phrase_scorer.phrase_count(), 2);
        assert_eq!(phrase_scorer.advance(), 2);
        assert_eq!(phrase_scorer.doc(), 2);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }
}
