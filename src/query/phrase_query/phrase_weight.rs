use super::PhraseScorer;
use crate::docset::SeekDangerResult;
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::weight::for_each_pruning_scorer;
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

    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let Some(mut scorer) = self.phrase_scorer(reader, 1.0)? else {
            return Ok(());
        };
        if self.slop == 0 && self.similarity_weight_opt.is_some() {
            scorer.for_each_pruning_exact(threshold, callback);
        } else {
            for_each_pruning_scorer(&mut scorer, threshold, callback);
        }
        Ok(())
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
    use crate::query::weight::for_each_pruning_scorer;
    use crate::query::{EnableScoring, PhraseQuery, Weight};
    use crate::schema::{Schema, TEXT};
    use crate::{DocSet, Index, Score, Term};

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

    #[test]
    fn test_phrase_pruning_matches_exhaustive_scoring() -> crate::Result<()> {
        let index = create_index(&[
            "a b a b a b",
            "a b",
            "a a a b b b",
            "a b c a b",
            "a c b",
            "b a b a b",
            "a a a a",
            "a b a b",
            "b b b b",
            "a b b a b",
        ])?;
        let field = index.schema().get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let reader = searcher.segment_reader(0);

        for (terms, slop) in [
            (&["a", "b"][..], 0),
            (&["a", "b", "a"][..], 0),
            (&["a", "a"][..], 0),
            (&["a", "b"][..], 1),
        ] {
            let phrase_terms = terms
                .iter()
                .map(|term| Term::from_field_text(field, term))
                .collect();
            let mut query = PhraseQuery::new(phrase_terms);
            query.set_slop(slop);
            let weight = query.phrase_weight(EnableScoring::enabled_from_searcher(&searcher))?;

            for top_k in [1, 3, 10] {
                for initial_threshold in [Score::MIN, 0.0, 1.0] {
                    let threshold_for_hits = |hits: &[(u32, Score)]| {
                        if hits.len() < top_k {
                            return initial_threshold;
                        }
                        let mut scores: Vec<_> = hits.iter().map(|(_, score)| *score).collect();
                        scores.sort_by(|a, b| b.total_cmp(a));
                        initial_threshold.max(scores[top_k - 1])
                    };

                    let mut exhaustive = Vec::new();
                    let mut scorer = weight.scorer(reader, 1.0)?;
                    for_each_pruning_scorer(
                        scorer.as_mut(),
                        initial_threshold,
                        &mut |doc, score| {
                            exhaustive.push((doc, score));
                            threshold_for_hits(&exhaustive)
                        },
                    );

                    let mut pruned = Vec::new();
                    weight.for_each_pruning(initial_threshold, reader, &mut |doc, score| {
                        pruned.push((doc, score));
                        threshold_for_hits(&pruned)
                    })?;
                    assert_eq!(
                        pruned, exhaustive,
                        "terms={terms:?}, slop={slop}, k={top_k}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_phrase_pruning_skips_full_blocks_with_global_statistics() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        for _ in 0..256 {
            writer.add_document(doc!(field => "a b"))?;
        }
        writer.add_document(doc!(field => "a b ".repeat(20)))?;
        writer.commit()?;
        // This segment changes the global average field length after the first
        // segment's block impact was serialized.
        for _ in 0..256 {
            writer.add_document(doc!(field => "c ".repeat(100)))?;
        }
        writer.commit()?;

        let searcher = index.reader()?.searcher();
        let weight = PhraseQuery::new(vec![
            Term::from_field_text(field, "a"),
            Term::from_field_text(field, "b"),
        ])
        .phrase_weight(EnableScoring::enabled_from_searcher(&searcher))?;
        let reader = searcher
            .segment_readers()
            .iter()
            .find(|reader| reader.max_doc() == 257)
            .unwrap();
        let threshold = weight.scorer(reader, 1.0)?.score() + 0.01;

        let mut exhaustive = Vec::new();
        let mut scorer = weight.scorer(reader, 1.0)?;
        for_each_pruning_scorer(scorer.as_mut(), threshold, &mut |doc, score| {
            exhaustive.push((doc, score));
            threshold
        });
        let mut pruned = Vec::new();
        weight.for_each_pruning(threshold, reader, &mut |doc, score| {
            pruned.push((doc, score));
            threshold
        })?;
        assert_eq!(exhaustive.len(), 1);
        assert_eq!(exhaustive[0].0, 256);
        assert_eq!(pruned, exhaustive);
        Ok(())
    }
}
