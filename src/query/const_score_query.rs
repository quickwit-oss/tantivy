use std::fmt;

use crate::docset::{SeekDangerResult, COLLECT_BLOCK_BUFFER_LEN};
use crate::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError, Term};

/// `ConstScoreQuery` is a wrapper over a query to provide a constant score.
/// The wrapped query is evaluated with scoring disabled and only determines which documents
/// match, avoiding unnecessary score computation.
///
/// The document set matched by the `ConstScoreQuery` is strictly the same as the underlying query.
/// The configured score is used for each document. Score explanations only report this constant
/// score and omit scoring details from the wrapped query.
pub struct ConstScoreQuery {
    query: Box<dyn Query>,
    score: Score,
}

impl ConstScoreQuery {
    /// Builds a const score query.
    pub fn new(query: Box<dyn Query>, score: Score) -> ConstScoreQuery {
        ConstScoreQuery { query, score }
    }
}

impl Clone for ConstScoreQuery {
    fn clone(&self) -> Self {
        ConstScoreQuery {
            query: self.query.box_clone(),
            score: self.score,
        }
    }
}

impl fmt::Debug for ConstScoreQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Const(score={}, query={:?})", self.score, self.query)
    }
}

impl Query for ConstScoreQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let scoring_enabled = enable_scoring.is_scoring_enabled();
        let inner_weight = self.query.weight(enable_scoring.scoring_disabled())?;
        Ok(if scoring_enabled {
            Box::new(ConstWeight::new(inner_weight, self.score))
        } else {
            inner_weight
        })
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.query.query_terms(visitor);
    }
}

struct ConstWeight {
    weight: Box<dyn Weight>,
    score: Score,
}

impl ConstWeight {
    pub fn new(weight: Box<dyn Weight>, score: Score) -> Self {
        ConstWeight { weight, score }
    }
}

impl Weight for ConstWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inner_scorer = self.weight.scorer(reader, boost)?;
        Ok(Box::new(ConstScorer::new(inner_scorer, boost * self.score)))
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.doc() > doc || scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        // The child query only determines whether the document matches. Its score does not
        // contribute to the constant score and is intentionally omitted from the explanation.
        Ok(Explanation::new("Const", self.score))
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        self.weight.count(reader)
    }
}

/// Wraps a `DocSet` and simply returns a constant `Scorer`.
/// The `ConstScorer` is useful if you have a `DocSet` where
/// you needed a scorer.
///
/// The `ConstScorer`'s constant score can be set
/// by calling `.set_score(...)`.
pub struct ConstScorer<TDocSet: DocSet> {
    docset: TDocSet,
    score: Score,
}

impl<TDocSet: DocSet> ConstScorer<TDocSet> {
    /// Creates a new `ConstScorer`.
    pub fn new(docset: TDocSet, score: Score) -> ConstScorer<TDocSet> {
        ConstScorer { docset, score }
    }
}

impl<TDocSet: DocSet> From<TDocSet> for ConstScorer<TDocSet> {
    fn from(docset: TDocSet) -> Self {
        ConstScorer::new(docset, 1.0)
    }
}

impl<TDocSet: DocSet> DocSet for ConstScorer<TDocSet> {
    fn advance(&mut self) -> DocId {
        self.docset.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.docset.seek(target)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        self.docset.seek_danger(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        self.docset.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        self.docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.docset.size_hint()
    }

    fn cost(&self) -> u64 {
        self.docset.cost()
    }
}

impl<TDocSet: DocSet + 'static> Scorer for ConstScorer<TDocSet> {
    #[inline]
    fn score(&mut self) -> Score {
        self.score
    }
}

#[cfg(test)]
mod tests {
    use super::ConstScoreQuery;
    use crate::collector::TopDocs;
    use crate::query::{
        Bm25StatisticsProvider, BoostQuery, EnableScoring, Query, Scorer, TermQuery,
    };
    use crate::schema::{Field, IndexRecordOption, Schema, FAST, TEXT};
    use crate::{DocAddress, Index, IndexWriter, TantivyDocument, TantivyError, Term, TERMINATED};

    struct PanickingStatistics;

    impl Bm25StatisticsProvider for PanickingStatistics {
        fn total_num_tokens(&self, _field: Field) -> crate::Result<u64> {
            panic!("ConstScoreQuery child requested total_num_tokens")
        }

        fn total_num_docs(&self) -> crate::Result<u64> {
            panic!("ConstScoreQuery child requested total_num_docs")
        }

        fn doc_freq(&self, _term: &Term) -> crate::Result<u64> {
            panic!("ConstScoreQuery child requested doc_freq")
        }
    }

    #[test]
    fn test_const_score_query_disables_child_scoring() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        let mut document = TantivyDocument::new();
        document.add_text(text_field, "rust");
        index_writer.add_document(document)?;
        let mut non_matching_document = TantivyDocument::new();
        non_matching_document.add_text(text_field, "search");
        index_writer.add_document(non_matching_document)?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = ConstScoreQuery::new(
            Box::new(TermQuery::new(
                Term::from_field_text(text_field, "rust"),
                IndexRecordOption::WithFreqs,
            )),
            0.42,
        );

        let weight = query.weight(EnableScoring::enabled_from_statistics_provider(
            &PanickingStatistics,
            &searcher,
        ))?;
        let mut scorer = weight.scorer(searcher.segment_reader(0), 1.0)?;
        assert_eq!(scorer.doc(), 0);
        assert_eq!(scorer.score(), 0.42);
        assert_eq!(scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_const_score_query_supports_term_query_on_fast_only_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let number_field = schema_builder.add_u64_field("number", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(number_field => 42u64))?;
        index_writer.add_document(doc!(number_field => 7u64))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = ConstScoreQuery::new(
            Box::new(TermQuery::new(
                Term::from_field_u64(number_field, 42u64),
                IndexRecordOption::Basic,
            )),
            1.5,
        );

        let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(top_docs, vec![(1.5, DocAddress::new(0, 0))]);
        Ok(())
    }

    #[test]
    fn test_const_score_query_explain_omits_child_scoring_details() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text_field => "rust rust rust other"))?;
        index_writer.add_document(doc!(text_field => "rust search"))?;
        index_writer.add_document(doc!(text_field => "search only"))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_text(text_field, "rust");
        let query = ConstScoreQuery::new(
            Box::new(TermQuery::new(term.clone(), IndexRecordOption::WithFreqs)),
            0.42,
        );

        let explanation = query.explain(&searcher, DocAddress::new(0, 0))?;
        assert_eq!(
            explanation.to_pretty_json(),
            r#"{
  "value": 0.42,
  "description": "Const"
}"#
        );

        let nested_query = BoostQuery::new(
            Box::new(ConstScoreQuery::new(
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)),
                0.42,
            )),
            2.0,
        );
        let nested_explanation = nested_query.explain(&searcher, DocAddress::new(0, 0))?;
        assert_eq!(
            nested_explanation.to_pretty_json(),
            r#"{
  "value": 0.84,
  "description": "Boost x2 of ...",
  "details": [
    {
      "value": 0.42,
      "description": "Const"
    }
  ]
}"#
        );
        Ok(())
    }

    #[test]
    fn test_const_score_query_explain_before_first_match_returns_error() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text_field => "alpha"))?;
        index_writer.add_document(doc!(text_field => "beta"))?;
        index_writer.add_document(doc!(text_field => "beta"))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = ConstScoreQuery::new(
            Box::new(TermQuery::new(
                Term::from_field_text(text_field, "beta"),
                IndexRecordOption::Basic,
            )),
            0.42,
        );

        let error = query.explain(&searcher, DocAddress::new(0, 0)).unwrap_err();
        assert!(matches!(
            error,
            TantivyError::InvalidArgument(message)
                if message == "Document #(0) does not match"
        ));
        Ok(())
    }
}
