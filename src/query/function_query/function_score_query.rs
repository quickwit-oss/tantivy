use std::fmt;
use std::sync::Arc;

use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError};

type ScoreFunction = Arc<dyn Fn(Score) -> Score + Send + Sync + 'static>;

/// A FunctionScoreQuery modifies the score of
/// matched documents using a custom function.
/// ```rust
/// use tantivy::collector::TopDocs;
/// use tantivy::query::{FunctionScoreQuery, ConstScoreQuery, AllQuery};
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, IndexWriter};
/// use std::sync::Arc;
///
/// fn example() -> tantivy::Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer: IndexWriter = index.writer(15_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ))?;
///         index_writer.commit()?;
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///     let MULTIPLIER: f32 = 2.0;
///     let OFFSET: f32 = 1.0;
///
///     {
///         let query = FunctionScoreQuery::new(
///             Box::new(ConstScoreQuery::new(Box::new(AllQuery), 0.42)),
///             Arc::new(move |score| score * MULTIPLIER + OFFSET),
///         );
///         let documents = searcher.search(&query, &TopDocs::with_limit(2))?;
///         for (score, _) in documents {
///            assert_eq!(score, 0.42 * 2.0 + 1.0);
///         }
///     }
///
///     Ok(())
/// }
/// # assert!(example().is_ok());
/// ```
pub struct FunctionScoreQuery {
    query: Box<dyn Query>,
    score_function: ScoreFunction,
}

impl FunctionScoreQuery {
    /// Creates a new FunctionScoreQuery.
    pub fn new(query: Box<dyn Query>, score_function: ScoreFunction) -> Self {
        FunctionScoreQuery {
            query,
            score_function,
        }
    }
}

impl Clone for FunctionScoreQuery {
    fn clone(&self) -> Self {
        FunctionScoreQuery {
            query: self.query.box_clone(),
            score_function: self.score_function.clone(),
        }
    }
}

impl fmt::Debug for FunctionScoreQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FunctionScore(query={:?})", self.query)
    }
}

impl Query for FunctionScoreQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let inner_weight = self.query.weight(enable_scoring)?;
        Ok(Box::new(FunctionScoreWeight::new(
            inner_weight,
            enable_scoring.is_scoring_enabled(),
            self.score_function.clone(),
        )))
    }
}

struct FunctionScoreWeight {
    weight: Box<dyn Weight>,
    scoring_enabled: bool,
    score_function: ScoreFunction,
}

impl FunctionScoreWeight {
    pub fn new(
        weight: Box<dyn Weight>,
        scoring_enabled: bool,
        score_function: ScoreFunction,
    ) -> Self {
        FunctionScoreWeight {
            weight,
            scoring_enabled,
            score_function,
        }
    }
}

impl Weight for FunctionScoreWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inner_scorer = self.weight.scorer(reader, boost)?;
        if !self.scoring_enabled {
            Ok(Box::new(inner_scorer))
        } else {
            Ok(Box::new(FunctionScorer::new(
                inner_scorer,
                self.score_function.clone(),
            )))
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.doc() < doc && scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        if !self.scoring_enabled {
            return Ok(Explanation::new("FunctionScore with no scoring", 1.0));
        }
        let mut explanation = Explanation::new("FunctionScore", scorer.score());
        let underlying_explanation = self.weight.explain(reader, doc)?;
        explanation.add_detail(underlying_explanation);
        Ok(explanation)
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        self.weight.count(reader)
    }
}

struct FunctionScorer {
    scorer: Box<dyn Scorer>,
    score_function: ScoreFunction,
}

impl FunctionScorer {
    pub fn new(scorer: Box<dyn Scorer>, score_function: ScoreFunction) -> Self {
        FunctionScorer {
            scorer,
            score_function,
        }
    }
}

impl DocSet for FunctionScorer {
    fn advance(&mut self) -> DocId {
        self.scorer.advance()
    }

    fn seek(&mut self, doc: DocId) -> DocId {
        self.scorer.seek(doc)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        self.scorer.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        self.scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.scorer.size_hint()
    }
}

impl Scorer for FunctionScorer {
    fn score(&mut self) -> Score {
        let score = self.scorer.score();
        (self.score_function)(score)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::FunctionScoreQuery;
    use crate::query::{AllQuery, ConstScoreQuery, Query};
    use crate::schema::Schema;
    use crate::{DocAddress, Index, IndexWriter, TantivyDocument};

    #[test]
    fn test_function_score_query_explain() -> crate::Result<()> {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(TantivyDocument::new())?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = FunctionScoreQuery::new(
            Box::new(ConstScoreQuery::new(Box::new(AllQuery), 0.42)),
            Arc::new(|score| score * 2.0),
        );
        let explanation = query.explain(&searcher, DocAddress::new(0, 0u32)).unwrap();
        assert_eq!(
            explanation.to_pretty_json(),
            r#"{
  "value": 0.84,
  "description": "FunctionScore",
  "details": [
    {
      "value": 0.42,
      "description": "Const",
      "details": [
        {
          "value": 1.0,
          "description": "AllQuery"
        }
      ]
    }
  ]
}"#
        );
        Ok(())
    }
}
