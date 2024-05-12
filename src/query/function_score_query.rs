use crate::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError};
use std::fmt;
use std::sync::Arc;

type Function = Arc<dyn Fn(&SegmentReader, Score, DocId) -> Score + Send + Sync + 'static>;

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
///             Arc::new(move 
/// |_, score, _| score * MULTIPLIER + OFFSET),
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
    function: Function,
}

impl FunctionScoreQuery {
    /// Creates a new FunctionScoreQuery.
    pub fn new(
        query: Box<dyn Query>,
        function: Function,
    ) -> Self {
        FunctionScoreQuery { query, function }
    }
}

impl Clone for FunctionScoreQuery {
    fn clone(&self) -> Self {
        FunctionScoreQuery {
            query: self.query.box_clone(),
            function: self.function.clone(),
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
            self.function.clone(),
        )))
    }
}

struct FunctionScoreWeight {
    weight: Box<dyn Weight>,
    scoring_enabled: bool,
    function: Function,
}

impl FunctionScoreWeight {
    pub fn new(
        weight: Box<dyn Weight>,
        scoring_enabled: bool,
        function: Function,
    ) -> Self {
        FunctionScoreWeight {
            weight,
            scoring_enabled,
            function,
        }
    }
}

impl Weight for FunctionScoreWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inner_scorer = self.weight.scorer(reader, boost)?;
        if !self.scoring_enabled {
            Ok(Box::new(inner_scorer))
        } else {
            let new_inner_scorer = self.weight.scorer(reader, boost)?;
            Ok(Box::new(FunctionScorer::new(
                inner_scorer,
                new_inner_scorer,
                Arc::new(reader.clone()),
                self.function.clone(),
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

struct FunctionScorer<TDocSet: DocSet> {
    docset: TDocSet,
    scorer: Box<dyn Scorer>,
    segment_reader: Arc<SegmentReader>,
    function: Function,
}

impl<TDocSet: DocSet> FunctionScorer<TDocSet> {
    pub fn new(
        docset: TDocSet,
        scorer: Box<dyn Scorer>,
        segment_reader: Arc<SegmentReader>,
        function: Function,
    ) -> Self {
        FunctionScorer {
            docset,
            scorer,
            segment_reader,
            function,
        }
    }
}

impl<TDocSet: DocSet> DocSet for FunctionScorer<TDocSet> {
    fn advance(&mut self) -> DocId {
        self.docset.advance()
    }

    fn doc(&self) -> DocId {
        self.docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.docset.size_hint()
    }
}

impl<TDocSet: DocSet + 'static> Scorer for FunctionScorer<TDocSet> {
    fn score(&mut self) -> Score {
        let score = self.scorer.score();
        let doc_id = self.doc();
        (self.function)(self.segment_reader.as_ref(), score, doc_id)
    }
}

#[cfg(test)]
mod tests {
    use super::FunctionScoreQuery;
    use crate::query::{AllQuery, ConstScoreQuery, Query};
    use crate::schema::FAST;
    use crate::schema::{Schema, document::OwnedValue, TEXT, STORED};
    use crate::{DocAddress, Index, IndexWriter, TantivyDocument};
    use crate::collector::TopDocs;
    use crate::{SegmentReader, DocId};
    use std::sync::Arc;

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
            Arc::new(|_, score, _| score * 2.0),
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

    #[test]
    fn test_function_score_get_fast_field_from_segment_reader() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT | STORED | FAST); 
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                title => "The Name of the Wind",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Function that multiplies the score by the length of the title
        let title_length_scorer = Arc::new(|segment_reader: &SegmentReader, score: f32, doc_id: DocId| {
            let field_reader_opt = segment_reader.fast_fields().str("title").unwrap();
            if field_reader_opt.is_none() {
                return score;
            }
            let field_reader = field_reader_opt.unwrap();
            let mut bytes = Vec::new();
            let ord = field_reader.term_ords(doc_id).next().unwrap();
            field_reader.ord_to_bytes(ord, &mut bytes).unwrap();
            let title = String::from_utf8(bytes).unwrap();
            score * title.as_str().len() as f32
        });

        let query = FunctionScoreQuery::new(
            Box::new(ConstScoreQuery::new(Box::new(AllQuery), 0.42)),
            title_length_scorer
        );
        let results = searcher.search(&query, &TopDocs::with_limit(1))?;
        for (score, doc_address) in results {
            let doc = searcher.doc::<TantivyDocument>(doc_address)?;
            if let Some(OwnedValue::Str(title)) = doc.get_first(title) {
                assert_eq!(title, "The Name of the Wind");
                assert_eq!(score, "The Name of the Wind".len() as f32 * 0.42);
            } else {
                panic!("Title field not found or not a string");
            }
        }
        Ok(())
    }
}
