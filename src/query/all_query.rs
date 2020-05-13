use crate::core::Searcher;
use crate::core::SegmentReader;
use crate::docset::{DocSet, TERMINATED};
use crate::query::boost_query::BoostScorer;
use crate::query::explanation::does_not_match;
use crate::query::{Explanation, Query, Scorer, Weight};
use crate::DocId;
use crate::Score;

/// Query that matches all of the documents.
///
/// All of the document get the score 1f32.
#[derive(Clone, Debug)]
pub struct AllQuery;

impl Query for AllQuery {
    fn weight(&self, _: &Searcher, _: bool) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(AllWeight))
    }
}

/// Weight associated to the `AllQuery` query.
pub struct AllWeight;

impl Weight for AllWeight {
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>> {
        let all_scorer = AllScorer {
            doc: 0u32,
            max_doc: reader.max_doc(),
        };
        Ok(Box::new(BoostScorer::new(all_scorer, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        if doc >= reader.max_doc() {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("AllQuery", 1f32))
    }
}

/// Scorer associated to the `AllQuery` query.
pub struct AllScorer {
    doc: DocId,
    max_doc: DocId,
}

impl DocSet for AllScorer {
    fn advance(&mut self) -> bool {
        if self.doc + 1 >= self.max_doc {
            self.doc = TERMINATED;
            return false;
        }
        self.doc += 1;
        true
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

impl Scorer for AllScorer {
    fn score(&mut self) -> Score {
        1f32
    }
}

#[cfg(test)]
mod tests {
    use super::AllQuery;
    use crate::query::Query;
    use crate::schema::{Schema, TEXT};
    use crate::Index;

    fn create_test_index() -> Index {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
        index_writer.add_document(doc!(field=>"aaa"));
        index_writer.add_document(doc!(field=>"bbb"));
        index_writer.commit().unwrap();
        index_writer.add_document(doc!(field=>"ccc"));
        index_writer.commit().unwrap();
        index
    }

    #[test]
    fn test_all_query() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let weight = AllQuery.weight(&searcher, false).unwrap();
        {
            let reader = searcher.segment_reader(0);
            let mut scorer = weight.scorer(reader, 1.0f32).unwrap();
            assert_eq!(scorer.doc(), 0u32);
            assert!(scorer.advance());
            assert_eq!(scorer.doc(), 1u32);
            assert!(!scorer.advance());
        }
        {
            let reader = searcher.segment_reader(1);
            let mut scorer = weight.scorer(reader, 1.0f32).unwrap();
            assert_eq!(scorer.doc(), 0u32);
            assert!(!scorer.advance());
        }
    }

    #[test]
    fn test_all_query_with_boost() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let weight = AllQuery.weight(&searcher, false).unwrap();
        let reader = searcher.segment_reader(0);
        {
            let mut scorer = weight.scorer(reader, 2.0f32).unwrap();
            assert_eq!(scorer.doc(), 0u32);
            assert_eq!(scorer.score(), 2.0f32);
        }
        {
            let mut scorer = weight.scorer(reader, 1.5f32).unwrap();
            assert_eq!(scorer.doc(), 0u32);
            assert_eq!(scorer.score(), 1.5f32);
        }
    }
}
