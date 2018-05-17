use core::Searcher;
use core::SegmentReader;
use docset::DocSet;
use query::{Query, Scorer, Weight};
use DocId;
use Result;
use Score;

/// Query that matches all of the documents.
///
/// All of the document get the score 1f32.
#[derive(Clone, Debug)]
pub struct AllQuery;

impl Query for AllQuery {
    fn weight(&self, _: &Searcher, _: bool) -> Result<Box<Weight>> {
        Ok(Box::new(AllWeight))
    }
}

/// Weight associated to the `AllQuery` query.
pub struct AllWeight;

impl Weight for AllWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        Ok(Box::new(AllScorer {
            state: State::NotStarted,
            doc: 0u32,
            max_doc: reader.max_doc(),
        }))
    }
}

enum State {
    NotStarted,
    Started,
    Finished,
}

/// Scorer associated to the `AllQuery` query.
pub struct AllScorer {
    state: State,
    doc: DocId,
    max_doc: DocId,
}

impl DocSet for AllScorer {
    fn advance(&mut self) -> bool {
        match self.state {
            State::NotStarted => {
                self.state = State::Started;
                self.doc = 0;
            }
            State::Started => {
                self.doc += 1u32;
            }
            State::Finished => {
                return false;
            }
        }
        if self.doc < self.max_doc {
            return true;
        } else {
            self.state = State::Finished;
            return false;
        }
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
    use query::Query;
    use schema::{SchemaBuilder, TEXT};
    use Index;

    #[test]
    fn test_all_query() {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
        index_writer.add_document(doc!(field=>"aaa"));
        index_writer.add_document(doc!(field=>"bbb"));
        index_writer.commit().unwrap();
        index_writer.add_document(doc!(field=>"ccc"));
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let weight = AllQuery.weight(&searcher, false).unwrap();
        {
            let reader = searcher.segment_reader(0);
            let mut scorer = weight.scorer(reader).unwrap();
            assert!(scorer.advance());
            assert_eq!(scorer.doc(), 0u32);
            assert!(scorer.advance());
            assert_eq!(scorer.doc(), 1u32);
            assert!(!scorer.advance());
        }
        {
            let reader = searcher.segment_reader(1);
            let mut scorer = weight.scorer(reader).unwrap();
            assert!(scorer.advance());
            assert_eq!(scorer.doc(), 0u32);
            assert!(!scorer.advance());
        }
    }

}
