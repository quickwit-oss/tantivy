mod term_query;
mod term_weight;
mod term_scorer;

pub use self::term_query::TermQuery;
pub use self::term_weight::TermWeight;
pub use self::term_scorer::TermScorer;


#[cfg(test)]
mod tests {
    
    use postings::{DocSet, VecPostings};
    use query::Scorer;
    use query::term_query::TermScorer;
    use query::Query;
    use fastfield::U64FastFieldReader;
    use query::TermQuery;
    use Index;
    use schema::*;
    use postings::SegmentPostingsOption;
    use fastfield::FastFieldReader;

    fn abs_diff(left: f32, right: f32) -> f32 {
        (right - left).abs()
    }   


    #[test]
    pub fn test_term_query_no_freq() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let index = Index::create_from_tempdir(schema).unwrap();
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field => "a");
                index_writer.add_document(doc);
            }
            assert!(index_writer.commit().is_ok());
        }

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let term_query = TermQuery::new(Term::from_field_text(text_field, "a"), SegmentPostingsOption::NoFreq);
        let term_weight = term_query.weight(&searcher).unwrap();
        let segment_reader = searcher.segment_reader(0);
        let mut term_scorer = term_weight.scorer(segment_reader).unwrap();
        assert!(term_scorer.advance());
        assert_eq!(term_scorer.doc(), 0);
        assert_eq!(term_scorer.score(), 0.30685282);
    }

       
    #[test]
    pub fn test_term_scorer() {
        let left_fieldnorms = U64FastFieldReader::from(vec!(10, 4));
        assert_eq!(left_fieldnorms.get(0), 10);
        assert_eq!(left_fieldnorms.get(1), 4);
        let left = VecPostings::from(vec!(1));
        let mut left_scorer = TermScorer {
            idf: 0.30685282,
            fieldnorm_reader_opt: Some(left_fieldnorms),
            postings: left,
        };
        left_scorer.advance();
        assert!(abs_diff(left_scorer.score(), 0.15342641) < 0.001f32);
    }

}