mod boolean_query;
mod boolean_scorer;
mod boolean_weight;
mod score_combiner;

pub use self::boolean_query::BooleanQuery;
pub use self::boolean_scorer::BooleanScorer;
pub use self::score_combiner::ScoreCombiner;



#[cfg(test)]
mod tests {
    
    use super::*;
    use postings::{DocSet, VecPostings};
    use query::Scorer;
    use query::OccurFilter;
    use query::term_query::TermScorer;
    use query::Occur;
    use query::Query;
    use query::TermQuery;
    use collector::tests::TestCollector;
    use Index;
    use schema::*;
    use fastfield::{U32FastFieldReader};

    fn abs_diff(left: f32, right: f32) -> f32 {
        (right - left).abs()
    }   


    #[test]
    pub fn test_boolean_query() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_from_tempdir(schema).unwrap();
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field => "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let doc = doc!(text_field => "a c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let doc = doc!(text_field => "b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let doc = doc!(text_field => "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            {
                let doc = doc!(text_field => "d");
                index_writer.add_document(doc).unwrap();
            }
            assert!(index_writer.commit().is_ok());
        }
        let make_term_query = |text: &str| {
            let term_query = TermQuery::from(Term::from_field_text(text_field, text));
            let query: Box<Query> = box term_query;
            query
        };


        let matching_docs = |boolean_query: &Query| {
            let searcher = index.searcher();
            let mut test_collector = TestCollector::default();
            searcher.search(boolean_query, &mut test_collector).unwrap();
            test_collector.docs()
        };
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Must, make_term_query("a")) ]);
            assert_eq!(matching_docs(&boolean_query), vec!(0, 1, 3));
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Should, make_term_query("a")) ]);
            assert_eq!(matching_docs(&boolean_query), vec!(0, 1, 3));
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Should, make_term_query("a")),  (Occur::Should, make_term_query("b"))]);
            assert_eq!(matching_docs(&boolean_query), vec!(0, 1, 2, 3));
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Must, make_term_query("a")),  (Occur::Should, make_term_query("b"))]);
            assert_eq!(matching_docs(&boolean_query), vec!(0, 1, 3));
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Must, make_term_query("a")), 
                                                        (Occur::Should, make_term_query("b")),
                                                        (Occur::MustNot, make_term_query("d")),
                                                        ]);
            assert_eq!(matching_docs(&boolean_query), vec!(0, 1));
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::MustNot, make_term_query("d")),]);
            // TODO optimize this use case : only MustNot subqueries... no need
            // to read any postings.
            assert_eq!(matching_docs(&boolean_query), Vec::new());
        }
    }

    #[test]
    pub fn test_boolean_scorer() {
        let occurs = vec!(Occur::Should, Occur::Should);
        let occur_filter = OccurFilter::new(&occurs);
       
        let left_fieldnorms = U32FastFieldReader::from(vec!(100,200,300));
        
        let left = VecPostings::from(vec!(1, 2, 3));
        let left_scorer = TermScorer {
            idf: 1f32,
            fieldnorm_reader: left_fieldnorms,
            postings: left,
        };
        
        let right_fieldnorms = U32FastFieldReader::from(vec!(15,25,35));
        let right = VecPostings::from(vec!(1, 3, 8));
        
        let right_scorer = TermScorer {
            idf: 4f32,
            fieldnorm_reader: right_fieldnorms,
            postings: right,
        };

        let mut boolean_scorer = BooleanScorer::new(vec!(left_scorer, right_scorer), occur_filter);
        assert_eq!(boolean_scorer.next(), Some(1u32));
        assert!(abs_diff(boolean_scorer.score(), 0.8707107) < 0.001);
        assert_eq!(boolean_scorer.next(), Some(2u32));
        assert!(abs_diff(boolean_scorer.score(), 0.028867513) < 0.001f32);
        assert_eq!(boolean_scorer.next(), Some(3u32));
        assert_eq!(boolean_scorer.next(), Some(8u32));
        assert!(abs_diff(boolean_scorer.score(), 0.5163978) < 0.001f32);
        assert!(!boolean_scorer.advance());
    }
    
    
    #[test]
    pub fn test_term_scorer() {
        let left_fieldnorms = U32FastFieldReader::from(vec!(10, 4));
        assert_eq!(left_fieldnorms.get(0), 10);
        assert_eq!(left_fieldnorms.get(1), 4);
        let left = VecPostings::from(vec!(1));
        let mut left_scorer = TermScorer {
            idf: 0.30685282,
            fieldnorm_reader: left_fieldnorms,
            postings: left,
        };
        left_scorer.advance();
        assert!(abs_diff(left_scorer.score(), 0.15342641) < 0.001f32);
    }

}
