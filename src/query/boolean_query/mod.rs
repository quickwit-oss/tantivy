mod boolean_query;
mod boolean_weight;

pub use self::boolean_query::BooleanQuery;

#[cfg(test)]
mod tests {

    use super::*;
    use query::Occur;
    use query::Query;
    use query::TermQuery;
    use query::Intersection;
    use query::Scorer;
    use collector::tests::TestCollector;
    use Index;
    use downcast::Downcast;
    use schema::*;
    use query::QueryParser;
    use query::RequiredOptionalScorer;
    use query::score_combiner::SumWithCoordsCombiner;
    use query::term_query::TermScorerNoDeletes;

    fn aux_test_helper() -> (Index, Field) {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_from_tempdir(schema).unwrap();
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field => "a b c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field => "a c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field => "b c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field => "a b c d");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field => "d");
                index_writer.add_document(doc);
            }
            assert!(index_writer.commit().is_ok());
        }
        index.load_searchers().unwrap();
        (index, text_field)
    }

    #[test]
    pub fn test_boolean_non_all_term_disjunction() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("(+a +b) d").unwrap();
        assert_eq!(query.count(&*index.searcher()).unwrap(), 3);
    }

    #[test]
    pub fn test_boolean_single_must_clause() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("+a").unwrap();
        let searcher = index.searcher();
        let weight = query.weight(&*searcher, true).unwrap();
        let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
        assert!(Downcast::<TermScorerNoDeletes>::is_type(&*scorer));
    }

    #[test]
    pub fn test_boolean_termonly_intersection() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.searcher();
        {
            let query = query_parser.parse_query("+a +b +c").unwrap();
            let weight = query.weight(&*searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(Downcast::<Intersection<TermScorerNoDeletes>>::is_type(&*scorer));
        }
        {
            let query = query_parser.parse_query("+a +(b c)").unwrap();
            let weight = query.weight(&*searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(Downcast::<Intersection<Box<Scorer>>>::is_type(&*scorer));
        }
    }

    #[test]
    pub fn test_boolean_reqopt() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.searcher();
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&*searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(Downcast::<RequiredOptionalScorer<Box<Scorer>, Box<Scorer>, SumWithCoordsCombiner>>::is_type(&*scorer));
        }
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&*searcher, false).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            println!("{:?}", scorer.type_name());
            assert!(Downcast::<TermScorerNoDeletes>::is_type(&*scorer));
        }
    }

    #[test]
    pub fn test_boolean_query() {

        let (index, text_field) = aux_test_helper();

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
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
            let boolean_query = BooleanQuery::from(vec![(Occur::Must, make_term_query("a"))]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::Should, make_term_query("a"))]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Should, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 2, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
                (Occur::MustNot, make_term_query("d")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1]);
        }
        {
            let boolean_query = BooleanQuery::from(vec![(Occur::MustNot, make_term_query("d"))]);
            assert_eq!(matching_docs(&boolean_query), Vec::<u32>::new());
        }
    }
}
