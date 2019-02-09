mod boolean_query;
mod boolean_weight;

pub use self::boolean_query::BooleanQuery;

#[cfg(test)]
mod tests {

    use super::*;
    use collector::tests::TestCollector;
    use query::score_combiner::SumWithCoordsCombiner;
    use query::term_query::TermScorer;
    use query::Intersection;
    use query::Occur;
    use query::Query;
    use query::QueryParser;
    use query::RequiredOptionalScorer;
    use query::Scorer;
    use query::TermQuery;
    use schema::*;
    use DocId;
    use Index;

    fn aux_test_helper() -> (Index, Field) {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
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
        let weight = query.weight(&searcher, true).unwrap();
        let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
        assert!(scorer.is::<TermScorer>());
    }

    #[test]
    pub fn test_boolean_termonly_intersection() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.searcher();
        {
            let query = query_parser.parse_query("+a +b +c").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(scorer.is::<Intersection<TermScorer>>());
        }
        {
            let query = query_parser.parse_query("+a +(b c)").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(scorer.is::<Intersection<Box<Scorer>>>());
        }
    }

    #[test]
    pub fn test_boolean_reqopt() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.searcher();
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(scorer
                .is::<RequiredOptionalScorer<Box<Scorer>, Box<Scorer>, SumWithCoordsCombiner>>());
        }
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&searcher, false).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
            assert!(scorer.is::<TermScorer>());
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
            let query: Box<Query> = Box::new(term_query);
            query
        };

        let matching_docs = |boolean_query: &Query| {
            let searcher = index.searcher();
            let test_docs = searcher.search(boolean_query, &TestCollector).unwrap();
            test_docs
                .docs()
                .iter()
                .cloned()
                .map(|doc| doc.1)
                .collect::<Vec<DocId>>()
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

    #[test]
    pub fn test_intersection_score() {
        let (index, text_field) = aux_test_helper();

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<Query> = Box::new(term_query);
            query
        };

        let score_docs = |boolean_query: &Query| {
            let searcher = index.searcher();
            let fruit = searcher.search(boolean_query, &TestCollector).unwrap();
            fruit.scores().to_vec()
        };

        {
            let boolean_query = BooleanQuery::from(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Must, make_term_query("b")),
            ]);
            assert_eq!(score_docs(&boolean_query), vec![0.977973, 0.84699446]);
        }
    }
}
