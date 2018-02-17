mod boolean_query;
mod boolean_weight;

pub use self::boolean_query::BooleanQuery;

#[cfg(test)]
mod tests {

    use super::*;
    use query::Occur;
    use query::Query;
    use query::TermQuery;
    use collector::tests::TestCollector;
    use Index;
    use schema::*;
    use fastfield::U64FastFieldReader;
    use schema::IndexRecordOption;

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

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<Query> = box term_query;
            query
        };

        index.load_searchers().unwrap();

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
