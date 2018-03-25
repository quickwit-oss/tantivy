mod phrase_query;
mod phrase_weight;
mod phrase_scorer;

pub use self::phrase_query::PhraseQuery;
pub use self::phrase_weight::PhraseWeight;
pub use self::phrase_scorer::PhraseScorer;

#[cfg(test)]
mod tests {

    use super::*;
    use core::Index;
    use schema::{SchemaBuilder, Term, TEXT};
    use collector::tests::TestCollector;
    use tests::assert_nearly_equals;


    fn create_index(texts: &[&'static str]) -> Index {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            for &text in texts {
                let doc = doc!(text_field=>text);
                index_writer.add_document(doc);
            }
            assert!(index_writer.commit().is_ok());
        }
        index.load_searchers().unwrap();
        index
    }

    #[test]
    pub fn test_phrase_query() {
        let index = create_index(&[
            "b b b d c g c",
            "a b b d c g c",
            "a b a b c",
            "c a b a d ga a",
            "a b c"
        ]);
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let test_query = |texts: Vec<&str>| {
            let mut test_collector = TestCollector::default();
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            searcher
                .search(&phrase_query, &mut test_collector)
                .expect("search should succeed");
            test_collector.docs()
        };
        assert_eq!(test_query(vec!["a", "b", "c"]), vec![2, 4]);
        assert_eq!(test_query(vec!["a", "b"]), vec![1, 2, 3, 4]);
        assert_eq!(test_query(vec!["b", "b"]), vec![0, 1]);
        assert!(test_query(vec!["g", "ewrwer"]).is_empty());
        assert!(test_query(vec!["g", "a"]).is_empty());
    }


    #[test]
    pub fn test_phrase_score() {
        let index = create_index(&["a b c", "a b c a b"]);
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let test_query = |texts: Vec<&str>| {
            let mut test_collector = TestCollector::default();
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            searcher
                .search(&phrase_query, &mut test_collector)
                .expect("search should succeed");
            test_collector.scores()
        };
        let scores = test_query(vec!["a", "b"]);
        assert_nearly_equals(scores[0], 0.40618482);
        assert_nearly_equals(scores[1], 0.46844664);

    }

    #[test] // motivated by #234
    pub fn test_phrase_query_docfreq_order() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                // 0
                let doc = doc!(text_field=>"b");
                index_writer.add_document(doc);
            }
            {
                // 1
                let doc = doc!(text_field=>"a b");
                index_writer.add_document(doc);
            }
            {
                // 2
                let doc = doc!(text_field=>"b a");
                index_writer.add_document(doc);
            }
            assert!(index_writer.commit().is_ok());
        }

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let test_query = |texts: Vec<&str>| {
            let mut test_collector = TestCollector::default();
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            searcher
                .search(&phrase_query, &mut test_collector)
                .expect("search should succeed");
            test_collector.docs()
        };
        assert_eq!(test_query(vec!["a", "b"]), vec![1]);
        assert_eq!(test_query(vec!["b", "a"]), vec![2]);
    }
}
