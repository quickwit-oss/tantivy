mod phrase_query;
mod phrase_weight;
mod phrase_scorer;

pub use self::phrase_query::PhraseQuery;
pub use self::phrase_weight::PhraseWeight;
pub use self::phrase_scorer::PhraseScorer;


#[cfg(test)]
mod tests {
        
    use super::*;
    use query::Query;
    use core::Index;
    use schema::FieldValue;
    use schema::{Document, Term, SchemaBuilder, TEXT};
    use collector::tests::TestCollector;

    #[test]
    pub fn test_phrase_query() {
                
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {   // 0
                let doc = doc!(text_field=>"b b b d c g c");
                index_writer.add_document(doc).unwrap();
            }
            {   // 1
                let doc = doc!(text_field=>"a b b d c g c");
                index_writer.add_document(doc).unwrap();
            }
            {   // 2
                let doc = doc!(text_field=>"a b a b c");
                index_writer.add_document(doc).unwrap();
            }
            {   // 3
                let doc = doc!(text_field=>"c a b a d ga a");
                index_writer.add_document(doc).unwrap();
            }
            {   // 4
                let doc = doc!(text_field=>"a b c");
                index_writer.add_document(doc).unwrap();
            }
            assert!(index_writer.commit().is_ok());
        }

        let searcher = index.searcher();
        let test_query = |texts: Vec<&str>| {
            let mut test_collector = TestCollector::default();
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::from(terms);
            phrase_query.search(&*searcher, &mut test_collector).expect("search should succeed");
            test_collector.docs()
        };
        assert_eq!(test_query(vec!("a", "b", "c")), vec!(2, 4));
        assert_eq!(test_query(vec!("a", "b")), vec!(1, 2, 3, 4));
        assert_eq!(test_query(vec!("b", "b")), vec!(0, 1));
        assert_eq!(test_query(vec!("g", "ewrwer")), vec!());
        assert_eq!(test_query(vec!("g", "a")), vec!());
    }
    
}
