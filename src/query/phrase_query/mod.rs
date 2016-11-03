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
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b b d c g c");
                index_writer.add_document(doc).unwrap();
            }
            // {
            //     let mut doc = Document::default();
            //     doc.add_text(text_field, "a b a b c");
            //     index_writer.add_document(doc).unwrap();
            // }
            // {
            //     let mut doc = Document::default();
            //     doc.add_text(text_field, "c a b a d ga a");
            //     index_writer.add_document(doc).unwrap();
            // }
            // {
            //     let mut doc = Document::default();
            //     doc.add_text(text_field, "a b c");
            //     index_writer.add_document(doc).unwrap();
            // }
            assert!(index_writer.commit().is_ok());
        }
        let mut test_collector = TestCollector::default();
        let build_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| {
                    Term::from_field_text(text_field, text)
                })
                .collect();
            PhraseQuery::from(terms)
        };
        let phrase_query = build_query(vec!("a", "b"));
        let searcher = index.searcher();
        phrase_query.search(&*searcher, &mut test_collector).expect("search should succeed");
        assert_eq!(test_collector.docs(), vec!(0, 1, 2, 3));
    }
    
    
}
