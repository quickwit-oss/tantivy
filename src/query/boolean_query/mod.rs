mod block_wand;
mod boolean_query;
mod boolean_weight;

pub(crate) use self::block_wand::block_wand;
pub use self::boolean_query::BooleanQuery;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::assert_nearly_equals;
    use crate::collector::tests::TEST_COLLECTOR_WITH_SCORE;
    use crate::collector::TopDocs;
    use crate::query::score_combiner::SumWithCoordsCombiner;
    use crate::query::term_query::TermScorer;
    use crate::query::Intersection;
    use crate::query::Occur;
    use crate::query::Query;
    use crate::query::QueryParser;
    use crate::query::RequiredOptionalScorer;
    use crate::query::Scorer;
    use crate::query::TermQuery;
    use crate::schema::*;
    use crate::Index;
    use crate::{DocAddress, DocId, Score};

    fn aux_test_helper() -> (Index, Field) {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.add_document(doc!(text_field => "a b c"));
            index_writer.add_document(doc!(text_field => "a c"));
            index_writer.add_document(doc!(text_field => "b c"));
            index_writer.add_document(doc!(text_field => "a b c d"));
            index_writer.add_document(doc!(text_field => "d"));
            assert!(index_writer.commit().is_ok());
        }
        (index, text_field)
    }

    #[test]
    pub fn test_boolean_non_all_term_disjunction() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("(+a +b) d").unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(query.count(&searcher).unwrap(), 3);
    }

    #[test]
    pub fn test_boolean_single_must_clause() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("+a").unwrap();
        let searcher = index.reader().unwrap().searcher();
        let weight = query.weight(&searcher, true).unwrap();
        let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0).unwrap();
        assert!(scorer.is::<TermScorer>());
    }

    #[test]
    pub fn test_boolean_termonly_intersection() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.reader().unwrap().searcher();
        {
            let query = query_parser.parse_query("+a +b +c").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0).unwrap();
            assert!(scorer.is::<Intersection<TermScorer>>());
        }
        {
            let query = query_parser.parse_query("+a +(b c)").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0).unwrap();
            assert!(scorer.is::<Intersection<Box<dyn Scorer>>>());
        }
    }

    #[test]
    pub fn test_boolean_reqopt() {
        let (index, text_field) = aux_test_helper();
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.reader().unwrap().searcher();
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&searcher, true).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0).unwrap();
            assert!(scorer.is::<RequiredOptionalScorer<
                Box<dyn Scorer>,
                Box<dyn Scorer>,
                SumWithCoordsCombiner,
            >>());
        }
        {
            let query = query_parser.parse_query("+a b").unwrap();
            let weight = query.weight(&searcher, false).unwrap();
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0).unwrap();
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
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };

        let reader = index.reader().unwrap();

        let matching_docs = |boolean_query: &dyn Query| {
            reader
                .searcher()
                .search(boolean_query, &TEST_COLLECTOR_WITH_SCORE)
                .unwrap()
                .docs()
                .iter()
                .cloned()
                .map(|doc| doc.doc_id)
                .collect::<Vec<DocId>>()
        };
        {
            let boolean_query = BooleanQuery::new(vec![(Occur::Must, make_term_query("a"))]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::new(vec![(Occur::Should, make_term_query("a"))]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::new(vec![
                (Occur::Should, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 2, 3]);
        }
        {
            let boolean_query = BooleanQuery::new(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1, 3]);
        }
        {
            let boolean_query = BooleanQuery::new(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Should, make_term_query("b")),
                (Occur::MustNot, make_term_query("d")),
            ]);
            assert_eq!(matching_docs(&boolean_query), vec![0, 1]);
        }
        {
            let boolean_query = BooleanQuery::new(vec![(Occur::MustNot, make_term_query("d"))]);
            assert_eq!(matching_docs(&boolean_query), Vec::<u32>::new());
        }
    }

    #[test]
    pub fn test_boolean_query_two_excluded() {
        let (index, text_field) = aux_test_helper();

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };

        let reader = index.reader().unwrap();

        let matching_topdocs = |query: &dyn Query| {
            reader
                .searcher()
                .search(query, &TopDocs::with_limit(3))
                .unwrap()
        };

        let score_doc_4: Score; // score of doc 4 should not be influenced by exclusion
        {
            let boolean_query_no_excluded =
                BooleanQuery::new(vec![(Occur::Must, make_term_query("d"))]);
            let topdocs_no_excluded = matching_topdocs(&boolean_query_no_excluded);
            assert_eq!(topdocs_no_excluded.len(), 2);
            let (top_score, top_doc) = topdocs_no_excluded[0];
            assert_eq!(top_doc, DocAddress::new(0, 4));
            assert_eq!(topdocs_no_excluded[1].1, DocAddress::new(0, 3)); // ignore score of doc 3.
            score_doc_4 = top_score;
        }

        {
            let boolean_query_two_excluded = BooleanQuery::new(vec![
                (Occur::Must, make_term_query("d")),
                (Occur::MustNot, make_term_query("a")),
                (Occur::MustNot, make_term_query("b")),
            ]);
            let topdocs_excluded = matching_topdocs(&boolean_query_two_excluded);
            assert_eq!(topdocs_excluded.len(), 1);
            let (top_score, top_doc) = topdocs_excluded[0];
            assert_eq!(top_doc, DocAddress::new(0, 4));
            assert_eq!(top_score, score_doc_4);
        }
    }

    #[test]
    pub fn test_boolean_query_with_weight() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.add_document(doc!(text_field => "a b c"));
            index_writer.add_document(doc!(text_field => "a c"));
            index_writer.add_document(doc!(text_field => "b c"));
            assert!(index_writer.commit().is_ok());
        }
        let term_a: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "a"),
            IndexRecordOption::WithFreqs,
        ));
        let term_b: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "b"),
            IndexRecordOption::WithFreqs,
        ));
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let boolean_query =
            BooleanQuery::new(vec![(Occur::Should, term_a), (Occur::Should, term_b)]);
        let boolean_weight = boolean_query.weight(&searcher, true).unwrap();
        {
            let mut boolean_scorer = boolean_weight
                .scorer(searcher.segment_reader(0u32), 1.0)
                .unwrap();
            assert_eq!(boolean_scorer.doc(), 0u32);
            assert_nearly_equals!(boolean_scorer.score(), 0.84163445);
        }
        {
            let mut boolean_scorer = boolean_weight
                .scorer(searcher.segment_reader(0u32), 2.0)
                .unwrap();
            assert_eq!(boolean_scorer.doc(), 0u32);
            assert_nearly_equals!(boolean_scorer.score(), 1.6832689);
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
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };
        let reader = index.reader().unwrap();
        let score_docs = |boolean_query: &dyn Query| {
            let fruit = reader
                .searcher()
                .search(boolean_query, &TEST_COLLECTOR_WITH_SCORE)
                .unwrap();
            fruit.scores().to_vec()
        };

        {
            let boolean_query = BooleanQuery::new(vec![
                (Occur::Must, make_term_query("a")),
                (Occur::Must, make_term_query("b")),
            ]);
            let scores = score_docs(&boolean_query);
            assert_nearly_equals!(scores[0], 0.977973);
            assert_nearly_equals!(scores[1], 0.84699446);
        }
    }

    #[test]
    pub fn test_explain() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 5_000_000)?;
        index_writer.add_document(doc!(text=>"a"));
        index_writer.add_document(doc!(text=>"b"));
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let term_a: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text, "a"),
            IndexRecordOption::Basic,
        ));
        let term_b: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text, "b"),
            IndexRecordOption::Basic,
        ));
        let query = BooleanQuery::from(vec![(Occur::Should, term_a), (Occur::Should, term_b)]);
        let explanation = query.explain(&searcher, DocAddress::new(0, 0u32))?;
        assert_nearly_equals!(explanation.value(), 0.6931472);
        Ok(())
    }
}
