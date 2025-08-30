mod block_wand;
mod boolean_query;
mod boolean_weight;

pub(crate) use self::block_wand::{block_wand, block_wand_single_scorer};
pub use self::boolean_query::BooleanQuery;
pub use self::boolean_weight::BooleanWeight;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::collector::tests::TEST_COLLECTOR_WITH_SCORE;
    use crate::collector::TopDocs;
    use crate::query::term_query::TermScorer;
    use crate::query::{
        EnableScoring, Intersection, Occur, Query, QueryParser, RequiredOptionalScorer, Scorer,
        SumCombiner, TermQuery,
    };
    use crate::schema::*;
    use crate::{assert_nearly_equals, DocAddress, DocId, Index, IndexWriter, Score};

    fn aux_test_helper() -> crate::Result<(Index, Field)> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field => "a b c"))?;
            index_writer.add_document(doc!(text_field => "a c"))?;
            index_writer.add_document(doc!(text_field => "b c"))?;
            index_writer.add_document(doc!(text_field => "a b c d"))?;
            index_writer.add_document(doc!(text_field => "d"))?;
            index_writer.commit()?;
        }
        Ok((index, text_field))
    }

    #[test]
    pub fn test_boolean_non_all_term_disjunction() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("(+a +b) d")?;
        let searcher = index.reader()?.searcher();
        assert_eq!(query.count(&searcher)?, 3);
        Ok(())
    }

    #[test]
    pub fn test_boolean_single_must_clause() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let query = query_parser.parse_query("+a")?;
        let searcher = index.reader()?.searcher();
        let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
        let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
        assert!(scorer.is::<TermScorer>());
        Ok(())
    }

    #[test]
    pub fn test_boolean_termonly_intersection() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.reader()?.searcher();
        {
            let query = query_parser.parse_query("+a +b +c")?;
            let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
            assert!(scorer.is::<Intersection<TermScorer>>());
        }
        {
            let query = query_parser.parse_query("+a +(b c)")?;
            let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
            assert!(scorer.is::<Intersection<Box<dyn Scorer>>>());
        }
        Ok(())
    }

    #[test]
    pub fn test_boolean_reqopt() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let searcher = index.reader()?.searcher();
        {
            let query = query_parser.parse_query("+a b")?;
            let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
            assert!(scorer
                .is::<RequiredOptionalScorer<Box<dyn Scorer>, Box<dyn Scorer>, SumCombiner>>());
        }
        {
            let query = query_parser.parse_query("+a b")?;
            let weight = query.weight(EnableScoring::disabled_from_schema(searcher.schema()))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
            assert!(scorer.is::<TermScorer>());
        }
        Ok(())
    }

    #[test]
    pub fn test_boolean_query() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };

        let reader = index.reader()?;

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
        Ok(())
    }

    #[test]
    pub fn test_boolean_query_two_excluded() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };

        let reader = index.reader()?;

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
        Ok(())
    }

    #[test]
    pub fn test_boolean_query_with_weight() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field => "a b c"))?;
            index_writer.add_document(doc!(text_field => "a c"))?;
            index_writer.add_document(doc!(text_field => "b c"))?;
            index_writer.commit()?;
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
        let boolean_weight = boolean_query
            .weight(EnableScoring::enabled_from_searcher(&searcher))
            .unwrap();
        {
            let mut boolean_scorer = boolean_weight.scorer(searcher.segment_reader(0u32), 1.0)?;
            assert_eq!(boolean_scorer.doc(), 0u32);
            assert_nearly_equals!(boolean_scorer.score(), 0.84163445);
        }
        {
            let mut boolean_scorer = boolean_weight.scorer(searcher.segment_reader(0u32), 2.0)?;
            assert_eq!(boolean_scorer.doc(), 0u32);
            assert_nearly_equals!(boolean_scorer.score(), 1.6832689);
        }
        Ok(())
    }

    #[test]
    pub fn test_intersection_score() -> crate::Result<()> {
        let (index, text_field) = aux_test_helper()?;

        let make_term_query = |text: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, text),
                IndexRecordOption::Basic,
            );
            let query: Box<dyn Query> = Box::new(term_query);
            query
        };
        let reader = index.reader()?;
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
        Ok(())
    }

    #[test]
    pub fn test_explain() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text=>"a"))?;
        index_writer.add_document(doc!(text=>"b"))?;
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
        assert_nearly_equals!(explanation.value(), std::f32::consts::LN_2);
        Ok(())
    }
}

/// A proptest which generates arbitrary permutations of a simple boolean AST, and then matches
/// the result against an index which contains all permutations of documents with N fields.
#[cfg(test)]
mod proptest_boolean_query {
    use std::collections::{BTreeMap, HashSet};

    use proptest::collection::vec;
    use proptest::prelude::*;

    use crate::collector::DocSetCollector;
    use crate::query::{BooleanQuery, Occur, Query, TermQuery};
    use crate::schema::{Field, OwnedValue, Schema, TEXT};
    use crate::{DocId, Index, Term};

    #[derive(Debug, Clone)]
    enum BooleanQueryAST {
        Leaf { field_idx: usize },
        Union(Vec<BooleanQueryAST>),
        Intersection(Vec<BooleanQueryAST>),
    }

    impl BooleanQueryAST {
        fn matches(&self, doc_id: DocId) -> bool {
            match self {
                BooleanQueryAST::Leaf { field_idx } => Self::matches_field(doc_id, *field_idx),
                BooleanQueryAST::Union(children) => {
                    children.iter().any(|child| child.matches(doc_id))
                }
                BooleanQueryAST::Intersection(children) => {
                    children.iter().all(|child| child.matches(doc_id))
                }
            }
        }

        fn matches_field(doc_id: DocId, field_idx: usize) -> bool {
            ((doc_id as usize) >> field_idx) & 1 == 1
        }

        fn to_query(&self, fields: &[Field]) -> Box<dyn Query> {
            match self {
                BooleanQueryAST::Leaf { field_idx } => Box::new(TermQuery::new(
                    Term::from_field_text(fields[*field_idx], "true"),
                    crate::schema::IndexRecordOption::Basic,
                )),
                BooleanQueryAST::Union(children) => {
                    let sub_queries = children
                        .iter()
                        .map(|child| (Occur::Should, child.to_query(fields)))
                        .collect();
                    Box::new(BooleanQuery::new(sub_queries))
                }
                BooleanQueryAST::Intersection(children) => {
                    let sub_queries = children
                        .iter()
                        .map(|child| (Occur::Must, child.to_query(fields)))
                        .collect();
                    Box::new(BooleanQuery::new(sub_queries))
                }
            }
        }
    }

    fn create_index_with_boolean_permutations(num_fields: usize) -> (Index, Vec<Field>) {
        let mut schema_builder = Schema::builder();
        let fields: Vec<Field> = (0..num_fields)
            .map(|i| schema_builder.add_text_field(&format!("field_{}", i), TEXT))
            .collect();
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();

        for doc_id in 0..(1 << num_fields) {
            let mut doc: BTreeMap<_, OwnedValue> = BTreeMap::default();
            for (field_idx, &field) in fields.iter().enumerate() {
                if (doc_id >> field_idx) & 1 == 1 {
                    doc.insert(field, "true".into());
                }
            }
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        (index, fields)
    }

    fn arb_boolean_query_ast(num_fields: usize) -> impl Strategy<Value = BooleanQueryAST> {
        let leaf = (0..num_fields).prop_map(|field_idx| BooleanQueryAST::Leaf { field_idx });
        leaf.prop_recursive(
            8,   // 8 levels of recursion
            256, // 256 nodes max
            10,  // 10 items per collection
            |inner| {
                prop_oneof![
                    vec(inner.clone(), 1..10).prop_map(BooleanQueryAST::Union),
                    vec(inner, 1..10).prop_map(BooleanQueryAST::Intersection),
                ]
            },
        )
    }

    #[test]
    fn proptest_boolean_query() {
        let num_fields = 8;
        let (index, fields) = create_index_with_boolean_permutations(num_fields);
        let searcher = index.reader().unwrap().searcher();
        proptest!(|(ast in arb_boolean_query_ast(num_fields))| {
            let query = ast.to_query(&fields);

            let mut matching_docs = HashSet::new();
            for doc_id in 0..(1 << num_fields) {
                if ast.matches(doc_id as DocId) {
                    matching_docs.insert(doc_id as DocId);
                }
            }

            let doc_addresses = searcher.search(&*query, &DocSetCollector).unwrap();
            let result_docs: HashSet<DocId> =
                doc_addresses.into_iter().map(|doc_address| doc_address.doc_id).collect();
            prop_assert_eq!(result_docs, matching_docs);
        });
    }
}
