mod block_wand;
mod boolean_query;
mod boolean_weight;

pub(crate) use self::block_wand::{block_wand, block_wand_single_scorer};
pub use self::boolean_query::BooleanQuery;
pub use self::boolean_weight::BooleanWeight;

#[cfg(test)]
mod tests {

    use std::ops::Bound;

    use super::*;
    use crate::collector::tests::TEST_COLLECTOR_WITH_SCORE;
    use crate::collector::{Count, TopDocs};
    use crate::query::term_query::TermScorer;
    use crate::query::{
        AllScorer, EmptyScorer, EnableScoring, Intersection, Occur, Query, QueryParser, RangeQuery,
        RequiredOptionalScorer, Scorer, SumCombiner, TermQuery,
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
                .search(query, &TopDocs::with_limit(3).order_by_score())
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

    #[test]
    pub fn test_boolean_weight_optimization() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text_field=>"hello"))?;
        index_writer.add_document(doc!(text_field=>"hello happy"))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let term_match_all: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "hello"),
            IndexRecordOption::Basic,
        ));
        let term_match_some: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "happy"),
            IndexRecordOption::Basic,
        ));
        let term_match_none: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "tax"),
            IndexRecordOption::Basic,
        ));
        {
            let query = BooleanQuery::from(vec![
                (Occur::Must, term_match_all.box_clone()),
                (Occur::Must, term_match_some.box_clone()),
            ]);
            let weight = query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0f32)?;
            assert!(scorer.is::<TermScorer>());
        }
        {
            let query = BooleanQuery::from(vec![
                (Occur::Must, term_match_all.box_clone()),
                (Occur::Must, term_match_some.box_clone()),
                (Occur::Must, term_match_none.box_clone()),
            ]);
            let weight = query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0f32)?;
            assert!(scorer.is::<EmptyScorer>());
        }
        {
            let query = BooleanQuery::from(vec![
                (Occur::Should, term_match_all.box_clone()),
                (Occur::Should, term_match_none.box_clone()),
            ]);
            let weight = query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0f32)?;
            assert!(scorer.is::<AllScorer>());
        }
        {
            let query = BooleanQuery::from(vec![
                (Occur::Should, term_match_some.box_clone()),
                (Occur::Should, term_match_none.box_clone()),
            ]);
            let weight = query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
            let scorer = weight.scorer(searcher.segment_reader(0u32), 1.0f32)?;
            assert!(scorer.is::<TermScorer>());
        }
        Ok(())
    }

    #[test]
    pub fn test_min_should_match_with_all_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let num_field =
            schema_builder.add_i64_field("num", NumericOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        index_writer.add_document(doc!(text_field => "apple", num_field => 10i64))?;
        index_writer.add_document(doc!(text_field => "banana", num_field => 20i64))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();

        let effective_all_match_query: Box<dyn Query> = Box::new(RangeQuery::new(
            Bound::Excluded(Term::from_field_i64(num_field, 0)),
            Bound::Unbounded,
        ));
        let term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "apple"),
            IndexRecordOption::Basic,
        ));

        // in some previous version, we would remove the 2 all_match, but then say we need *4*
        // matches out of the 3 term queries, which matches nothing.
        let mut bool_query = BooleanQuery::new(vec![
            (Occur::Should, effective_all_match_query.box_clone()),
            (Occur::Should, effective_all_match_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
        ]);
        bool_query.set_minimum_number_should_match(4);
        let count = searcher.search(&bool_query, &Count)?;
        assert_eq!(count, 1);

        Ok(())
    }

    // =========================================================================
    // AllScorer Preservation Regression Tests
    // =========================================================================
    //
    // These tests verify the fix for a bug where AllScorer instances (produced by
    // queries matching all documents, such as range queries covering all values)
    // were incorrectly removed from Boolean query processing, causing documents
    // to be unexpectedly excluded from results.
    //
    // The bug manifested in several scenarios:
    // 1. SHOULD + SHOULD where one clause is AllScorer
    // 2. MUST (AllScorer) + SHOULD
    // 3. Range queries in Boolean clauses when all documents match the range

    /// Regression test: SHOULD clause with AllScorer combined with other SHOULD clauses.
    ///
    /// When a SHOULD clause produces an AllScorer (e.g., from AllQuery or a range query
    /// matching all documents), the Boolean query should still match all documents.
    ///
    /// Bug before fix: AllScorer was removed during optimization, leaving only the
    /// other SHOULD clauses, which incorrectly excluded documents.
    #[test]
    pub fn test_should_with_all_scorer_regression() -> crate::Result<()> {
        use crate::collector::Count;
        use crate::query::AllQuery;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        index_writer.add_document(doc!(text_field => "hello"))?;
        index_writer.add_document(doc!(text_field => "world"))?;
        index_writer.add_document(doc!(text_field => "hello world"))?;
        index_writer.add_document(doc!(text_field => "foo"))?;
        index_writer.add_document(doc!(text_field => "bar"))?;
        index_writer.add_document(doc!(text_field => "baz"))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let all_query: Box<dyn Query> = Box::new(AllQuery);
        let term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "hello"),
            IndexRecordOption::Basic,
        ));

        // AllQuery OR TermQuery should match all 6 docs
        let bool_query = BooleanQuery::new(vec![
            (Occur::Should, all_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
        ]);
        let count = searcher.search(&bool_query, &Count)?;
        assert_eq!(count, 6, "SHOULD with AllQuery should match all docs");

        // Order should not matter
        let bool_query_reversed = BooleanQuery::new(vec![
            (Occur::Should, term_query.box_clone()),
            (Occur::Should, all_query.box_clone()),
        ]);
        let count_reversed = searcher.search(&bool_query_reversed, &Count)?;
        assert_eq!(
            count_reversed, 6,
            "Order of SHOULD clauses should not matter"
        );

        Ok(())
    }

    /// Regression test: MUST clause with AllScorer combined with SHOULD clause.
    ///
    /// When MUST contains an AllScorer, all documents satisfy the MUST constraint.
    /// The SHOULD clause should only affect scoring, not filtering.
    ///
    /// Bug before fix: AllScorer was removed, leaving an empty must_scorers vector.
    /// intersect_scorers([]) incorrectly returned EmptyScorer, matching 0 documents.
    #[test]
    pub fn test_must_all_with_should_regression() -> crate::Result<()> {
        use crate::collector::Count;
        use crate::query::AllQuery;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        index_writer.add_document(doc!(text_field => "apple"))?;
        index_writer.add_document(doc!(text_field => "banana"))?;
        index_writer.add_document(doc!(text_field => "cherry"))?;
        index_writer.add_document(doc!(text_field => "date"))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let all_query: Box<dyn Query> = Box::new(AllQuery);
        let term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "apple"),
            IndexRecordOption::Basic,
        ));

        // MUST(all) AND SHOULD(term) should match all 4 docs
        let bool_query = BooleanQuery::new(vec![
            (Occur::Must, all_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
        ]);
        let count = searcher.search(&bool_query, &Count)?;
        assert_eq!(count, 4, "MUST AllQuery + SHOULD should match all docs");

        Ok(())
    }

    /// Regression test: Range queries in Boolean clauses when all documents match.
    ///
    /// Range queries can return AllScorer as an optimization when all indexed values
    /// fall within the range. This test ensures such queries work correctly in
    /// Boolean combinations.
    ///
    /// This is the most common real-world manifestation of the bug, occurring in
    /// queries like: (age > 50 OR name = 'Alice') AND status = 'active'
    /// when all documents have age > 50.
    #[test]
    pub fn test_range_query_all_match_in_boolean() -> crate::Result<()> {
        use crate::collector::Count;
        use crate::query::RangeQuery;
        use crate::schema::NumericOptions;

        let mut schema_builder = Schema::builder();
        let name_field = schema_builder.add_text_field("name", TEXT);
        let age_field =
            schema_builder.add_i64_field("age", NumericOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        // All documents have age > 50, so range query will return AllScorer
        index_writer.add_document(doc!(name_field => "alice", age_field => 55_i64))?;
        index_writer.add_document(doc!(name_field => "bob", age_field => 60_i64))?;
        index_writer.add_document(doc!(name_field => "charlie", age_field => 70_i64))?;
        index_writer.add_document(doc!(name_field => "diana", age_field => 80_i64))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();

        let range_query: Box<dyn Query> = Box::new(RangeQuery::new(
            Bound::Excluded(Term::from_field_i64(age_field, 50)),
            Bound::Unbounded,
        ));
        let term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(name_field, "alice"),
            IndexRecordOption::Basic,
        ));

        // Verify preconditions
        assert_eq!(searcher.search(range_query.as_ref(), &Count)?, 4);
        assert_eq!(searcher.search(term_query.as_ref(), &Count)?, 1);

        // SHOULD(range) OR SHOULD(term): range matches all, so result is 4
        let should_query = BooleanQuery::new(vec![
            (Occur::Should, range_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
        ]);
        assert_eq!(
            searcher.search(&should_query, &Count)?,
            4,
            "SHOULD range OR term should match all"
        );

        // MUST(range) AND SHOULD(term): range matches all, term is optional
        let must_should_query = BooleanQuery::new(vec![
            (Occur::Must, range_query.box_clone()),
            (Occur::Should, term_query.box_clone()),
        ]);
        assert_eq!(
            searcher.search(&must_should_query, &Count)?,
            4,
            "MUST range + SHOULD term should match all"
        );

        Ok(())
    }

    /// Test multiple AllScorer instances in different clause types.
    ///
    /// Verifies correct behavior when AllScorers appear in multiple positions.
    #[test]
    pub fn test_multiple_all_scorers() -> crate::Result<()> {
        use crate::collector::Count;
        use crate::query::AllQuery;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        index_writer.add_document(doc!(text_field => "doc1"))?;
        index_writer.add_document(doc!(text_field => "doc2"))?;
        index_writer.add_document(doc!(text_field => "doc3"))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let all_query1: Box<dyn Query> = Box::new(AllQuery);
        let all_query2: Box<dyn Query> = Box::new(AllQuery);
        let term_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(text_field, "doc1"),
            IndexRecordOption::Basic,
        ));

        // Multiple AllQueries in SHOULD
        let multi_all_should = BooleanQuery::new(vec![
            (Occur::Should, all_query1.box_clone()),
            (Occur::Should, all_query2.box_clone()),
            (Occur::Should, term_query.box_clone()),
        ]);
        assert_eq!(
            searcher.search(&multi_all_should, &Count)?,
            3,
            "Multiple AllQueries in SHOULD"
        );

        // AllQuery in both MUST and SHOULD
        let all_must_and_should = BooleanQuery::new(vec![
            (Occur::Must, all_query1.box_clone()),
            (Occur::Should, all_query2.box_clone()),
        ]);
        assert_eq!(
            searcher.search(&all_must_and_should, &Count)?,
            3,
            "AllQuery in both MUST and SHOULD"
        );

        Ok(())
    }
}
