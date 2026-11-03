use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, TEXT};
use tantivy::{doc, DocAddress, Index, Term};

fn term(field: tantivy::schema::Field, value: &str) -> Box<dyn Query> {
    Box::new(TermQuery::new(
        Term::from_field_text(field, value),
        IndexRecordOption::WithFreqs,
    ))
}

#[test]
fn boolean_explain_nonmatching_early_doc_returns_error() -> tantivy::Result<()> {
    let mut builder = Schema::builder();
    let field = builder.add_text_field("title", TEXT);
    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads::<tantivy::TantivyDocument>(1, 50_000_000)?;
    writer.add_document(doc!(field => "amber"))?;
    writer.add_document(doc!(field => "quartz"))?;
    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    for clauses in [
        vec![(Occur::Should, term(field, "absent"))],
        vec![(Occur::Must, term(field, "quartz"))],
        vec![
            (Occur::Should, term(field, "quartz")),
            (Occur::Should, term(field, "absent")),
        ],
    ] {
        let query = BooleanQuery::new(clauses);
        assert!(query.explain(&searcher, DocAddress::new(0, 0)).is_err());
    }
    Ok(())
}

fn boost_case(outer: f32, inner: f32, field_boost: f32) -> tantivy::Result<()> {
    let mut builder = Schema::builder();
    let field = builder.add_text_field("title", TEXT);
    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads::<tantivy::TantivyDocument>(1, 50_000_000)?;
    for i in 0..10 {
        writer.add_document(doc!(field => if i == 0 { "amber" } else { "quartz" }))?;
    }
    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query = BoostQuery::new(
        Box::new(BoostQuery::new(
            Box::new(BoostQuery::new(term(field, "amber"), field_boost)),
            inner,
        )),
        outer,
    );
    let hits = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].0.is_finite());
    let explanation = query.explain(&searcher, hits[0].1)?;
    assert_eq!(explanation.value(), hits[0].0);
    fn finite(node: &serde_json::Value) {
        assert!(node["value"].as_f64().is_some_and(f64::is_finite), "{node}");
        if let Some(children) = node["details"].as_array() {
            for child in children {
                finite(child);
            }
        }
    }
    finite(&serde_json::to_value(explanation).unwrap());
    Ok(())
}

#[test]
fn zero_boost_explanation_preserves_outer_to_inner_arithmetic() -> tantivy::Result<()> {
    boost_case(0.0, 2.0, 3e38)
}

#[test]
fn small_boost_explanation_preserves_outer_to_inner_arithmetic() -> tantivy::Result<()> {
    boost_case(1e-20, 1e20, 1e20)
}

#[test]
fn built_in_contextual_explanations_match_native_scorers() -> tantivy::Result<()> {
    use std::ops::Bound::Included;

    use tantivy::indexer::NoMergePolicy;
    use tantivy::query::doc_predicate_query::{DocPredicateQuery, FunctionPredicate};
    use tantivy::query::{
        AllQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery, EnableScoring, ExistsQuery,
        FastFieldRangeQuery, FuzzyTermQuery, InvertedIndexRangeQuery, PhrasePrefixQuery,
        PhraseQuery, RegexPhraseQuery, RegexQuery,
    };
    use tantivy::schema::{FAST, INDEXED};
    use tantivy::{DocSet, SegmentReader, TERMINATED};
    let mut builder = Schema::builder();
    let text = builder.add_text_field("title", TEXT);
    let number = builder.add_u64_field("number", INDEXED | FAST);
    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads::<tantivy::TantivyDocument>(1, 50_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for i in 0..12 {
        writer.add_document(doc!(text => if i % 2 == 0 { "amber quartz rare" } else { "quartz rare" }, number => i as u64))?;
        if i == 5 {
            writer.commit()?;
        }
    }
    writer.commit()?;
    writer.delete_term(Term::from_field_u64(number, 10));
    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 2);
    let range = || {
        (
            Included(Term::from_field_u64(number, 0)),
            Included(Term::from_field_u64(number, 11)),
        )
    };
    let predicate: DocPredicateQuery =
        FunctionPredicate::from(|_: &SegmentReader| Ok(|doc: u32| doc.is_multiple_of(2))).into();
    let (lower, upper) = range();
    let (inverted_lower, inverted_upper) = range();
    let queries: Vec<Box<dyn Query>> = vec![
        Box::new(AllQuery),
        term(text, "amber"),
        Box::new(PhraseQuery::new(vec![
            Term::from_field_text(text, "amber"),
            Term::from_field_text(text, "quartz"),
        ])),
        Box::new(RegexPhraseQuery::new(
            text,
            vec!["amb.*".into(), "quartz".into()],
        )),
        Box::new(PhrasePrefixQuery::new(vec![
            Term::from_field_text(text, "amber"),
            Term::from_field_text(text, "qua"),
        ])),
        Box::new(PhrasePrefixQuery::new(vec![
            Term::from_field_text(text, "amber"),
            Term::from_field_text(text, "quartz"),
            Term::from_field_text(text, "ra"),
        ])),
        Box::new(RegexQuery::from_pattern("amb.*", text)?),
        Box::new(FuzzyTermQuery::new(
            Term::from_field_text(text, "ambr"),
            1,
            true,
        )),
        Box::new(BooleanQuery::new(vec![
            (Occur::Should, term(text, "amber")),
            (Occur::Should, term(text, "quartz")),
        ])),
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, term(text, "amber")),
            (Occur::Must, term(text, "quartz")),
        ])),
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, term(text, "amber")),
            (Occur::Should, term(text, "quartz")),
        ])),
        Box::new(DisjunctionMaxQuery::with_tie_breaker(
            vec![term(text, "amber"), term(text, "quartz")],
            0.1,
        )),
        Box::new(ConstScoreQuery::new(term(text, "amber"), 2.5)),
        Box::new(predicate),
        Box::new(ExistsQuery::new("number".into(), false)),
        Box::new(FastFieldRangeQuery::new(lower, upper)),
        Box::new(InvertedIndexRangeQuery::new(inverted_lower, inverted_upper)),
        Box::new(EmptyQuery),
    ];
    for query in queries {
        let expects_match = !query.is::<EmptyQuery>();
        for outer in [0.0, 1e-20, 0.3] {
            let query = BoostQuery::new(
                Box::new(BoostQuery::new(
                    Box::new(BoostQuery::new(query.box_clone(), 1e20)),
                    1e20,
                )),
                outer,
            );
            let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
            let mut matches = 0;
            for segment in searcher.segment_readers() {
                let mut scorer = weight.scorer(segment, 1.0)?;
                while scorer.doc() != TERMINATED {
                    matches += 1;
                    let explanation = weight.explain(segment, scorer.doc())?;
                    assert_eq!(
                        explanation.value().to_bits(),
                        scorer.score().to_bits(),
                        "{query:?}"
                    );
                    let json = serde_json::to_value(explanation).unwrap();
                    assert!(
                        !json.to_string().contains("details unavailable"),
                        "builtin fallback: {query:?}"
                    );
                    if scorer.score().is_finite() {
                        fn finite(node: &serde_json::Value) {
                            assert!(node["value"].as_f64().is_some_and(f64::is_finite), "{node}");
                            if let Some(children) = node["details"].as_array() {
                                for child in children {
                                    finite(child);
                                }
                            }
                        }
                        finite(&json);
                    }
                    scorer.advance();
                }
            }
            assert_eq!(matches > 0, expects_match, "{query:?}");
        }
    }
    Ok(())
}

#[test]
fn custom_weight_context_default_preserves_unit_details_and_native_scores() -> tantivy::Result<()> {
    use tantivy::query::{AllQuery, EnableScoring, Explanation, Scorer, Weight};
    use tantivy::{Score, SegmentReader};
    struct Custom(Box<dyn Weight>);
    impl Weight for Custom {
        fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
            self.0.scorer(reader, boost)
        }
        fn explain(&self, _: &SegmentReader, _: u32) -> tantivy::Result<Explanation> {
            Ok(Explanation::new("legacy detail", 1.0))
        }
    }
    let index = Index::create_in_ram(Schema::builder().build());
    let mut writer = index.writer_with_num_threads::<tantivy::TantivyDocument>(1, 50_000_000)?;
    writer.add_document(tantivy::TantivyDocument::new())?;
    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let weight = Custom(AllQuery.weight(EnableScoring::enabled_from_searcher(&searcher))?);
    let segment = searcher.segment_reader(0);
    assert_eq!(
        weight.explain_with_boost(segment, 0, 1.0)?.to_pretty_json(),
        "{\n  \"value\": 1.0,\n  \"description\": \"legacy detail\"\n}"
    );
    for boost in [0.0, 0.3, f32::NAN] {
        let explained = weight.explain_with_boost(segment, 0, boost)?.value();
        let scored = weight.scorer(segment, boost)?.score();
        assert_eq!(explained.to_bits(), scored.to_bits());
    }
    assert!(weight.explain_with_boost(segment, 1, 0.3).is_err());
    assert_eq!(
        AllQuery
            .explain(&searcher, DocAddress::new(0, 0))?
            .to_pretty_json(),
        "{\n  \"value\": 1.0,\n  \"description\": \"AllQuery\"\n}"
    );
    let nonfinite =
        BoostQuery::new(Box::new(AllQuery), f32::NAN).explain(&searcher, DocAddress::new(0, 0))?;
    assert!(nonfinite.value().is_nan());
    Ok(())
}
