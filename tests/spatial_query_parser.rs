use std::collections::BTreeMap;
use std::ops::Bound;

use tantivy::collector::{Count, DocSetCollector, FacetCollector, TopDocs};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, Occur, Query, QueryParser, QueryParserError,
    RangeQuery, SpatialPredicate, SpatialQuery, TermQuery,
};
use tantivy::schema::{
    Facet, FacetOptions, Field, IndexRecordOption, Schema, Value, INDEXED, SPHERE, STORED, STRING,
};
use tantivy::spatial::executor::{PlanNode, SpatialExecutor, SpatialRelation};
use tantivy::{DocAddress, Index, IndexWriter, Score, Searcher, TantivyDocument, Term};

// One kilometer in radians on a sphere with radius 6371 km.
const ONE_KM: f64 = 1.0 / 6371.0;

struct Fixture {
    searcher: Searcher,
    geometry: Field,
    name: Field,
    kind: Field,
    open: Field,
    area: Field,
}

fn square(lon: f64, lat: f64) -> Vec<[f64; 2]> {
    vec![
        [lon - 0.02, lat - 0.02],
        [lon + 0.02, lat - 0.02],
        [lon + 0.02, lat + 0.02],
        [lon - 0.02, lat + 0.02],
        [lon - 0.02, lat - 0.02],
    ]
}

fn term(field: Field, text: &str) -> Box<dyn Query> {
    Box::new(TermQuery::new(
        Term::from_field_text(field, text),
        IndexRecordOption::Basic,
    ))
}

fn and(queries: Vec<Box<dyn Query>>) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(
        queries
            .into_iter()
            .map(|query| (Occur::Must, query))
            .collect(),
    ))
}

fn parser_queries(parser: &QueryParser, text: &str) -> [Box<dyn Query>; 4] {
    let ast = query_grammar::parse_query(text).unwrap();
    let from_text = parser.parse_query(text).unwrap();
    let from_ast = parser.build_query_from_user_input_ast(ast.clone()).unwrap();
    let (lenient_text, errors) = parser.parse_query_lenient(text);
    assert!(errors.is_empty(), "{text}: {errors:?}");
    let (lenient_ast, errors) = parser.build_query_from_user_input_ast_lenient(ast);
    assert!(errors.is_empty(), "{text}: {errors:?}");
    [from_text, from_ast, lenient_text, lenient_ast]
}

impl Fixture {
    fn new(open_closed_trail: bool) -> tantivy::Result<Self> {
        Self::with_deletions(open_closed_trail, &[])
    }

    fn with_deletions(open_closed_trail: bool, deleted_names: &[&str]) -> tantivy::Result<Self> {
        let mut builder = Schema::builder();
        let geometry = builder.add_spatial_field("geometry", SPHERE);
        let name = builder.add_text_field("name", STRING | STORED);
        let kind = builder.add_text_field("kind", STRING);
        let open = builder.add_bool_field("open", INDEXED);
        let area = builder.add_u64_field("area", INDEXED);
        builder.add_facet_field("category", FacetOptions::default());
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for (label, kind, area, is_open, lon, lat) in [
            ("p_ok", "park", 100u64, false, 0.00, 0.00),
            ("p_hit", "park", 100, false, 1.00, 0.00),
            ("p_small", "park", 1, false, 1.04, 0.00),
            ("p_far", "park", 100, false, 3.00, 0.00),
            ("t_open", "trail", 0, true, 1.02, 0.01),
            ("t_closed", "trail", 0, open_closed_trail, -0.01, 0.01),
            ("r_open", "road", 0, true, 0.01, 0.02),
            ("n_hit", "road", 100, false, 1.01, 0.02),
            // This park is disjoint from t_open, but within one kilometer.
            ("p_band", "park", 100, false, 1.061, 0.00),
        ] {
            let size = if area >= 50 { "large" } else { "small" };
            let json = serde_json::json!({
                "name": label,
                "kind": kind,
                "open": is_open,
                "area": area,
                "category": format!("/{kind}/{size}"),
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [square(lon, lat)],
                },
            });
            writer.add_document(TantivyDocument::parse_json(&schema, &json.to_string())?)?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        if !deleted_names.is_empty() {
            for label in deleted_names {
                writer.delete_term(Term::from_field_text(name, label));
            }
            writer.commit()?;
            reader.reload()?;
        }
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment = &searcher.segment_readers()[0];
        // Deleted geometry must remain in the segment for these tests.
        assert_eq!(
            (segment.max_doc(), segment.num_deleted_docs()),
            (9, deleted_names.len() as u32)
        );
        Ok(Self {
            searcher,
            geometry,
            name,
            kind,
            open,
            area,
        })
    }

    fn parks(&self) -> Box<dyn Query> {
        term(self.kind, "park")
    }

    fn large(&self) -> Box<dyn Query> {
        Box::new(RangeQuery::new(
            Bound::Included(Term::from_field_u64(self.area, 50)),
            Bound::Unbounded,
        ))
    }

    fn open_features(&self, kind: &str) -> Box<dyn Query> {
        and(vec![
            term(self.kind, kind),
            Box::new(TermQuery::new(
                Term::from_field_bool(self.open, true),
                IndexRecordOption::Basic,
            )),
        ])
    }

    fn join(&self, outer: Box<dyn Query>, relation: SpatialRelation) -> Box<dyn Query> {
        Box::new(SpatialExecutor::new(PlanNode::Join {
            field: self.geometry,
            outer: Box::new(PlanNode::Query(outer)),
            inner: Box::new(PlanNode::Query(self.open_features("trail"))),
            relation,
        }))
    }

    fn literal_intersects(&self) -> SpatialQuery {
        SpatialQuery::with_predicate(
            self.geometry,
            square(1.02, 0.01),
            SpatialPredicate::Intersects,
        )
    }

    fn name(&self, address: DocAddress) -> tantivy::Result<String> {
        let doc: TantivyDocument = self.searcher.doc(address)?;
        Ok(doc
            .get_first(self.name)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned())
    }

    fn names(&self, query: &dyn Query) -> tantivy::Result<Vec<String>> {
        let mut names = self
            .searcher
            .search(query, &DocSetCollector)?
            .into_iter()
            .map(|address| self.name(address))
            .collect::<tantivy::Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    fn scores(&self, query: &dyn Query) -> tantivy::Result<BTreeMap<String, Score>> {
        self.searcher
            .search(
                query,
                &TopDocs::with_limit(self.searcher.num_docs() as usize).order_by_score(),
            )?
            .into_iter()
            .map(|(score, address)| Ok((self.name(address)?, score)))
            .collect()
    }

    fn assert_names(&self, query: &dyn Query, expected: &[&str]) -> tantivy::Result<()> {
        let mut expected: Vec<String> = expected.iter().map(|name| (*name).to_owned()).collect();
        expected.sort();
        assert_eq!(self.names(query)?, expected);
        Ok(())
    }

    fn assert_collectors(
        &self,
        context: &str,
        query: &dyn Query,
        expected_names: &[&str],
        expected_facets: &[(&str, u64)],
    ) -> tantivy::Result<()> {
        let mut names = expected_names.to_vec();
        names.sort();
        assert_eq!(self.names(query)?, names, "{context}: matching names");
        assert_eq!(
            self.searcher.search(query, &Count)?,
            expected_names.len(),
            "{context}: count"
        );
        let mut collector = FacetCollector::for_field("category");
        collector.add_facet("/park");
        let counts = self.searcher.search(query, &collector)?;
        let actual: Vec<_> = counts
            .get("/park")
            .map(|(facet, count)| (facet.clone(), count))
            .collect();
        let expected: Vec<_> = expected_facets
            .iter()
            .map(|(path, count)| (Facet::from(*path), *count))
            .collect();
        // FacetCollector omits buckets with no matching documents.
        assert_eq!(actual, expected, "{context}: facets");
        Ok(())
    }

    fn assert_parser_collectors(
        &self,
        text: &str,
        reference: &dyn Query,
        expected_names: &[&str],
        expected_facets: &[(&str, u64)],
    ) -> tantivy::Result<()> {
        self.assert_collectors(
            &format!("reference: {text}"),
            reference,
            expected_names,
            expected_facets,
        )?;
        let parser = QueryParser::for_index(self.searcher.index(), Vec::new());
        for (route, query) in ["text", "ast", "lenient text", "lenient ast"]
            .into_iter()
            .zip(parser_queries(&parser, text))
        {
            self.assert_collectors(
                &format!("{route}: {text}"),
                query.as_ref(),
                expected_names,
                expected_facets,
            )?;
        }
        Ok(())
    }
}

#[test]
fn test_parser_collectors_use_join_matches() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    for (relation, predicate) in [
        (SpatialRelation::Near(ONE_KM), "$within(1km, $query"),
        (SpatialRelation::Intersects, "$intersects($query"),
    ] {
        let near = matches!(relation, SpatialRelation::Near(_));
        for (occur, prefix, names, facets) in [
            (
                Occur::Must,
                "",
                if near {
                    ["p_band", "p_hit", "p_small"].as_slice()
                } else {
                    ["p_hit", "p_small"].as_slice()
                },
                [
                    ("/park/large", if near { 2 } else { 1 }),
                    ("/park/small", 1),
                ]
                .as_slice(),
            ),
            (
                Occur::MustNot,
                "NOT ",
                if near {
                    ["p_far", "p_ok"].as_slice()
                } else {
                    ["p_band", "p_far", "p_ok"].as_slice()
                },
                [("/park/large", if near { 2 } else { 3 })].as_slice(),
            ),
        ] {
            let reference = BooleanQuery::new(vec![
                (Occur::Must, fixture.parks()),
                (occur, fixture.join(Box::new(AllQuery), relation.clone())),
            ]);
            let text =
                format!("kind:park AND {prefix}geometry:{predicate}(kind:trail AND open:true))");
            fixture.assert_parser_collectors(&text, &reference, names, facets)?;
        }
        for (occur, prefix, names, facets) in [
            (Occur::Must, "", [].as_slice(), [].as_slice()),
            (
                Occur::MustNot,
                "NOT ",
                ["p_band", "p_far", "p_hit", "p_ok", "p_small"].as_slice(),
                [("/park/large", 4), ("/park/small", 1)].as_slice(),
            ),
        ] {
            let empty_join = SpatialExecutor::new(PlanNode::Join {
                field: fixture.geometry,
                outer: Box::new(PlanNode::Query(Box::new(AllQuery))),
                inner: Box::new(PlanNode::Query(Box::new(EmptyQuery))),
                relation: relation.clone(),
            });
            let reference = BooleanQuery::new(vec![
                (Occur::Must, fixture.parks()),
                (occur, Box::new(empty_join)),
            ]);
            let text = format!("kind:park AND {prefix}geometry:{predicate}(kind:missing))");
            fixture.assert_parser_collectors(&text, &reference, names, facets)?;
        }
    }
    Ok(())
}

#[test]
fn test_parser_joins_ignore_deleted_inner_geometry() -> tantivy::Result<()> {
    let fixture = Fixture::with_deletions(true, &["t_open"])?;
    for (relation, predicate) in [
        (
            SpatialRelation::Near(ONE_KM),
            "$within(1km, $query(kind:trail AND open:true))",
        ),
        (
            SpatialRelation::Intersects,
            "$intersects($query(kind:trail AND open:true))",
        ),
    ] {
        for (occur, prefix, names, facets) in [
            (
                Occur::Must,
                "",
                ["p_ok"].as_slice(),
                [("/park/large", 1)].as_slice(),
            ),
            (
                Occur::MustNot,
                "NOT ",
                ["p_band", "p_far", "p_hit", "p_small"].as_slice(),
                [("/park/large", 3), ("/park/small", 1)].as_slice(),
            ),
        ] {
            let reference = BooleanQuery::new(vec![
                (Occur::Must, fixture.parks()),
                (occur, fixture.join(Box::new(AllQuery), relation.clone())),
            ]);
            let text = format!("kind:park AND {prefix}geometry:{predicate}");
            fixture.assert_parser_collectors(&text, &reference, names, facets)?;
        }
    }
    Ok(())
}

#[test]
fn test_parser_joins_ignore_deleted_outer_geometry() -> tantivy::Result<()> {
    let fixture = Fixture::with_deletions(false, &["p_hit"])?;
    for (relation, predicate) in [
        (
            SpatialRelation::Near(ONE_KM),
            "$within(1km, $query(kind:trail AND open:true))",
        ),
        (
            SpatialRelation::Intersects,
            "$intersects($query(kind:trail AND open:true))",
        ),
    ] {
        let near = matches!(relation, SpatialRelation::Near(_));
        for (occur, prefix, names, facets) in [
            (
                Occur::Must,
                "",
                if near {
                    ["p_band", "p_small"].as_slice()
                } else {
                    ["p_small"].as_slice()
                },
                if near {
                    [("/park/large", 1), ("/park/small", 1)].as_slice()
                } else {
                    [("/park/small", 1)].as_slice()
                },
            ),
            (
                Occur::MustNot,
                "NOT ",
                if near {
                    ["p_far", "p_ok"].as_slice()
                } else {
                    ["p_band", "p_far", "p_ok"].as_slice()
                },
                [("/park/large", if near { 2 } else { 3 })].as_slice(),
            ),
        ] {
            let reference = BooleanQuery::new(vec![
                (Occur::Must, fixture.parks()),
                (occur, fixture.join(Box::new(AllQuery), relation.clone())),
            ]);
            let text = format!("kind:park AND {prefix}geometry:{predicate}");
            fixture.assert_parser_collectors(&text, &reference, names, facets)?;
        }
    }
    Ok(())
}

#[test]
fn test_join_filters_both_inputs() -> tantivy::Result<()> {
    for open_closed_trail in [false, true] {
        let fixture = Fixture::new(open_closed_trail)?;
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let mut expected = vec!["p_hit"];
            if matches!(relation, SpatialRelation::Near(_)) {
                expected.push("p_band");
            }
            if open_closed_trail {
                expected.push("p_ok");
            }
            let query = fixture.join(and(vec![fixture.parks(), fixture.large()]), relation);
            fixture.assert_names(query.as_ref(), &expected)?;
        }
    }
    Ok(())
}

#[test]
fn test_intersects_join_matches_literal_predicate() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let query = fixture.join(Box::new(AllQuery), SpatialRelation::Intersects);
    let joined = fixture.names(query.as_ref())?;
    assert_eq!(joined, fixture.names(&fixture.literal_intersects())?);
    // Use the literal predicate as the reference for the identical polygon.
    let others: Vec<&str> = joined
        .iter()
        .map(String::as_str)
        .filter(|name| *name != "t_open")
        .collect();
    assert_eq!(others, ["n_hit", "p_hit", "p_small"]);
    Ok(())
}

#[test]
fn test_join_boolean_combinations() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let intersects_self = fixture
        .names(&fixture.literal_intersects())?
        .iter()
        .any(|name| name == "t_open");
    for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
        let near = matches!(relation, SpatialRelation::Near(_));
        let mut joined = vec!["n_hit", "p_hit", "p_small"];
        if near {
            joined.push("p_band");
        }
        if near || intersects_self {
            joined.push("t_open");
        }
        let join = fixture.join(Box::new(AllQuery), relation);
        fixture.assert_names(join.as_ref(), &joined)?;

        let union = BooleanQuery::new(vec![
            (Occur::Should, fixture.parks()),
            (Occur::Should, join.box_clone()),
        ]);
        let mut expected = vec!["n_hit", "p_band", "p_far", "p_hit", "p_ok", "p_small"];
        if near || intersects_self {
            expected.push("t_open");
        }
        fixture.assert_names(&union, &expected)?;

        let outside = BooleanQuery::new(vec![
            (Occur::Must, fixture.parks()),
            (Occur::MustNot, join.box_clone()),
        ]);
        let mut expected = vec!["p_far", "p_ok"];
        if !near {
            expected.push("p_band");
        }
        fixture.assert_names(&outside, &expected)?;

        let without_roads = BooleanQuery::new(vec![
            (Occur::MustNot, term(fixture.kind, "road")),
            (Occur::Must, join),
        ]);
        joined.retain(|name| *name != "n_hit");
        fixture.assert_names(&without_roads, &joined)?;

        let large_union = and(vec![fixture.large(), Box::new(union)]);
        fixture.assert_names(
            large_union.as_ref(),
            &["n_hit", "p_band", "p_far", "p_hit", "p_ok"],
        )?;
    }
    Ok(())
}

#[test]
fn test_nested_join_uses_inner_result() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
        let inner = SpatialExecutor::new(PlanNode::Join {
            field: fixture.geometry,
            outer: Box::new(PlanNode::Query(term(fixture.kind, "trail"))),
            inner: Box::new(PlanNode::Query(fixture.open_features("road"))),
            relation: relation.clone(),
        });
        fixture.assert_names(&inner, &["t_closed"])?;
        let query = SpatialExecutor::new(PlanNode::Join {
            field: fixture.geometry,
            outer: Box::new(PlanNode::Query(and(vec![fixture.parks(), fixture.large()]))),
            inner: Box::new(PlanNode::Query(Box::new(inner))),
            relation,
        });
        fixture.assert_names(&query, &["p_ok"])?;
    }
    Ok(())
}

#[test]
fn test_join_with_empty_inner() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
        let query = SpatialExecutor::new(PlanNode::Join {
            field: fixture.geometry,
            outer: Box::new(PlanNode::Query(Box::new(AllQuery))),
            inner: Box::new(PlanNode::Query(Box::new(EmptyQuery))),
            relation,
        });
        fixture.assert_names(&query, &[])?;
    }
    Ok(())
}

#[test]
fn test_join_keeps_boolean_score_contributions() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parks = BoostQuery::new(fixture.parks(), 3.0);
    let park_scores = fixture.scores(&parks)?;
    for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
        let mut expected = vec!["p_hit", "p_small"];
        if matches!(relation, SpatialRelation::Near(_)) {
            expected.push("p_band");
        }
        let query = and(vec![
            Box::new(parks.clone()),
            fixture.join(Box::new(AllQuery), relation),
        ]);
        fixture.assert_names(query.as_ref(), &expected)?;
        let scores = fixture.scores(query.as_ref())?;
        expected.sort();
        assert_eq!(
            scores.keys().map(String::as_str).collect::<Vec<_>>(),
            expected
        );
        for (name, score) in scores {
            assert!(park_scores[&name] > 0.0);
            assert!((score - (park_scores[&name] + 1.0)).abs() < 1e-6);
        }
    }
    Ok(())
}

#[test]
fn test_parser_builds_spatial_inner_queries() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    for predicate in ["$within(1km, ", "$intersects("] {
        let nested = format!(
            "kind:park AND area:>=50 AND geometry:{predicate}$query(kind:trail AND \
             geometry:{predicate}$query(kind:road AND open:true))))"
        );
        let empty = format!("geometry:{predicate}$query())");
        for (text, expected) in [(nested, vec!["p_ok"]), (empty, Vec::new())] {
            let ast = query_grammar::parse_query(&text).unwrap();
            let from_text = parser.parse_query(&text).unwrap();
            fixture.assert_names(from_text.as_ref(), &expected)?;
            let from_ast = parser.build_query_from_user_input_ast(ast.clone()).unwrap();
            fixture.assert_names(from_ast.as_ref(), &expected)?;
            let (from_text, errors) = parser.parse_query_lenient(&text);
            assert!(errors.is_empty(), "{errors:?}");
            fixture.assert_names(from_text.as_ref(), &expected)?;
            let (from_ast, errors) = parser.build_query_from_user_input_ast_lenient(ast);
            assert!(errors.is_empty(), "{errors:?}");
            fixture.assert_names(from_ast.as_ref(), &expected)?;
        }
    }
    Ok(())
}

fn assert_parser_error(
    fixture: &Fixture,
    parser: &QueryParser,
    text: &str,
    error: QueryParserError,
    expected_names: &[&str],
) -> tantivy::Result<()> {
    let ast = query_grammar::parse_query(text).unwrap();
    assert_eq!(parser.parse_query(text).unwrap_err(), error);
    assert_eq!(
        parser
            .build_query_from_user_input_ast(ast.clone())
            .unwrap_err(),
        error
    );
    let (query, errors) = parser.parse_query_lenient(text);
    assert_eq!(errors.as_slice(), std::slice::from_ref(&error));
    fixture.assert_names(query.as_ref(), expected_names)?;
    let (query, errors) = parser.build_query_from_user_input_ast_lenient(ast);
    assert_eq!(errors.as_slice(), std::slice::from_ref(&error));
    fixture.assert_names(query.as_ref(), expected_names)
}

#[test]
fn test_parser_reports_inner_unknown_field() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    for predicate in ["$within(1km, ", "$intersects("] {
        for inner in [
            "unknown:trail".to_string(),
            format!("kind:trail AND geometry:{predicate}$query(unknown:road))"),
        ] {
            assert_parser_error(
                &fixture,
                &parser,
                &format!("kind:park AND geometry:{predicate}$query({inner}))"),
                QueryParserError::FieldDoesNotExist("unknown".to_string()),
                &[],
            )?;
        }
    }
    Ok(())
}

#[test]
fn test_parser_keeps_recovered_inner_query() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    for (predicate, expected) in [
        ("$within(1km, ", vec!["p_band", "p_hit", "p_ok", "p_small"]),
        ("$intersects(", vec!["p_hit", "p_ok", "p_small"]),
    ] {
        assert_parser_error(
            &fixture,
            &parser,
            &format!("kind:park AND geometry:{predicate}$query(kind:trail AND unknown:x))"),
            QueryParserError::FieldDoesNotExist("unknown".to_string()),
            &expected,
        )?;
    }
    Ok(())
}

#[test]
fn test_parser_validates_negative_inner_root() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    for (predicate, relation) in [
        ("$within(1km, ", SpatialRelation::Near(ONE_KM)),
        ("$intersects(", SpatialRelation::Intersects),
    ] {
        let reference = SpatialExecutor::new(PlanNode::Join {
            field: fixture.geometry,
            outer: Box::new(PlanNode::Query(fixture.parks())),
            inner: Box::new(PlanNode::Query(Box::new(BooleanQuery::new(vec![
                (Occur::Should, Box::new(AllQuery)),
                (Occur::MustNot, term(fixture.kind, "road")),
            ])))),
            relation: relation.clone(),
        });
        let expected = fixture.names(&reference)?;
        assert!(expected.iter().any(|name| name == "p_ok"));
        if matches!(relation, SpatialRelation::Near(_)) {
            // Every park, including p_far, is in the inner input and is near itself.
            fixture.assert_names(&reference, &["p_band", "p_far", "p_hit", "p_ok", "p_small"])?;
        }
        assert_parser_error(
            &fixture,
            &parser,
            &format!("kind:park AND geometry:{predicate}$query(-kind:road))"),
            QueryParserError::AllButQueryForbidden,
            &expected.iter().map(String::as_str).collect::<Vec<_>>(),
        )?;
    }
    Ok(())
}

#[test]
fn test_parser_separates_inner_syntax_and_resolution_errors() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let text = "kind:park AND geometry:$within(1km, $query(unknown:x AND kind:))";
    let (ast, grammar_errors) = query_grammar::parse_query_lenient(text);
    assert_eq!(
        grammar_errors,
        vec![query_grammar::LenientError {
            pos: 62,
            message: "expected word".to_string(),
        }]
    );
    assert_eq!(
        parser.parse_query(text).unwrap_err(),
        QueryParserError::SyntaxError(text.to_string())
    );
    assert_eq!(
        parser
            .build_query_from_user_input_ast(ast.clone())
            .unwrap_err(),
        QueryParserError::FieldDoesNotExist("unknown".to_string())
    );
    let (query, errors) = parser.parse_query_lenient(text);
    assert_eq!(
        errors,
        vec![
            QueryParserError::SyntaxError("expected word at position 62".to_string()),
            QueryParserError::FieldDoesNotExist("unknown".to_string()),
        ]
    );
    fixture.assert_names(query.as_ref(), &[])?;
    let (query, errors) = parser.build_query_from_user_input_ast_lenient(ast);
    assert_eq!(
        errors,
        vec![QueryParserError::FieldDoesNotExist("unknown".to_string())]
    );
    fixture.assert_names(query.as_ref(), &[])
}

#[test]
fn test_parser_rejects_unsupported_spatial_forms() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let parks = ["p_band", "p_far", "p_hit", "p_ok", "p_small"];
    for (spatial, message) in [
        (
            "$contains($query(unknown:trail))",
            "$contains joins are not supported.",
        ),
        (
            "$between(1km, 2km, $query(unknown:trail))",
            "$between is not supported.",
        ),
        ("$between(1km, 2km, 1 2)", "$between is not supported."),
        ("$knn(3, 1 2)", "$knn is not supported."),
    ] {
        assert_parser_error(
            &fixture,
            &parser,
            &format!("kind:park AND geometry:{spatial}"),
            QueryParserError::UnsupportedQuery(message.to_string()),
            &parks,
        )?;
    }
    assert_parser_error(
        &fixture,
        &parser,
        "kind:park AND missing:$contains($query(unknown:x))",
        QueryParserError::FieldDoesNotExist("missing".to_string()),
        &parks,
    )?;
    Ok(())
}

#[test]
fn test_parser_validates_programmatic_spatial_forms() -> tantivy::Result<()> {
    use query_grammar::{SpatialPredicateKind, UserInputAst, UserInputLeaf};

    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    for (predicate, coordinates, message) in [
        (
            SpatialPredicateKind::Knn(3),
            Vec::new(),
            "$knn is not supported.",
        ),
        (
            SpatialPredicateKind::Within(ONE_KM.into()),
            vec![(1.0.into(), 2.0.into())],
            "Spatial joins cannot also specify coordinates.",
        ),
    ] {
        let ast = UserInputAst::and(vec![
            query_grammar::parse_query("kind:park").unwrap(),
            UserInputLeaf::Spatial {
                field: Some("geometry".to_string()),
                predicate,
                coordinates,
                inner_query: Some(Box::new(query_grammar::parse_query("kind:trail").unwrap())),
            }
            .into(),
        ]);
        assert_eq!(
            parser
                .build_query_from_user_input_ast(ast.clone())
                .unwrap_err(),
            QueryParserError::UnsupportedQuery(message.to_string())
        );
        let (query, errors) = parser.build_query_from_user_input_ast_lenient(ast);
        assert_eq!(
            errors,
            vec![QueryParserError::UnsupportedQuery(message.to_string())]
        );
        fixture.assert_names(
            query.as_ref(),
            &["p_band", "p_far", "p_hit", "p_ok", "p_small"],
        )?;
    }
    Ok(())
}

#[test]
fn test_parser_applies_fuzzy_settings_to_inner_query() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    for (predicate, relation, expected) in [
        (
            "$within(1km, ",
            SpatialRelation::Near(ONE_KM),
            vec!["p_band", "p_hit", "p_small"],
        ),
        (
            "$intersects(",
            SpatialRelation::Intersects,
            vec!["p_hit", "p_small"],
        ),
    ] {
        let mut parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        let text = format!("kind:park AND geometry:{predicate}$query(kind:trai AND open:true))");
        let ast = query_grammar::parse_query(&text).unwrap();
        let exact = parser.parse_query(&text).unwrap();
        fixture.assert_names(exact.as_ref(), &[])?;
        parser.set_field_fuzzy(fixture.kind, false, 1, true);
        let reference = fixture.join(fixture.parks(), relation);
        fixture.assert_names(reference.as_ref(), &expected)?;
        let parsed = parser.parse_query(&text).unwrap();
        fixture.assert_names(parsed.as_ref(), &expected)?;
        let built = parser.build_query_from_user_input_ast(ast.clone()).unwrap();
        fixture.assert_names(built.as_ref(), &expected)?;
        let (parsed, errors) = parser.parse_query_lenient(&text);
        assert!(errors.is_empty(), "{errors:?}");
        fixture.assert_names(parsed.as_ref(), &expected)?;
        let (built, errors) = parser.build_query_from_user_input_ast_lenient(ast);
        assert!(errors.is_empty(), "{errors:?}");
        fixture.assert_names(built.as_ref(), &expected)?;
    }
    Ok(())
}

#[test]
fn test_parser_nested_negation_preserves_positive_scores() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let park_scores = fixture.scores(fixture.parks().as_ref())?;
    let parks = ["p_band", "p_far", "p_hit", "p_ok", "p_small"];
    for (text, boost, expected) in [
        ("kind:park AND NOT open:true", 1.0, parks.as_slice()),
        ("+kind:park +(-open:true)", 1.0, parks.as_slice()),
        ("kind:park AND (NOT open:true)^3", 1.0, parks.as_slice()),
        ("kind:park^3 AND NOT open:true", 3.0, parks.as_slice()),
        (
            "kind:park AND (-open:true -kind:road)",
            1.0,
            parks.as_slice(),
        ),
        (
            "kind:park AND NOT NOT area:>=50",
            1.0,
            ["p_band", "p_far", "p_hit", "p_ok"].as_slice(),
        ),
    ] {
        for query in parser_queries(&parser, text) {
            fixture.assert_names(query.as_ref(), expected)?;
            let scores = fixture.scores(query.as_ref())?;
            assert_eq!(scores.len(), expected.len(), "{text}");
            for name in expected {
                assert!(
                    (scores[*name] - park_scores[*name] * boost).abs() < 1e-6,
                    "{text}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn test_parser_negative_groups_with_optional_terms() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let park_scores = fixture.scores(fixture.parks().as_ref())?;
    let open_scores = fixture.scores(&TermQuery::new(
        Term::from_field_bool(fixture.open, true),
        IndexRecordOption::Basic,
    ))?;
    let closed = [
        "n_hit", "p_band", "p_far", "p_hit", "p_ok", "p_small", "t_closed",
    ];
    for (text, positive_scores, expected) in [
        (
            "kind:park OR NOT open:true",
            &park_scores,
            closed.as_slice(),
        ),
        (
            "(NOT open:true)^3 OR kind:park",
            &park_scores,
            closed.as_slice(),
        ),
        (
            "+(-kind:road) open:true",
            &open_scores,
            [
                "p_band", "p_far", "p_hit", "p_ok", "p_small", "t_closed", "t_open",
            ]
            .as_slice(),
        ),
        (
            "kind:park OR (-open:true -kind:road)",
            &park_scores,
            ["p_band", "p_far", "p_hit", "p_ok", "p_small", "t_closed"].as_slice(),
        ),
    ] {
        for query in parser_queries(&parser, text) {
            fixture.assert_names(query.as_ref(), expected)?;
            let scores = fixture.scores(query.as_ref())?;
            assert_eq!(scores.len(), expected.len(), "{text}");
            for name in expected {
                let expected_score = positive_scores.get(*name).copied().unwrap_or(0.0);
                assert!(
                    (scores[*name] - expected_score).abs() < 1e-6,
                    "{text}: {name}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn test_parser_negative_roots_keep_existing_contract() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let expected = [
        "p_band", "p_far", "p_hit", "p_ok", "p_small", "t_closed", "t_open",
    ];
    for (text, expected_score) in [
        ("-kind:road", 1.0),
        ("NOT kind:road", 1.0),
        ("(-kind:road)^3", 3.0),
    ] {
        let ast = query_grammar::parse_query(text).unwrap();
        assert_eq!(
            parser.parse_query(text).unwrap_err(),
            QueryParserError::AllButQueryForbidden
        );
        assert_eq!(
            parser
                .build_query_from_user_input_ast(ast.clone())
                .unwrap_err(),
            QueryParserError::AllButQueryForbidden
        );
        for (query, errors) in [
            parser.parse_query_lenient(text),
            parser.build_query_from_user_input_ast_lenient(ast),
        ] {
            assert_eq!(errors, [QueryParserError::AllButQueryForbidden]);
            fixture.assert_names(query.as_ref(), &expected)?;
            let scores = fixture.scores(query.as_ref())?;
            assert_eq!(scores.len(), expected.len());
            assert!(
                scores.values().all(|score| *score == expected_score),
                "{text}"
            );
        }
    }
    let excluded = BooleanQuery::new(vec![(Occur::MustNot, term(fixture.kind, "road"))]);
    fixture.assert_names(&excluded, &[])?;
    assert!(fixture.scores(&excluded)?.is_empty());
    Ok(())
}

#[test]
fn test_parser_composes_empty_asts() -> tantivy::Result<()> {
    use query_grammar::UserInputAst;

    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let park = query_grammar::parse_query("kind:park").unwrap();
    let empty = UserInputAst::empty_query();
    let park_scores = fixture.scores(fixture.parks().as_ref())?;
    let parks = ["p_band", "p_far", "p_hit", "p_ok", "p_small"];
    let all = [
        "n_hit", "p_band", "p_far", "p_hit", "p_ok", "p_small", "r_open", "t_closed", "t_open",
    ];
    let not_empty = empty.clone().unary(Occur::MustNot);
    for (ast, expected) in [
        (empty.clone(), [].as_slice()),
        (
            UserInputAst::Boost(Box::new(empty.clone()), 3.0.into()),
            [].as_slice(),
        ),
        (
            UserInputAst::and(vec![park.clone(), empty.clone()]),
            [].as_slice(),
        ),
        (
            UserInputAst::and(vec![empty.clone(), park.clone()]),
            [].as_slice(),
        ),
        (
            UserInputAst::or(vec![park.clone(), empty.clone()]),
            parks.as_slice(),
        ),
        (
            UserInputAst::or(vec![empty, park.clone()]),
            parks.as_slice(),
        ),
        (
            UserInputAst::and(vec![park.clone(), not_empty.clone()]),
            parks.as_slice(),
        ),
        (
            UserInputAst::or(vec![park.clone(), not_empty.clone()]),
            all.as_slice(),
        ),
        (
            UserInputAst::and(vec![park, not_empty.unary(Occur::MustNot)]),
            [].as_slice(),
        ),
    ] {
        let strict = parser.build_query_from_user_input_ast(ast.clone()).unwrap();
        let (lenient, errors) = parser.build_query_from_user_input_ast_lenient(ast);
        assert!(errors.is_empty(), "{errors:?}");
        for query in [strict, lenient] {
            fixture.assert_names(query.as_ref(), expected)?;
            let scores = fixture.scores(query.as_ref())?;
            assert_eq!(scores.len(), expected.len());
            for name in expected {
                let expected_score = park_scores.get(*name).copied().unwrap_or(0.0);
                assert!((scores[*name] - expected_score).abs() < 1e-6);
            }
        }
    }
    Ok(())
}

#[test]
fn test_parser_keeps_empty_spatial_inputs() -> tantivy::Result<()> {
    use query_grammar::{SpatialPredicateKind, UserInputAst, UserInputLeaf};

    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    for (predicate, text) in [
        (
            SpatialPredicateKind::Within(ONE_KM.into()),
            "$within(1km, $query())",
        ),
        (SpatialPredicateKind::Intersects, "$intersects($query())"),
    ] {
        let text = format!("kind:park AND geometry:{text}");
        for query in parser_queries(&parser, &text) {
            fixture.assert_names(query.as_ref(), &[])?;
        }
        let inner = UserInputAst::and(vec![
            query_grammar::parse_query("kind:trail").unwrap(),
            UserInputAst::empty_query(),
        ]);
        let spatial = UserInputLeaf::Spatial {
            field: Some("geometry".to_owned()),
            predicate,
            coordinates: Vec::new(),
            inner_query: Some(Box::new(inner)),
        };
        let ast = UserInputAst::and(vec![
            query_grammar::parse_query("kind:park").unwrap(),
            spatial.into(),
        ]);
        let strict = parser.build_query_from_user_input_ast(ast.clone()).unwrap();
        let (lenient, errors) = parser.build_query_from_user_input_ast_lenient(ast);
        assert!(errors.is_empty(), "{errors:?}");
        for query in [strict, lenient] {
            fixture.assert_names(query.as_ref(), &[])?;
            assert!(fixture.scores(query.as_ref())?.is_empty());
        }
    }
    Ok(())
}

#[test]
fn test_parser_retains_empty_groups_from_grammar_recovery() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let parks = ["p_band", "p_far", "p_hit", "p_ok", "p_small"];
    let park_scores = fixture.scores(fixture.parks().as_ref())?;
    for (text, position, expected) in [
        ("kind:park AND (open:)", 20, [].as_slice()),
        ("kind:park OR (open:)", 19, parks.as_slice()),
        ("kind:park AND NOT ()", 19, parks.as_slice()),
    ] {
        let (ast, grammar_errors) = query_grammar::parse_query_lenient(text);
        assert_eq!(grammar_errors.len(), 1);
        assert_eq!(
            parser.parse_query(text).unwrap_err(),
            QueryParserError::SyntaxError(text.to_owned())
        );
        let (from_text, errors) = parser.parse_query_lenient(text);
        assert_eq!(
            errors,
            [QueryParserError::SyntaxError(format!(
                "expected word at position {position}"
            ))]
        );
        let strict_ast = parser.build_query_from_user_input_ast(ast.clone()).unwrap();
        let (lenient_ast, errors) = parser.build_query_from_user_input_ast_lenient(ast);
        assert!(errors.is_empty(), "{errors:?}");
        for query in [from_text, strict_ast, lenient_ast] {
            fixture.assert_names(query.as_ref(), expected)?;
            let scores = fixture.scores(query.as_ref())?;
            assert_eq!(scores.len(), expected.len());
            for name in expected {
                assert!((scores[*name] - park_scores[*name]).abs() < 1e-6);
            }
        }
    }
    Ok(())
}

#[test]
fn test_parser_omits_failed_clauses_during_recovery() -> tantivy::Result<()> {
    let fixture = Fixture::new(false)?;
    let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
    let parks = ["p_band", "p_far", "p_hit", "p_ok", "p_small"];
    for text in [
        "kind:park AND missing:x",
        "kind:park OR missing:x",
        "kind:park AND NOT missing:x",
    ] {
        assert_parser_error(
            &fixture,
            &parser,
            text,
            QueryParserError::FieldDoesNotExist("missing".to_owned()),
            &parks,
        )?;
    }
    let (query, errors) =
        parser.parse_query_lenient("kind:park AND geometry:$within(oops, $query(kind:trail))");
    assert!(!errors.is_empty());
    fixture.assert_names(query.as_ref(), &parks)?;
    Ok(())
}

mod boolean_regressions {
    use query_grammar::{SpatialPredicateKind, UserInputAst, UserInputLeaf};
    use tantivy::query::QueryParser;

    use super::*;

    fn join_ast(relation: &SpatialRelation) -> UserInputAst {
        let predicate = match relation {
            SpatialRelation::Near(radius) => SpatialPredicateKind::Within((*radius).into()),
            SpatialRelation::Intersects => SpatialPredicateKind::Intersects,
            _ => unreachable!(),
        };
        UserInputLeaf::Spatial {
            field: Some("geometry".to_owned()),
            predicate,
            coordinates: Vec::new(),
            inner_query: Some(Box::new(ast("kind:trail AND open:true"))),
        }
        .into()
    }

    fn join_text(relation: &SpatialRelation) -> &'static str {
        match relation {
            SpatialRelation::Near(_) => "geometry:$within(1km, $query(kind:trail AND open:true))",
            SpatialRelation::Intersects => "geometry:$intersects($query(kind:trail AND open:true))",
            _ => unreachable!(),
        }
    }

    fn ast(text: &str) -> UserInputAst {
        query_grammar::parse_query(text).unwrap()
    }

    fn assert_ast_and_text(
        fixture: &Fixture,
        input: UserInputAst,
        text: &str,
        reference: &dyn Query,
    ) -> tantivy::Result<()> {
        assert!(ast(text) == input);
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for query in parser_queries(&parser, text) {
            assert_same_query(fixture, query.as_ref(), reference)?;
        }
        Ok(())
    }

    fn assert_same_query(
        fixture: &Fixture,
        query: &dyn Query,
        reference: &dyn Query,
    ) -> tantivy::Result<()> {
        assert_eq!(fixture.names(query)?, fixture.names(reference)?);
        let actual = fixture.scores(query)?;
        let expected = fixture.scores(reference)?;
        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        );
        for (name, score) in expected {
            assert!(
                (actual[&name] - score).abs() < 1e-6,
                "{name}: parsed {}, expected {score}",
                actual[&name]
            );
        }
        Ok(())
    }

    #[test]
    fn test_parser_keeps_nested_outer_predicates() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let input = UserInputAst::and(vec![
                ast("area:[50 TO *]"),
                UserInputAst::and(vec![ast("kind:park"), join_ast(&relation)]),
            ]);
            let text = format!(
                "area:[50 TO *] AND (kind:park AND {})",
                join_text(&relation)
            );
            let mut expected = vec!["p_hit"];
            if matches!(relation, SpatialRelation::Near(_)) {
                expected.push("p_band");
            }
            let reference = and(vec![
                fixture.large(),
                fixture.parks(),
                fixture.join(Box::new(AllQuery), relation),
            ]);
            fixture.assert_names(reference.as_ref(), &expected)?;
            let strict = parser.build_query_from_user_input_ast(input.clone())?;
            let (lenient, errors) = parser.build_query_from_user_input_ast_lenient(input);
            assert!(errors.is_empty(), "{errors:?}");
            for query in [strict, lenient]
                .into_iter()
                .chain(parser_queries(&parser, &text))
            {
                assert_same_query(&fixture, query.as_ref(), reference.as_ref())?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_parser_preserves_join_union() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let input = UserInputAst::or(vec![ast("kind:park"), join_ast(&relation)]);
            let text = format!("kind:park OR {}", join_text(&relation));
            let reference = BooleanQuery::new(vec![
                (Occur::Should, fixture.parks()),
                (Occur::Should, fixture.join(Box::new(AllQuery), relation)),
            ]);
            assert_ast_and_text(&fixture, input, &text, &reference)?;
        }
        Ok(())
    }

    #[test]
    fn test_parser_preserves_prohibited_join() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let input = UserInputAst::Clause(vec![
                (Some(Occur::Must), ast("kind:park")),
                (Some(Occur::MustNot), join_ast(&relation)),
            ]);
            let text = format!("kind:park AND -{}", join_text(&relation));
            let reference = BooleanQuery::new(vec![
                (Occur::Must, fixture.parks()),
                (Occur::MustNot, fixture.join(Box::new(AllQuery), relation)),
            ]);
            assert_ast_and_text(&fixture, input, &text, &reference)?;
        }
        Ok(())
    }

    #[test]
    fn test_parser_preserves_prohibited_sibling() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let input = UserInputAst::Clause(vec![
                (Some(Occur::MustNot), ast("kind:road")),
                (Some(Occur::Must), join_ast(&relation)),
            ]);
            let text = format!("-kind:road +{}", join_text(&relation));
            let reference = BooleanQuery::new(vec![
                (Occur::MustNot, term(fixture.kind, "road")),
                (Occur::Must, fixture.join(Box::new(AllQuery), relation)),
            ]);
            assert_ast_and_text(&fixture, input, &text, &reference)?;
        }
        Ok(())
    }

    #[test]
    fn test_parser_preserves_negated_join_group() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let input = UserInputAst::and(vec![
                ast("kind:park"),
                join_ast(&relation).unary(Occur::MustNot),
            ]);
            let text = format!("kind:park AND NOT {}", join_text(&relation));
            let reference = BooleanQuery::new(vec![
                (Occur::Must, fixture.parks()),
                (Occur::MustNot, fixture.join(Box::new(AllQuery), relation)),
            ]);
            assert_ast_and_text(&fixture, input, &text, &reference)?;
        }
        Ok(())
    }

    #[test]
    fn test_parser_keeps_outer_predicate_around_union() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let text = format!("area:[50 TO *] AND (kind:park OR {})", join_text(&relation));
            let union = BooleanQuery::new(vec![
                (Occur::Should, fixture.parks()),
                (Occur::Should, fixture.join(Box::new(AllQuery), relation)),
            ]);
            let reference = and(vec![fixture.large(), Box::new(union)]);
            fixture.assert_names(
                reference.as_ref(),
                &["n_hit", "p_band", "p_far", "p_hit", "p_ok"],
            )?;
            for query in parser_queries(&parser, &text) {
                assert_same_query(&fixture, query.as_ref(), reference.as_ref())?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_parser_preserves_outer_score_contribution() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let text = format!("kind:park^3 AND {}", join_text(&relation));
            let reference = and(vec![
                Box::new(BoostQuery::new(fixture.parks(), 3.0)),
                fixture.join(Box::new(AllQuery), relation),
            ]);
            for query in parser_queries(&parser, &text) {
                assert_same_query(&fixture, query.as_ref(), reference.as_ref())?;
            }
        }
        Ok(())
    }

    fn join_with_inner(
        fixture: &Fixture,
        inner: Box<dyn Query>,
        relation: SpatialRelation,
    ) -> Box<dyn Query> {
        Box::new(SpatialExecutor::new(PlanNode::Join {
            field: fixture.geometry,
            outer: Box::new(PlanNode::Query(Box::new(AllQuery))),
            inner: Box::new(PlanNode::Query(inner)),
            relation,
        }))
    }

    #[test]
    fn test_parser_combines_multiple_joins() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let trail_text = join_text(&relation);
            let road_text = trail_text.replace("kind:trail AND open:true", "kind:road");
            let trail = fixture.join(Box::new(AllQuery), relation.clone());
            let road = join_with_inner(&fixture, term(fixture.kind, "road"), relation.clone());
            let conjunction = and(vec![fixture.parks(), trail.box_clone(), road.box_clone()]);
            let union = and(vec![
                fixture.parks(),
                Box::new(BooleanQuery::union(vec![trail, road])),
            ]);
            let mut union_names = vec!["p_hit", "p_ok", "p_small"];
            if matches!(relation, SpatialRelation::Near(_)) {
                union_names.push("p_band");
            }
            fixture.assert_names(conjunction.as_ref(), &["p_hit", "p_small"])?;
            fixture.assert_names(union.as_ref(), &union_names)?;
            for (text, reference) in [
                (
                    format!("kind:park AND {trail_text} AND {road_text}"),
                    conjunction,
                ),
                (
                    format!("kind:park AND ({trail_text} OR {road_text})"),
                    union,
                ),
            ] {
                for query in parser_queries(&parser, &text) {
                    assert_same_query(&fixture, query.as_ref(), reference.as_ref())?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_parser_keeps_join_filters_in_or_branches() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let trail_text = join_text(&relation);
            let road_text = trail_text.replace("kind:trail", "kind:road");
            let mut expected = vec!["p_hit", "p_small", "t_closed"];
            if matches!(relation, SpatialRelation::Near(_)) {
                expected.push("p_band");
            }
            let reference = BooleanQuery::union(vec![
                and(vec![
                    fixture.parks(),
                    fixture.join(Box::new(AllQuery), relation.clone()),
                ]),
                and(vec![
                    term(fixture.kind, "trail"),
                    join_with_inner(&fixture, fixture.open_features("road"), relation),
                ]),
            ]);
            fixture.assert_names(&reference, &expected)?;
            let text = format!("(kind:park AND {trail_text}) OR (kind:trail AND {road_text})");
            for query in parser_queries(&parser, &text) {
                assert_same_query(&fixture, query.as_ref(), &reference)?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_parser_preserves_optional_and_boosted_join_scores() -> tantivy::Result<()> {
        use tantivy::query::ConstScoreQuery;

        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for relation in [SpatialRelation::Near(ONE_KM), SpatialRelation::Intersects] {
            let join_text = join_text(&relation);
            let join = fixture.join(Box::new(AllQuery), relation);
            let complement = BooleanQuery::new(vec![
                (Occur::MustNot, join.box_clone()),
                (
                    Occur::Must,
                    Box::new(ConstScoreQuery::new(Box::new(AllQuery), 0.0)),
                ),
            ]);
            let cases: Vec<(String, Box<dyn Query>)> = vec![
                (
                    format!("+kind:park {join_text}"),
                    Box::new(BooleanQuery::new(vec![
                        (Occur::Must, fixture.parks()),
                        (Occur::Should, join.box_clone()),
                    ])),
                ),
                (
                    format!("+{join_text} kind:park"),
                    Box::new(BooleanQuery::new(vec![
                        (Occur::Must, join.box_clone()),
                        (Occur::Should, fixture.parks()),
                    ])),
                ),
                (
                    format!("kind:park AND ({join_text})^3"),
                    and(vec![
                        fixture.parks(),
                        Box::new(BoostQuery::new(join.box_clone(), 3.0)),
                    ]),
                ),
                (
                    format!("(kind:park AND {join_text})^3"),
                    Box::new(BoostQuery::new(
                        and(vec![fixture.parks(), join.box_clone()]),
                        3.0,
                    )),
                ),
                (
                    format!("kind:park OR (NOT {join_text})^3"),
                    Box::new(BooleanQuery::union(vec![
                        fixture.parks(),
                        Box::new(BoostQuery::new(Box::new(complement), 3.0)),
                    ])),
                ),
                (
                    format!("kind:park AND NOT NOT {join_text}"),
                    and(vec![
                        fixture.parks(),
                        Box::new(ConstScoreQuery::new(join, 0.0)),
                    ]),
                ),
            ];
            for (text, reference) in cases {
                assert!(!fixture.names(reference.as_ref())?.is_empty(), "{text}");
                for query in parser_queries(&parser, &text) {
                    assert_same_query(&fixture, query.as_ref(), reference.as_ref())?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_parser_keeps_ordinary_boolean_scores() -> tantivy::Result<()> {
        let fixture = Fixture::new(false)?;
        let parser = QueryParser::for_index(fixture.searcher.index(), Vec::new());
        for (text, occurs, boost, count) in [
            (
                "kind:park open:false",
                [Occur::Should, Occur::Should],
                1.0,
                7,
            ),
            (
                "+kind:park open:false",
                [Occur::Must, Occur::Should],
                1.0,
                5,
            ),
            (
                "kind:park AND open:false",
                [Occur::Must, Occur::Must],
                1.0,
                5,
            ),
            (
                "kind:park OR open:false",
                [Occur::Should, Occur::Should],
                1.0,
                7,
            ),
            (
                "(kind:park open:false)^2",
                [Occur::Should, Occur::Should],
                2.0,
                7,
            ),
        ] {
            let closed = Box::new(TermQuery::new(
                Term::from_field_bool(fixture.open, false),
                IndexRecordOption::Basic,
            ));
            let reference =
                BooleanQuery::new(vec![(occurs[0], fixture.parks()), (occurs[1], closed)]);
            let reference = BoostQuery::new(Box::new(reference), boost);
            assert_eq!(fixture.names(&reference)?.len(), count);
            for query in parser_queries(&parser, text) {
                assert_same_query(&fixture, query.as_ref(), &reference)?;
            }
        }
        Ok(())
    }
}
