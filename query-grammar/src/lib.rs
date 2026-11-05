#![allow(clippy::derive_partial_eq_without_eq)]

use serde::Serialize;

mod infallible;
mod occur;
mod query_grammar;
mod user_input_ast;

pub use crate::infallible::LenientError;
pub use crate::occur::Occur;
use crate::query_grammar::{parse_to_ast, parse_to_ast_lenient};
pub use crate::user_input_ast::{
    Delimiter, SpatialPredicateKind, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Error;

/// Parse a query
pub fn parse_query(query: &str) -> Result<UserInputAst, Error> {
    let (_remaining, user_input_ast) = parse_to_ast(query).map_err(|_| Error)?;
    Ok(user_input_ast)
}

/// Parse a query, trying to recover from syntax errors, and giving hints toward fixing errors.
pub fn parse_query_lenient(query: &str) -> (UserInputAst, Vec<LenientError>) {
    parse_to_ast_lenient(query)
}

#[cfg(test)]
mod tests {
    use crate::{UserInputAst, parse_query, parse_query_lenient};

    #[test]
    fn test_deduplication() {
        let ast: UserInputAst = parse_query("a a").unwrap();
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"bool","clauses":[[null,{"type":"literal","field_name":null,"phrase":"a","delimiter":"none","slop":0,"prefix":false}]]}"#
        );
    }

    #[test]
    fn test_parse_query_serialization() {
        let ast = parse_query("title:hello OR title:x").unwrap();
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"bool","clauses":[["should",{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}],["should",{"type":"literal","field_name":"title","phrase":"x","delimiter":"none","slop":0,"prefix":false}]]}"#
        );
    }

    #[test]
    fn test_parse_query_wrong_query() {
        assert!(parse_query("title:").is_err());
    }

    #[test]
    fn test_spatial_intersects() {
        // Test simple case first.
        let ast = parse_query(r#"geo:$intersects(1.0 2.0, 3.0 4.0, 5.0 6.0)"#).unwrap();
        let debug = format!("{ast:?}");
        assert!(debug.contains("$intersects"), "got: {debug}");
    }

    #[test]
    fn test_spatial_contains() {
        let ast = parse_query(r#"geo:$contains(1.0 2.0, 3.0 4.0, 5.0 6.0)"#).unwrap();
        let debug = format!("{ast:?}");
        assert!(debug.contains("$contains"), "got: {debug}");
    }

    #[test]
    fn test_spatial_too_few_coords() {
        assert!(parse_query(r#"geo:$intersects(1.0 2.0, 3.0 4.0)"#).is_err());
    }

    #[test]
    fn test_parse_query_lenient_wrong_query() {
        let (_, errors) = parse_query_lenient("title:");
        assert!(errors.len() == 1);
        let json = serde_json::to_string(&errors).unwrap();
        assert_eq!(json, r#"[{"pos":6,"message":"expected word"}]"#);
    }

    fn assert_spatial_parses(text: &str) -> UserInputAst {
        let strict = parse_query(text).unwrap();
        let (lenient, errors) = parse_query_lenient(text);
        assert_eq!(strict, lenient, "{text}");
        assert!(errors.is_empty(), "{text}: {errors:?}");
        strict
    }

    fn inner_query(ast: &UserInputAst) -> &UserInputAst {
        let UserInputAst::Leaf(leaf) = ast else {
            panic!("expected spatial leaf: {ast:?}");
        };
        let crate::UserInputLeaf::Spatial {
            inner_query: Some(inner),
            ..
        } = leaf.as_ref()
        else {
            panic!("expected spatial join: {ast:?}");
        };
        inner
    }

    #[test]
    fn test_spatial_query_boundaries() {
        for inner in [
            r#"name:"trail ) east""#,
            "name:'trail ( west'",
            r#"name:"trail \" ) east""#,
            r"name:/[)]/",
            r"name:/trail(east|west)/",
            r"name:/path\/[)]/",
            "(kind:trail OR kind:road)^2 AND open:true",
        ] {
            let text = format!("geo:$within(1km, $query({inner}))");
            let ast = assert_spatial_parses(&text);
            assert_eq!(inner_query(&ast), &parse_query(inner).unwrap());
        }
    }

    #[test]
    fn test_spatial_nested_query() {
        let child = "kind:trail AND geo:$intersects($query(kind:road))";
        let ast = assert_spatial_parses(&format!("geo:$within(1km, $query({child}))"));
        assert_eq!(inner_query(&ast), &parse_query(child).unwrap());
        let UserInputAst::Clause(clauses) = inner_query(&ast) else {
            panic!("expected inner conjunction");
        };
        assert_eq!(
            inner_query(&clauses[1].1),
            &parse_query("kind:road").unwrap()
        );
    }

    #[test]
    fn test_spatial_empty_query() {
        for text in [
            "geo:$intersects($query())",
            "geo:$within(1km, $query( \n ))",
        ] {
            let ast = assert_spatial_parses(text);
            assert_eq!(inner_query(&ast), &UserInputAst::empty_query());
        }
    }

    #[test]
    fn test_spatial_query_marker_spacing() {
        for text in [
            "$intersects($query(kind:trail))",
            "$contains($query(kind:trail))",
            "$within(1km, $query(kind:trail))",
            "$between(1km, 2km, $query(kind:trail))",
            "$within(1km, $query())",
            "$within(1km, $query(geo:$intersects($query(kind:trail))))",
        ] {
            let expected = assert_spatial_parses(text);
            for marker in ["$query (", "$query \n ( \t"] {
                assert_eq!(
                    assert_spatial_parses(&text.replace("$query(", marker)),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_spatial_coordinate_forms() {
        use crate::{SpatialPredicateKind, UserInputLeaf};

        for (text, predicate, pairs) in [
            (
                "$intersects(1 2, 3 4, 5 6)",
                SpatialPredicateKind::Intersects,
                vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)],
            ),
            (
                "$contains (1 2, 3 4, 5 6)",
                SpatialPredicateKind::Contains,
                vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)],
            ),
            (
                "$within(0.5rad, -1 2)",
                SpatialPredicateKind::Within(0.5.into()),
                vec![(-1.0, 2.0)],
            ),
            (
                "$between(0.1rad, 0.5rad, 1 2)",
                SpatialPredicateKind::Between(0.1.into(), 0.5.into()),
                vec![(1.0, 2.0)],
            ),
            (
                "$knn(3, 1 2)",
                SpatialPredicateKind::Knn(3),
                vec![(1.0, 2.0)],
            ),
        ] {
            let ast = assert_spatial_parses(text);
            assert_eq!(
                ast,
                UserInputLeaf::Spatial {
                    field: None,
                    predicate,
                    coordinates: pairs
                        .into_iter()
                        .map(|(lon, lat)| (lon.into(), lat.into()))
                        .collect(),
                    inner_query: None,
                }
                .into()
            );
        }
        // Miles and feet retain their existing radius constants.
        for (distance, radians) in [
            ("1mi", 1.0 / 3958.8),
            ("1km", 1.0 / 6371.0),
            ("1m", 1.0 / 6_371_000.0),
            ("1ft", 1.0 / 20_902_464.0),
        ] {
            let ast = assert_spatial_parses(&format!("$within({distance}, 1 2)"));
            assert_eq!(
                ast,
                UserInputLeaf::Spatial {
                    field: None,
                    predicate: SpatialPredicateKind::Within(radians.into()),
                    coordinates: vec![(1.0.into(), 2.0.into())],
                    inner_query: None,
                }
                .into()
            );
        }
    }

    #[test]
    fn test_spatial_join_debug() {
        for (text, expected) in [
            (
                "$intersects($query(kind:trail))",
                "$intersects($query(\"kind\":trail))",
            ),
            (
                "$contains($query(kind:trail))",
                "$contains($query(\"kind\":trail))",
            ),
            (
                "$within(0.5rad, $query(kind:trail))",
                "$within(0.5rad, $query(\"kind\":trail))",
            ),
            (
                "$between(0.1rad, 0.5rad, $query(kind:trail))",
                "$between(0.1rad, 0.5rad, $query(\"kind\":trail))",
            ),
            (
                "$within(1rad, $query())",
                "$within(1rad, $query(<emptyclause>))",
            ),
        ] {
            let ast = assert_spatial_parses(text);
            assert_eq!(format!("{ast:?}"), expected);
        }
    }

    #[test]
    fn test_spatial_query_normalization() {
        let ast = assert_spatial_parses("geo:$within(1km, $query((kind:trail kind:trail)^2))^3");
        let UserInputAst::Boost(join, _) = ast else {
            panic!("expected boosted join");
        };
        let UserInputAst::Boost(child, _) = inner_query(&join) else {
            panic!("expected boosted inner query");
        };
        let UserInputAst::Clause(clauses) = child.as_ref() else {
            panic!("expected inner clause");
        };
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].1, parse_query("kind:trail").unwrap());

        let ast = assert_spatial_parses("kind:trail AND geo:$within(1km, $query(kind:trail))");
        let UserInputAst::Clause(clauses) = ast else {
            panic!("expected outer conjunction");
        };
        assert_eq!(clauses.len(), 2);
        assert_eq!(&clauses[0].1, inner_query(&clauses[1].1));
    }

    #[test]
    fn test_spatial_query_default_field() {
        let ast = assert_spatial_parses("title:(geo:$within(1km, $query(oak)))");
        assert_eq!(inner_query(&ast), &parse_query("oak").unwrap());
    }

    #[test]
    fn test_spatial_rejects_malformed_calls() {
        for text in [
            "$within(abc)",
            "$within(1km, $query(kind:))",
            "$within(1km, $query(kind:trail)",
            "$within(1km, $query(kind:trail",
            "$within(1km, $query(kind:trail ^oops))",
            "$intersects($query(kind:trail AND))",
            "$knn(3, $query(kind:trail))",
            "$knn(3, $query (kind:trail))",
            "$within(1km, 1 2, 3 4)",
            "$intersects(1 2, 3 4)",
        ] {
            assert!(parse_query(text).is_err(), "{text}");
        }
        for text in ["$word", "$within", "$withinExtra", "$within 1km"] {
            assert_spatial_parses(text);
        }
    }

    #[test]
    fn test_spatial_lenient_discards_unknown_boundary() {
        for (call, error_at, message) in [
            (
                "$within(abc, $query(kind:trail)) AND name:later",
                "abc",
                "invalid spatial parameters",
            ),
            (
                r#"$within(abc, $query(name:"trail ) east")) AND name:later"#,
                "abc",
                "invalid spatial parameters",
            ),
            (
                "$within(1km, invalid) AND name:later",
                "invalid",
                "invalid spatial coordinates",
            ),
            (
                "$within(1km, 1 2, 3 4) AND name:later",
                ", 3",
                "invalid spatial coordinates",
            ),
            (
                "$within(1km, $query(kind:trail ^oops)) AND name:later",
                "^oops",
                "expected ')' after $query",
            ),
            (
                "$within(1km, $query(kind:trail ^)) AND name:later",
                "^",
                "expected ')' after $query",
            ),
        ] {
            let text = format!("kind:park AND geo:{call}");
            let (ast, errors) = parse_query_lenient(&text);
            assert_eq!(ast, parse_query("kind:park").unwrap(), "{text}");
            assert_eq!(
                errors,
                vec![crate::LenientError {
                    pos: text.find(error_at).unwrap(),
                    message: message.to_string()
                }],
                "{text}"
            );
        }
    }

    #[test]
    fn test_spatial_lenient_child_error_position() {
        let text = "kind:park AND geo:$within(1km, $query(kind:))";
        let (ast, errors) = parse_query_lenient(text);
        assert_eq!(
            ast,
            parse_query("kind:park AND geo:$within(1km, $query())").unwrap()
        );
        assert_eq!(
            errors,
            vec![crate::LenientError {
                pos: text.find("))").unwrap(),
                message: "expected word".to_string()
            }]
        );
    }

    #[test]
    fn test_spatial_lenient_missing_delimiters() {
        for (suffix, expected_messages) in [
            (
                "kind:trail",
                vec![
                    "expected ')' after $query",
                    "expected ')' after spatial predicate",
                ],
            ),
            ("kind:trail)", vec!["expected ')' after spatial predicate"]),
        ] {
            let text = format!("kind:park AND geo:$within(1km, $query({suffix}");
            let (ast, errors) = parse_query_lenient(&text);
            assert_eq!(
                ast,
                parse_query("kind:park AND geo:$within(1km, $query(kind:trail))").unwrap()
            );
            let expected = expected_messages
                .into_iter()
                .map(|message| crate::LenientError {
                    pos: text.len(),
                    message: message.to_string(),
                })
                .collect::<Vec<_>>();
            assert_eq!(errors, expected);
        }
        let text = "kind:park AND geo:$within(1km, $query(kind:trail) AND open:true";
        let (ast, errors) = parse_query_lenient(text);
        assert_eq!(
            ast,
            parse_query("kind:park AND geo:$within(1km, $query(kind:trail)) AND open:true")
                .unwrap()
        );
        assert_eq!(
            errors,
            vec![crate::LenientError {
                pos: text.rfind("AND").unwrap(),
                message: "expected ')' after spatial predicate".to_string()
            }]
        );
    }
}
