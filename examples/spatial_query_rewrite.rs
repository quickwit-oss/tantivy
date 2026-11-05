// Each join searches a separate input. Restricting the final result leaves
// its inner queries unrestricted, so visit those inputs before building.
// An input is the root query or a join's query argument. Walk Boolean
// groups and boosts within an input without wrapping each subtree.
// QueryParser validates the rewritten AST. Validate the original first if
// it must also be accepted on its own.

use std::mem;

use tantivy::collector::DocSetCollector;
use tantivy::query::{QueryParser, QueryParserError};
use tantivy::query_grammar::{self, UserInputAst, UserInputLeaf};
use tantivy::schema::{Schema, Value, SPHERE, STORED, STRING};
use tantivy::{Index, IndexWriter, Searcher, TantivyDocument};

// The restriction is an ordinary predicate with no join inputs of its own.
fn restrict_input(input: &mut UserInputAst, restriction: &UserInputAst) {
    visit_subtree(input, restriction);
    let original = mem::replace(input, UserInputAst::empty_query());
    // Wrap the whole expression so its OR branches keep their meaning.
    *input = UserInputAst::and(vec![restriction.clone(), original]);
}

fn visit_subtree(ast: &mut UserInputAst, restriction: &UserInputAst) {
    match ast {
        UserInputAst::Clause(children) => {
            // Leave occurrences, including parser defaults, unchanged.
            for (_, child) in children {
                visit_subtree(child, restriction);
            }
        }
        UserInputAst::Boost(child, _) => visit_subtree(child, restriction),
        UserInputAst::Leaf(leaf) => {
            if let UserInputLeaf::Spatial {
                inner_query: Some(inner),
                ..
            } = leaf.as_mut()
            {
                restrict_input(inner, restriction);
            }
        }
    }
}

fn parse_ast(text: &str) -> Result<UserInputAst, QueryParserError> {
    query_grammar::parse_query(text).map_err(|_| QueryParserError::SyntaxError(text.to_owned()))
}

fn square(lon: f64, lat: f64) -> Vec<[f64; 2]> {
    vec![
        [lon - 0.002, lat - 0.002],
        [lon + 0.002, lat - 0.002],
        [lon + 0.002, lat + 0.002],
        [lon - 0.002, lat + 0.002],
        [lon - 0.002, lat - 0.002],
    ]
}

fn make_index() -> tantivy::Result<Index> {
    let mut builder = Schema::builder();
    builder.add_text_field("name", STORED);
    builder.add_text_field("kind", STRING);
    builder.add_text_field("dataset", STRING);
    builder.add_spatial_field("geometry", SPHERE);
    let schema = builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
    for (name, kind, dataset, lon, lat) in [
        ("park_good", "park", "current", 0.0, 0.0),
        ("trail_good", "trail", "current", 0.003, 0.001),
        ("road_good", "road", "current", 0.006, 0.002),
        ("park_old_trail", "park", "current", 1.0, 0.0),
        ("trail_old", "trail", "previous", 1.003, 0.001),
        ("road_near_old_trail", "road", "current", 1.006, 0.002),
        ("park_old_road", "park", "current", 2.0, 0.0),
        ("trail_near_old_road", "trail", "current", 2.003, 0.001),
        ("road_old", "road", "previous", 2.006, 0.002),
        ("park_previous", "park", "previous", 0.0005, 0.0005),
    ] {
        let json = serde_json::json!({
            "name": name,
            "kind": kind,
            "dataset": dataset,
            "geometry": {"type": "Polygon", "coordinates": [square(lon, lat)]},
        });
        writer.add_document(TantivyDocument::parse_json(&schema, &json.to_string())?)?;
    }
    writer.commit()?;
    Ok(index)
}

fn matching_names(
    parser: &QueryParser,
    searcher: &Searcher,
    ast: UserInputAst,
) -> tantivy::Result<Vec<String>> {
    let query = parser.build_query_from_user_input_ast(ast)?;
    let name = searcher.schema().get_field("name")?;
    let mut names = Vec::new();
    for address in searcher.search(query.as_ref(), &DocSetCollector)? {
        let doc: TantivyDocument = searcher.doc(address)?;
        names.push(doc.get_first(name).unwrap().as_str().unwrap().to_owned());
    }
    names.sort();
    Ok(names)
}

fn main() -> tantivy::Result<()> {
    let index = make_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, Vec::new());
    let restriction = parse_ast("dataset:current")?;
    let join =
        "geometry:$within(1km, $query(kind:trail AND geometry:$within(1km, $query(kind:road))))";
    let original = parse_ast(&format!("kind:park AND {join}"))?;
    assert_eq!(
        matching_names(&parser, &searcher, original.clone())?,
        [
            "park_good",
            "park_old_road",
            "park_old_trail",
            "park_previous"
        ]
    );

    let outer_only = UserInputAst::and(vec![restriction.clone(), original.clone()]);
    assert_eq!(
        matching_names(&parser, &searcher, outer_only)?,
        ["park_good", "park_old_road", "park_old_trail"]
    );

    let mut rewritten = original;
    restrict_input(&mut rewritten, &restriction);
    println!("Rewritten AST (diagnostic): {rewritten:?}");
    let names = matching_names(&parser, &searcher, rewritten)?;
    assert_eq!(names, ["park_good"]);
    println!("Matches: {names:?}");

    let mut negative = parse_ast(&format!("kind:park AND NOT {join}"))?;
    let outer_only = UserInputAst::and(vec![restriction.clone(), negative.clone()]);
    assert!(matching_names(&parser, &searcher, outer_only)?.is_empty());
    restrict_input(&mut negative, &restriction);
    assert_eq!(
        matching_names(&parser, &searcher, negative)?,
        ["park_old_road", "park_old_trail"]
    );

    for (text, expected) in [
        (
            "kind:park OR kind:trail".to_owned(),
            vec![
                "park_good",
                "park_old_road",
                "park_old_trail",
                "trail_good",
                "trail_near_old_road",
            ],
        ),
        (format!("(kind:park AND {join})^2"), vec!["park_good"]),
        (String::new(), Vec::new()),
        (
            "kind:park AND geometry:$within(1km, $query())".to_owned(),
            Vec::new(),
        ),
    ] {
        let mut ast = parse_ast(&text)?;
        restrict_input(&mut ast, &restriction);
        assert_eq!(matching_names(&parser, &searcher, ast)?, expected, "{text}");
    }
    Ok(())
}
