// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// - create an index in a directory
// - index a few documents into our index
// - search for the best document matching a basic query
// - retrieve the best document's original content.
// ---
// Importing tantivy...
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{FuzzyTermQuery, Query, QueryParser, QueryParserError, TermQuery};
use tantivy::{schema::*, Searcher};
use tantivy::{Index, ReloadPolicy};
use tempfile::TempDir;

fn make_json_term(
    query_string: &str,
    query_parser: &QueryParser,
) -> Result<Term, QueryParserError> {
    let json_query =
        query_parser.parse_query(query_string)?;
    let mut terms = Vec::new();
    json_query.query_terms(&mut |term, _| {
        println!("json term: {:?}", term);
        terms.push(term.clone());
    });

    return Ok(terms[0].clone());
}

fn execute_search_query(query: Box<dyn Query>, searcher: &Searcher) {
    let (_top_docs, count) = searcher
        .search(&query, &(TopDocs::with_limit(5), Count))
        .unwrap();
    println!("\n **Query matched: {} documents", count.to_string());
    // for (score, doc_address) in top_docs {
    //     let retrieved_doc = searcher.doc(doc_address);
    //     // Note that the score is not lower for the fuzzy hit.

    //     // There's an issue open for that: https://github.com/quickwit-oss/tantivy/issues/563
    //     println!("score {score:?} doc {}", schema.to_json(&retrieved_doc));
    //     // score 1.0 doc {"title":["The Diary of Muadib"]}
    //     //
    //     // score 1.0 doc {"title":["The Diary of a Young Girl"]}
    //     //
    //     // score 1.0 doc {"title":["A Dairy Cow"]}
    // }
}

fn main() -> tantivy::Result<()> {
    let index_path = TempDir::new()?;
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let _attributes = schema_builder.add_json_field("attributes", STORED | TEXT);

    let schema = schema_builder.build();
    let index = Index::create_in_dir(&index_path, schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;
    let doc = schema.parse_document(
        r#"{
        "title": "gone with the wind",
        "attributes": {
            "description": "the best vacuum cleaner ever"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = schema.parse_document(
        r#"{
        "title": "tale of two cities",
        "attributes": {
            "description": "once in a lifetime"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = schema.parse_document(
        r#"{
        "title": "dune",
        "attributes": {
            "description": "the quick brown fox"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;

    index_writer.commit()?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;
    let searcher = reader.searcher();

    // ### FuzzyTermQuery
    {
        let search_query = "attributes.description:brown";
        let query_parser = QueryParser::for_index(&index, vec![title]);

        let json_term = make_json_term(search_query, &query_parser)?;
        println!("json term field: {:?}", json_term.field());
        let json_query = FuzzyTermQuery::new(json_term.clone(), 1, true);

        let string_term = Term::from_field_text(title, "done");
        let string_query = FuzzyTermQuery::new(string_term, 1, true);

        let term_query = TermQuery::new(json_term.clone(), IndexRecordOption::Basic);

        println!("\n json fuzzy query");
        execute_search_query(Box::new(json_query), &searcher);
        println!("\n string fuzzy query");
        execute_search_query(Box::new(string_query), &searcher);
        println!("\n term query");
        execute_search_query(Box::new(term_query), &searcher);
    }

    Ok(())
}
