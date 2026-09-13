#![no_main]

//! Fuzzes [`QueryParser`], the entry point that turns a user-supplied query
//! string into a executable query against a real schema.
//!
//! Query strings are the classic untrusted input for a search engine: they
//! arrive straight from end users. On top of the raw grammar (covered by the
//! `query_grammar` target) this exercises field resolution, per-type value
//! parsing (text, u64, ranges) and the tokenizers invoked during parsing.
//! Rejecting a malformed query with an `Err` is correct; panicking is not.

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, FAST, INDEXED, TEXT};
use tantivy::Index;

/// Schema and index are fixed, so build them once rather than on every input:
/// re-creating them per iteration would dominate the runtime and starve the
/// fuzzer of executions.
struct Harness {
    index: Index,
    default_fields: Vec<Field>,
}

fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let body = schema_builder.add_text_field("body", TEXT);
        // A numeric field, so typed-value and range parsing stay reachable.
        // It has to be INDEXED as well as FAST: `compute_logical_ast_for_leaf`
        // rejects a non-indexed field with `FieldNotIndexed` before it ever
        // reaches `u64::from_str`, which would leave `count:123` and
        // `count:invalid` unable to exercise the numeric conversion. FAST keeps
        // the fast-field range path (`compute_boundary_term`) reachable too.
        schema_builder.add_u64_field("count", INDEXED | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        Harness {
            index,
            default_fields: vec![title, body],
        }
    })
}

fuzz_target!(|data: &str| {
    let harness = harness();
    let parser = QueryParser::for_index(&harness.index, harness.default_fields.clone());

    // An `Err` is the expected outcome for a malformed query, so it is ignored
    // here; the property under test is only that neither call panics.
    let _ = parser.parse_query(data);
    let _ = parser.parse_query_lenient(data);
});
