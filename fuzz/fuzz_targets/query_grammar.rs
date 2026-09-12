#![no_main]

//! Fuzzes the query-grammar string parser.
//!
//! Query strings come from untrusted end-users, so parsing must never panic.
//! The parser is security-relevant because malformed, crafted, or extremely large
//! queries from adversaries could trigger unbounded memory consumption or CPU time.

use libfuzzer_sys::fuzz_target;
use tantivy_query_grammar::{parse_query, parse_query_lenient};

fuzz_target!(|data: &str| {
    // Test strict parser: Err is a legitimate, expected outcome for garbage input.
    if let Ok(ast) = parse_query(data) {
        // For successful parses, exercise the Debug impl to catch panics there.
        let _formatted = format!("{ast:?}");
    }

    // Test lenient parser: always returns an AST (may have errors attached).
    // Exercise the Debug impl of the AST and error list.
    let (ast, errors) = parse_query_lenient(data);
    let _formatted_ast = format!("{ast:?}");
    let _formatted_errors = format!("{errors:?}");
});
