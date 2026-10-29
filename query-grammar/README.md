# Tantivy Query Grammar

This crate parses the text of a search query, such as
`title:"big wolf"~1 AND -body:fox`, into a syntax tree. It is the first step of
tantivy's [`QueryParser`](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html),
which documents the query language itself.

## What it does and does not do

The crate only knows the query *syntax*. It produces a
`UserInputAst`, a tree of clauses (`+`, `-`, `AND`,
`OR`), boosts (`^2.0`) and leaves: literals and phrases (with slop `~n` and
prefix `*`), ranges (`[a TO c}`), sets (`IN [a b]`), regexes, `field:*` and `*`.

It has no notion of an index. It does not check that field names exist, does
not tokenize or type-check values, and does not build `Query` objects. Those
steps happen in `tantivy::query::QueryParser`, which turns the `UserInputAst`
into a `LogicalAst` using the schema and tokenizers, and then into a `Query`:

```text
query string --(tantivy-query-grammar)--> UserInputAst --(QueryParser + schema)--> LogicalAst --> Box<dyn Query>
```

Keeping the grammar in its own crate lets it be reused (for instance by
[Quickwit](https://github.com/quickwit-oss/quickwit)) and keeps the parser
combinators (`nom`) out of the main crate.

## API

- `parse_query(&str) -> Result<UserInputAst, Error>` parses strictly and fails
  on any syntax error.
- `parse_query_lenient(&str) -> (UserInputAst, Vec<LenientError>)` always
  returns a tree, recovering from errors and reporting where they occurred,
  which is useful for queries typed by end users.

```rust
use tantivy_query_grammar::{parse_query, parse_query_lenient};

let ast = parse_query("title:\"big wolf\"~1 AND -body:fox").unwrap();
assert_eq!(format!("{ast:?}"), r#"(+"title":"big wolf"~1 -"body":fox)"#);

assert!(parse_query("title:(wolf").is_err());
let (ast, errors) = parse_query_lenient("title:(wolf");
assert_eq!(format!("{ast:?}"), r#""title":wolf"#);
assert_eq!(errors[0].message, "expected ')'");
```
