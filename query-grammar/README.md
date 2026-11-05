# Tantivy Query Grammar

This crate is used by tantivy to parse queries.

Spatial joins accept a query argument, for example
`geometry:$within(1km, $query(kind:trail AND open:true))`. The `inner_query`
field of `UserInputLeaf::Spatial` contains `Option<Box<UserInputAst>>`.
Callers can inspect and modify that child before building an executable query.
It is serialized as a nested AST object; earlier versions stored query text.
Literal geometry has `inner_query: None`, serialized as `null`.

Both parser entry points parse query arguments recursively. Strict parsing
accepts whitespace between a function name and its opening parenthesis and
rejects malformed spatial calls. Lenient parsing reports errors and preserves
the inner query when its boundary can be identified. If invalid parameters or
an unfinished expression prevent finding that boundary, it discards the
malformed call and the remaining input. When only the call's closing parenthesis
is missing after a complete query argument, later clauses can still be parsed.

The grammar recognizes spatial syntax independently of execution support.
Tantivy's query builder accepts distance and intersection joins, and literal
distance, intersection, and containment predicates. Recognized but unimplemented
predicates return `UnsupportedQuery` during resolution.

A spatial join remains a Boolean clause, including when it is optional or
prohibited. Required ordinary predicates restrict its outer input before
spatial evaluation and retain their scores in the containing Boolean query.
Each inner query argument is resolved and evaluated as an independent input.

An empty user AST matches no documents, including when it is a child of a
Boolean group. This also applies to an empty `Clause` supplied directly to a
query builder. Terms omitted during tokenization or query resolution are still
discarded. An empty group retained by lenient grammar recovery has the same
meaning as an explicit empty AST. For example, `kind:park AND (open:)` reports
a syntax error and matches nothing. A query builder given that recovered AST
sees its empty child but cannot report the original grammar error.

The [spatial query rewrite example](../examples/spatial_query_rewrite.rs) applies
an ordinary predicate to the outermost query and every nested spatial input.
Run it from the repository root with `cargo run --example spatial_query_rewrite`.
