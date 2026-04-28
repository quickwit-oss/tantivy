// Benchmark for the query grammar parsing deeply nested queries.
//
// Regression guard for https://github.com/quickwit-oss/tantivy/issues/2498:
// at depth 20/21 the old parser took 0.87 s / 1.72 s respectively because
// `ast()` retried `occur_leaf` on backtrack, giving O(2^n) time. With the
// fix parsing is linear and completes in microseconds.
//
// Run with: `cargo bench --bench query_parser_nested`.

use binggan::{black_box, BenchRunner};
use tantivy::query_grammar::parse_query;

fn nested_query(depth: usize, leading_plus: bool) -> String {
    let leading = "(".repeat(depth);
    let trailing = ")".repeat(depth);
    let prefix = if leading_plus { "+" } else { "" };
    format!("{prefix}{leading}title:test{trailing}")
}

fn main() {
    let mut runner = BenchRunner::new();

    for depth in [20, 21] {
        for leading_plus in [false, true] {
            let query = nested_query(depth, leading_plus);
            let label = format!(
                "parse_nested_depth_{depth}_{}",
                if leading_plus { "plus" } else { "plain" },
            );
            runner.bench_function(&label, move |_| {
                black_box(parse_query(black_box(&query)).unwrap());
            });
        }
    }
}
