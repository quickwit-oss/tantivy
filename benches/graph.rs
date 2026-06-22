// Benchmarks the search workspace of src/vector/ivf/graph.rs.
//
// workspace/visited_check — the `Workspace.visited` bitset, exercised
// with the same contains-then-insert pattern `search` inlines. The
// bitset was chosen over a Vec<u32> epoch-stamp array, which measured
// ~8-10% slower at n=1M and is 32x larger. To evaluate another
// representation, mirror the swap here and in `Workspace` and rerun —
// the bench names are stable, so `-- --save-baseline <name>` before and
// `-- --baseline <name>` after prints the per-case delta.
//
// Two axes:
// - n: node count (the centroid graph size the set is sized to)
// - q: queries served by one allocation
//
// One iteration = allocate the set, then run q queries, each of which
// resets the set and inserts n uniform-random node ids (~63% land on
// unvisited slots, so both branch outcomes are exercised). Every
// measured operation is one check-and-insert, so with throughput set to
// n * q elements, elem/s reads directly as checks/sec; q=1 additionally
// carries the allocation cost (the fresh-workspace-per-query regime)
// while larger q amortizes it and isolates the per-query reset.
//
// Run with:  cargo bench --bench graph

use std::hint::black_box;
use std::time::Duration;

use common::BitSet;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Node counts to sweep (the centroid graph size).
const NODE_COUNTS: &[usize] = &[100_000, 300_000, 1_000_000];

/// Queries served by a single allocation. Each query costs n inserts,
/// so amortization of the allocation saturates quickly; larger values
/// only inflate iteration time (n * q inserts each).
const QUERIES_PER_ALLOC: &[usize] = &[1, 10, 100];

/// Same LCG as benches/distance.rs; deterministic and cheap enough to be
/// noise next to the visited access it feeds.
#[inline]
fn lcg(state: u64) -> u64 {
    state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

/// One iteration: q queries over one allocation, n random inserts each.
#[inline]
fn run_batch(n: usize, queries: usize) -> usize {
    let mut visited = BitSet::with_max_value(n as u32);
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut first_visits = 0;
    for _ in 0..queries {
        visited.clear();
        for _ in 0..n {
            state = lcg(state);
            let node = ((state >> 33) as usize % n) as u32;
            if !visited.contains(node) {
                visited.insert(node);
                first_visits += 1;
            }
        }
    }
    first_visits
}

fn bench_visited(c: &mut Criterion) {
    let mut group = c.benchmark_group("workspace/visited_check");
    for &n in NODE_COUNTS {
        for &q in QUERIES_PER_ALLOC {
            group.throughput(Throughput::Elements((n * q) as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("n={n}"), q),
                &(n, q),
                |bn, &(n, q)| bn.iter(|| run_batch(black_box(n), black_box(q))),
            );
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_visited
}
criterion_main!(benches);
