# vortex-bench

Experiment: storing the fast fields that feed tantivy aggregations as Vortex arrays, and measuring
the impact on `agg_bench` query time and on file size.

Standalone crate with its own workspace, so that the Vortex dependency tree stays out of the
tantivy workspace. It depends on tantivy by path (`..`) and requires no change to tantivy.

The report is `report/vortex_study_en.html` (English, complete). `report/vortex_agg_bench.html` is
an earlier French version.

## Layout

- `src/format.rs`: layout of the `.vortex` segment component.
- `src/writer.rs`: `VortexPlugin`, a `SegmentPlugin` converting the fast fields into Vortex files.
- `src/source.rs`: `VortexValueSource` and its provider, segment cache, chunk LRU, run dispatch.
- `src/point.rs`: point accessors reading single values from compressed chunks.
- `src/opaque.rs`: control variant, the tantivy columns behind a registered `ValueSource`.
- `benches/agg_bench_vortex.rs`: copy of `agg_bench` running each request with 3 interleaved
  variants (columnar, opaque, vortex) and checking the results.
- `examples/`: size report, decode micro-benchmark, tantivy codec inspection, struct file sizes,
  profiling harness, and a tour of the Vortex API (`vortex_client_usage.rs`).
- `patches/value_source_bounds_fast_paths.patch`: two-line tantivy patch letting two aggregation
  fast paths use `ValueSource::bounds()` (measured, not applied).
- `report/`: aggregation script, report templates and generator (`build_en.py`).
- `runs/`: raw results (JSON lines) and aggregated data used by the report.

## Running

```sh
cargo bench --bench agg_bench_vortex                  # builds the indexes in agg_bench_vortex/
VORTEX_BENCH_INPUTS=full VORTEX_BENCH_GROUPS=terms VORTEX_BENCH_SEGMENTS= \
  cargo bench --bench agg_bench_vortex                # subset
cargo run --release --example size_report -- runs/sizes.json agg_bench_vortex/Full
cargo run --release --example decode_bench -- runs/decode.json agg_bench_vortex/Full
python3 report/aggregate.py runs/agg_v3.json runs/v3b_run1 runs/v3b_run2 runs/v3b_run3
python3 report/build_en.py
```

Reader tuning knobs (environment variables): `VORTEX_CHUNK_CACHE` (LRU size, 1 = v1 behaviour),
`VORTEX_POINT_SPAN_RATIO` (0 disables point access = v2 behaviour), `VORTEX_POINT_RUN_COST`,
`VORTEX_PROMOTE_DIVISOR` (promotion from point access to decompression), `VORTEX_TAKE_SPAN_RATIO`
(experimental `take()` path).

## Runs

| directory | implementation |
|---|---|
| `run1` | v1: one cached decompressed chunk |
| `run2`, `run3`, `run5_v2` | v2: chunk decompression + LRU of 8 chunks (full runs) |
| `run4` | v2: rerun of the 26 most extreme requests |
| `phase2` | v2 with the tantivy `bounds()` patch applied |
| `v3a_partial` | v3a: point access without promotion (804 of 836 measurements) |
| `v3b_run1`, `v3b_run2`, `v3b_run3` | v3: point access with promotion (final; run3 = reruns) |
| `ab_*`, `ab2_*`, `ab3` | targeted comparisons of the promotion settings |
| `cache1`, `cache8`, `dry` | LRU size check and initial dry run |

`agg_point.json` aggregates `run5_v2`, a run made with the v2 binary while v3 was expected.
