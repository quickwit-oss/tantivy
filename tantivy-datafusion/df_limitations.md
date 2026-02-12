# DataFusion Aggregation Limitations & Performance Analysis

Analysis of where DataFusion's generic aggregation path is slower than tantivy's
native `AggregationSegmentCollector`, and what causes the overhead.

## Benchmark Summary (1M docs, pre-warmed contexts)

| Case | Native | Pushdown | DataFusion | Winner |
|---|---|---|---|---|
| avg_f64 | 5.5ms | 6.8ms | 6.8ms | Tie (pushdown skipped) |
| stats_f64 | 5.9ms | 10.0ms | 8.8ms | DF (pushdown skipped) |
| terms_few | 3.3ms | **6.8ms** | 15.7ms | **Pushdown 2.3x** |
| terms_few_with_avg | 8.5ms | **13.1ms** | 17.2ms | **Pushdown 1.3x** |
| histogram_100 | 4.7ms | **7.5ms** | 10.7ms | **Pushdown 1.4x** |
| range_6 | 4.6ms | 14.8ms | 15.4ms | Tie |
| range_with_avg | 11.6ms | 13.3ms | 11.5ms | DF |

## Category 1: Metric-only aggregations (avg, stats, count, min, max)

**Pushdown: disabled** — DataFusion is already optimal here.

### Why DF is fast for metrics

Metric-only aggregations have no GROUP BY. DataFusion's `AggregateExec` uses
vectorized Arrow kernels (SIMD sum/min/max) on columnar batches — a single pass
over contiguous memory. This is already within ~25% of tantivy's native path.

The remaining gap vs native tantivy is:
- **Arrow batch materialization**: `FastFieldDataSource` reads tantivy columnar
  data into Arrow arrays (1M f64 values → 8MB allocation + copy). Tantivy's
  native collector operates directly on the columnar storage with zero copy.
- **Two-phase overhead**: DataFusion splits into Partial + Final with a
  `RepartitionExec` + `CoalescePartitionsExec` between them.

### Potential upstream improvements

1. **Single-phase aggregation for 1 partition**: When `target_partitions=1`,
   DF could skip the Partial→Final split and use `mode=Single`.
2. **Zero-copy fast field access**: A custom `DataSource` that exposes tantivy
   columnar data as Arrow arrays without copying (via shared memory or FFI
   buffers) would eliminate the materialization cost.

## Category 2: Terms & Histogram (GROUP BY aggregations)

**Pushdown: enabled** — 1.3–2.3x faster than DataFusion.

### Why DF is slow for terms/histogram

DataFusion's `AggregateExec` uses a generic hash-based GROUP BY:
1. Hash each row's group key (string hashing for terms)
2. Probe/insert into a hash table per group
3. Two-phase: Partial hash tables → repartition → Final hash merge

For terms with 7 distinct values over 1M rows, this means 1M string hash
operations into a 7-entry table. Tantivy's native collector uses
**ordinal-indexed counting** — each term has a dense integer ordinal, and the
collector increments `counts[ordinal]` directly. No hashing.

### Why pushdown still has a ~2x gap vs native

- **Doc ID collection overhead**: `collect_block` iterates `0..max_doc`, whereas
  native tantivy drives iteration more efficiently via the `Collector` trait
- **Arrow conversion**: After tantivy produces results, converting to Arrow
  `RecordBatch` (string arrays for keys, i64 for counts) adds allocation
- **DF planning cost**: `ctx.table("f")` → logical plan → physical plan → collect
  is ~1-2ms fixed overhead per query even with pre-warmed context

### Potential improvement: Global dictionary ordinals for GROUP BY

Currently, string fast fields produce `DictionaryArray<Int32, Utf8>` where
dictionary keys are **per-segment ordinals**. Different segments have different
ordinal→string mappings, so DataFusion must resolve to strings for cross-segment
grouping.

If we introduced a **global dictionary** across segments:
1. Build a merged dictionary at reader open time (union of per-segment dicts)
2. Remap per-segment ordinals to global ordinals during batch generation
3. All batches share the same dictionary → DF can GROUP BY integer keys directly

This would eliminate string hashing entirely for terms aggregations, making DF's
hash GROUP BY operate on i32 keys instead of variable-length strings. Expected
improvement: 3-5x for terms aggregations in the non-pushdown path.

For the pushdown path this doesn't help (tantivy already uses ordinals natively),
but it would make the DF path competitive enough that pushdown might not be
needed for terms.

## Category 3: Range aggregations

**Pushdown: enabled** via CASE-based GROUP BY, but currently no faster than DF.

### Two translation strategies

`translate_range` has two implementations selected based on context:

#### FILTER/UNION path (used by `translate_aggregations`)

```sql
-- Single-row aggregate with N filtered counts:
SELECT count(*) FILTER (WHERE price < 3) as __b0_doc_count,
       count(*) FILTER (WHERE price >= 3 AND price < 5) as __b1_doc_count, ...
FROM f
-- Then UNION ALL + SELECT to reshape into N rows
```

This produces a `UnionExec(ProjectionExec(AggregateExec(...)), ...)` plan.
DataFusion's optimizer generates N independent branches, each with a full scan.
**With 6 range buckets = 6 full scans** of the data. Despite the N-scan cost,
each branch is a simple filtered count (no hash table), which is efficient.

#### CASE GROUP BY path (used by `execute_aggregations` pushdown)

```sql
SELECT CASE WHEN price < 3 THEN 'cheap'
            WHEN price >= 3 AND price < 5 THEN 'mid'
            WHEN price >= 5 THEN 'expensive'
       END as bucket,
       count(*) as doc_count
FROM f
WHERE bucket IS NOT NULL
GROUP BY bucket
```

Single scan with hash GROUP BY on string bucket keys. This is correct and
produces a normal `AggregateExec(GROUP BY bucket)` that the pushdown optimizer
can replace with `TantivyAggregateExec`. However, hashing 1M string keys adds
overhead that offsets the single-scan benefit.

### Why pushdown doesn't help much for range

The CASE path does 1 scan but hashes string keys for every row. Tantivy's native
collector uses `RangeDocSet` with SIMD columnar scans — no hashing, no string
allocation. The gap is the same doc ID collection + Arrow conversion overhead
as for terms, plus the string hashing cost of the CASE GROUP BY.

### Potential improvements

1. **Integer bucket keys in CASE**: Instead of `THEN 'cheap'`, use `THEN 0`,
   `THEN 1`, etc., and resolve to string labels after aggregation. Integer
   hashing is ~10x faster than string hashing.
2. **Bypass DF for range**: Call tantivy's native collector directly from
   `execute_aggregations` without going through translate_aggregations at all.
   The range result set is tiny (N buckets) — no need for DF's batch processing.
