# Quickwit-DataFusion Integration Guide

Reference for building a `quickwit-datafusion` crate that orchestrates DataFusion over Quickwit splits (each a tantivy index).

---

## Table Providers

### 1. `TantivyTableProvider` — Fast Field Reader

**What it does**: Exposes tantivy fast fields (columnar storage) as Arrow columns via `DataSourceExec` / `FastFieldDataSource`.

**Schema**: All FAST-flagged fields from the tantivy schema, plus two synthetic columns:
- `_doc_id: UInt32` — tantivy document ID within the segment
- `_segment_ord: UInt32` — segment ordinal within the index

**Type mapping**:
| tantivy type | Arrow type | Notes |
|---|---|---|
| Str (single-valued) | `Dictionary(Int32, Utf8)` | Ordinals as keys, dictionary per segment |
| Str (multi-valued) | `List<Utf8>` | |
| U64/I64/F64/Bool | Native Arrow types | |
| Date | `Timestamp(Microsecond, None)` | |
| IpAddr | `Utf8` | Formatted as string |
| Bytes | `Binary` | |

**Key behaviors**:
- `supports_filters_pushdown` for UDF predicates (e.g., `full_text()`)
- `with_fetch` returns `None` — declines limit pushdown to keep FilterExec above
- Dynamic filter pushdown via `try_pushdown_filters` — accepts all filters, applies lazily

**Partitioning** (critical for joins):
- When `_doc_id` and `_segment_ord` are projected: `Hash([_doc_id, _segment_ord], num_segments)` — co-partitioned with inverted index / document providers
- When they're NOT projected (aggregation queries): `UnknownPartitioning(N)` with N = chunked sub-segment ranges for parallelism

### 2. `TantivyInvertedIndexProvider` — Full-Text Search

**What it does**: Runs tantivy queries through the inverted index, returns doc IDs with optional BM25 scores.

**Schema**: `(_doc_id: UInt32, _segment_ord: UInt32, [text_columns: Utf8...], _score: Float32)`

**Key behaviors**:
- `_score` is `Float32` — BM25 relevance score (null when no query)
- Declines dynamic filters (`PushedDown::No`) — this is always the build side of a join
- Uses `Hash([_doc_id, _segment_ord], num_segments)` partitioning

**TopK pushdown**: The `TopKPushdown` rule pushes `ORDER BY _score DESC LIMIT K` into the data source, enabling `for_each_pruning` (Block-WAND) for efficient top-K retrieval.

### 3. `TantivyDocumentProvider` — Stored Documents

**What it does**: Returns stored documents as JSON-serialized strings for enrichment.

**Schema**: `(_doc_id: UInt32, _segment_ord: UInt32, _document: Utf8)`

**Usage**: Always joined with fast fields or inverted index on `(_doc_id, _segment_ord)`.

---

## How Providers Compose

### Full-text search + fast field enrichment (the core pattern)

```sql
SELECT inv.id, f.price, inv._score
FROM inv                                    -- InvertedIndexDataSource (build side)
JOIN f ON inv._doc_id = f._doc_id           -- FastFieldDataSource (probe side)
   AND inv._segment_ord = f._segment_ord
WHERE full_text(inv.category, 'electronics')
ORDER BY inv._score DESC
LIMIT 10
```

Plan:
```
SortExec(TopK, _score DESC, fetch=10)
  HashJoinExec(CollectLeft, on=[_doc_id, _segment_ord])
    InvertedIndexDataSource       ← build side, topK pushed down
    FastFieldDataSource           ← probe side, dynamic filters from join
```

### Three-way join (search + fast fields + documents)

```sql
SELECT f.id, f.price, d._document
FROM inv
JOIN f ON (inv._doc_id = f._doc_id AND inv._segment_ord = f._segment_ord)
JOIN d ON (f._doc_id = d._doc_id AND f._segment_ord = d._segment_ord)
WHERE full_text(inv.category, 'query')
```

### Aggregation-only (no join needed)

```sql
SELECT category, count(*), avg(price) FROM f GROUP BY category
```

Plan (with ordinal optimization):
```
AggregateExec(Final)
  CoalescePartitionsExec
    DenseOrdinalAggExec(Partial, N partitions)
      DataSourceExec(FastFieldDataSource, N chunked partitions)
```

---

## Partitioning and Co-partitioning

### The co-partitioning contract

All three providers declare `Hash([_doc_id, _segment_ord], num_segments)` when join columns are projected. DataFusion recognizes co-partitioned join sides and **skips `RepartitionExec` shuffles** — critical for performance.

**Requirements**:
- Both sides must declare the same `Hash(exprs, N)` partitioning
- `N` must match (= number of segments)
- `target_partitions` in `SessionConfig` should equal `num_segments` for join queries to prevent fan-out

### Chunked partitions (aggregation only)

When `_doc_id`/`_segment_ord` are NOT projected, `FastFieldDataSource` chunks each segment into `target_partitions / num_segments` sub-ranges. This gives DataFusion parallelism for aggregations.

**Safety**: Chunking is disabled when:
- `_doc_id` or `_segment_ord` are in the projection (join query)
- A tantivy query is set (doc IDs come from the query, not ranges)

### For Quickwit

Each Quickwit split is a separate tantivy index. The partitioning strategy should be:

1. **Across splits**: Each split gets its own set of providers. DataFusion can federate via `UNION ALL` or a custom multi-split provider.
2. **Within a split**: Use co-partitioning for joins (1 partition per segment). Use chunking for aggregations (N partitions per segment).
3. **target_partitions**: Set per-split based on segment count for join queries. Use default (num_cpus) for aggregation queries.

---

## Physical Optimizer Rules

### 1. `TopKPushdown`

**What it does**: Pushes `ORDER BY _score DESC LIMIT K` into `InvertedIndexDataSource`, enabling Block-WAND pruning.

**When it fires**: Finds `SortExec(fetch=K)` with `_score DESC`, traverses safe operators (CooperativeExec, HashJoinExec where probe side has no filters), and pushes topK into the inverted index data source.

**When to use**: Always register for search queries with relevance ranking.

### 2. `FastFieldFilterPushdown`

**What it does**: Moves fast field predicates (e.g., `price > 2`, `status = 'active'`) from the probe-side `FastFieldDataSource` into the build-side `InvertedIndexDataSource` as tantivy `RangeQuery`/`TermQuery`. This pre-filters at the inverted index level.

**Order**: Must run BEFORE `TopKPushdown` so the join becomes pure 1:1 enrichment.

**When to use**: Always register for queries that combine text search with fast field filters.

### 3. `AggPushdown`

**What it does**: Replaces DataFusion's `AggregateExec` with `TantivyAggregateExec` when `FastFieldDataSource` has tantivy `Aggregations` stashed. Calls native `AggregationSegmentCollector` directly.

**When it fires**: Only for bucket aggregations (terms, histogram, range) with GROUP BY. Skips metric-only aggs (avg, stats) where DataFusion's vectorized Arrow path is already faster.

**Key implementation detail**: The `TantivyAggregateExec` now uses 512-doc block collection (matching native tantivy's chunking) for cache locality. For the no-query case, it generates doc ID blocks manually.

**When to use**: Register for the `execute_aggregations` API path where tantivy agg specs are stashed on the provider.

### 4. `OrdinalGroupByOptimization`

**What it does**: Replaces `AggregateExec(Partial)` with `DenseOrdinalAggExec` when GROUP BY column is `Dictionary(Int32, Utf8)` from a `FastFieldDataSource`. Uses dictionary ordinals as dense array indices — no hash table.

**Plan transformation** (two-phase):
```
BEFORE:
AggregateExec(FinalPartitioned)
  RepartitionExec(Hash)
    AggregateExec(Partial)
      DataSourceExec

AFTER:
AggregateExec(Final)
  CoalescePartitionsExec        ← stateless, replaces RepartitionExec
    DenseOrdinalAggExec(State)
      DataSourceExec
```

Uses `CoalescePartitionsExec` instead of `RepartitionExec` because the partial output is tiny (~7 rows per partition). This makes the plan stateless and reusable.

**When to use**: Register for SQL aggregation queries on string fast fields. Particularly effective for low-cardinality fields (< 100 terms) where tantivy also uses dense array indexing internally.

### Pushdown vs OrdinalGroupBy: when to use which

| Scenario | Best path | Why |
|---|---|---|
| Terms agg (low cardinality) | **OrdinalGroupBy** | Dense ordinal indexing, parallelizable |
| Terms agg + sub-aggs (avg, etc.) | **OrdinalGroupBy** | Combines ordinal grouping with DF's vectorized metrics |
| Simple metrics (avg, stats, count) | **Neither** (DF native) | DF's vectorized Arrow path beats both |
| Histogram agg | **AggPushdown** | No ordinal advantage, native tantivy is efficient |
| Range agg | **AggPushdown** | CASE-based GROUP BY is slow; native range scans are SIMD-fast |
| Pre-filtered agg (with tantivy query) | **AggPushdown** | Query + collect in one pass, no materialization |

---

## Performance Characteristics

### Benchmark results (1M docs, 1 segment)

| Benchmark | native | pushdown_exec | ordinal_df (multi) | ordinal_1t (single) |
|---|---|---|---|---|
| avg_f64 | 5.3ms | **1.7ms** | 2.3ms | 7.9ms |
| stats_f64 | 5.3ms | **2.0ms** | 2.9ms | 10.5ms |
| terms_few | **3.1ms** | 3.9ms | 3.8ms | ~12ms |
| terms_few_with_avg | 8.3ms | 8.9ms | **5.7ms** | ~21ms |
| histogram_100 | **4.7ms** | 5.0ms | 7.9ms | — |
| range_6 | **4.4ms** | 4.8ms | 15.3ms | — |

`pushdown_exec` = execution only (no planning). `ordinal_df` = multi-threaded (~10 CPUs). `ordinal_1t` = single-threaded.

### Key takeaways

1. **Simple metrics (avg, stats)**: DataFusion's vectorized Arrow path beats native tantivy, even single-threaded. With multi-thread chunking, 2-3x faster.

2. **Terms aggregation**: Single-threaded ordinal is ~3x slower than native (Arrow materialization overhead). Multi-threaded ordinal matches or beats native on wall clock but uses ~7x more CPU.

3. **Planning overhead**: ~1-2ms per query. Irrelevant for IO-bound Quickwit workloads (S3 reads are 100-200ms). Can be eliminated by caching physical plans (works for pushdown and ordinal plans with CoalescePartitionsExec).

4. **Range/histogram**: AggPushdown (native tantivy collectors) is the only competitive path. The CASE-based GROUP BY approach is ~3x slower due to branch evaluation + string hash GROUP BY.

---

## Multi-threading the Tantivy Reads

### Tantivy's native model

Tantivy parallelizes across segments (via Rayon), not within segments. Each segment is processed by one thread. With 1 large segment, native tantivy is single-threaded.

### Our chunking approach

`FastFieldDataSource` splits each segment into sub-ranges by doc ID:
```
1M docs, 1 segment, target_partitions=10:
  partition 0: docs 0..100K
  partition 1: docs 100K..200K
  ...
  partition 9: docs 900K..1M
```

**Correctness invariants**:
- Dictionary stability: All chunks from the same segment produce the same dictionary values (same `StrColumn.ord_to_str` results). Only the keys differ per chunk.
- Alive bitset: Each chunk filters by `alive_bitset` within its range.
- Query filtering: When a tantivy query is active, query results are intersected with the chunk's doc range.

**Watch out for**:
- Dictionary is rebuilt per chunk. With 150K unique terms and 10 chunks, that's 10 dictionary builds. This is the dominant cost for single-threaded ordinal.
- Chunking only makes sense when you have idle cores. Under high query concurrency, it wastes CPU (see `why_tantivy_is_single_threaded.md`).
- Never chunk when join columns are projected — breaks co-partitioning.

### For Quickwit

In production Quickwit deployments with many splits and segments, there's already abundant parallelism. Chunking is most valuable for:
- Small indexes with few segments (embedded/analytical use cases)
- Interactive single-query latency optimization
- Aggregation-only queries where co-partitioning isn't needed

---

## Dynamic Filtering

### How it works

`FastFieldDataSource` accepts all pushed filters via `try_pushdown_filters → PushedDown::Yes`. Filters are stored and applied lazily in the stream after batch generation.

`DynamicFilterPhysicalExpr` (from DataFusion's hash join) evaluates to `Scalar(Boolean(true))` before the build side completes. Once the build side finishes, it provides min/max bounds that prune non-matching rows on the probe side.

### Why it matters for joins

In a search + fast field join:
```
HashJoinExec(CollectLeft)
  InvertedIndexDataSource        ← build side (say 50 matching docs)
  FastFieldDataSource            ← probe side (1M docs, dynamic filter prunes to ~50)
```

Without dynamic filters, the probe side reads all 1M docs. With dynamic filters, it reads 1M docs but then immediately prunes to ~50 based on the build side's doc ID range. This is important when the query is selective.

### Important: `InvertedIndexDataSource` declines dynamic filters

The inverted index provider returns `PushedDown::No` for all pushed filters. It's always the build side — it produces the doc ID set that the probe side joins against.

---

## Composing Doc ID Calls

### The doc ID flow

1. **InvertedIndexDataSource** produces `(_doc_id, _segment_ord)` for matching docs
2. **HashJoinExec** matches these with `FastFieldDataSource` on the same columns
3. **FastFieldDataSource** reads fast fields for the matched doc IDs
4. **DocumentDataSource** (optional) reads stored docs for enrichment

### For Quickwit: cross-split orchestration

Each split is independent. The pattern for a distributed query:

1. **Fan-out**: Send query to all relevant splits (prune by timestamp range, etc.)
2. **Per-split execution**: Each split runs the full DataFusion plan (search + join + agg)
3. **Merge**: Combine results from all splits

For aggregations:
- Each split produces partial aggregation results
- Merge is just concatenation + final aggregate
- DataFusion's `UNION ALL` or a custom merge operator works

For top-K search:
- Each split produces its top-K results
- Merge selects global top-K from all splits' results
- Need a distributed top-K merge operator

### Session configuration per query type

```rust
// For join queries (search + enrichment):
let config = SessionConfig::new()
    .with_target_partitions(num_segments);  // co-partitioning

// For aggregation queries (no join):
let config = SessionConfig::new();  // default target_partitions = num_cpus
```

---

## `full_text` UDF

Scalar UDF `full_text(column, 'query_string')` that pushes text search predicates into the table provider.

**Important**: The first argument must be a column reference, not a literal. DataFusion only pushes predicates that reference table columns. `full_text('category', 'query')` with a literal first arg will NOT be pushed down.

**Usage**:
```rust
ctx.register_udf(full_text_udf());
// Then in SQL:
// WHERE full_text(category, 'electronics')
```

Pushed into the provider via `supports_filters_pushdown → Exact`. Multiple `full_text` predicates are combined via `BooleanQuery::intersection`.

---

## Agg Translator (ES-compatible API)

`translate_aggregations(df, &aggs)` converts tantivy `Aggregations` (Elasticsearch-compatible JSON format) into DataFusion `DataFrame` operations.

Supports: terms, histogram, date_histogram, range, avg, sum, min, max, count, stats, extended_stats, percentiles, cardinality.

`execute_aggregations(index, &aggs)` is the high-level API that creates a session with `AggPushdown` + `OrdinalGroupByOptimization`, stashes aggs on the provider, and collects results.

`create_session_with_pushdown(index, &aggs)` builds the session for reuse across multiple queries.

---

## Summary: What to Build in quickwit-datafusion

1. **Multi-split provider**: Wraps multiple tantivy indexes (one per split), presents a unified table to DataFusion. Handles split pruning (timestamp ranges, etc.).

2. **Distributed aggregation**: Fan-out to splits, partial aggregate per split, merge results. Can leverage DataFusion's `UNION ALL` or custom merge.

3. **Distributed top-K**: Per-split top-K with global merge. Integrate with the existing `TopKPushdown` rule.

4. **Session management**: Configure `target_partitions` appropriately per query type. Cache physical plans for hot queries.

5. **Storage integration**: Connect `FastFieldDataSource` to Quickwit's hotcache / S3 storage layer instead of local tantivy index files.

6. **Wire format**: Arrow IPC for intermediate results between nodes. Small overhead for aggregation results (7-70 rows), significant advantage for scan results vs JSON serialization.

---

## Multi-Split Architecture: Union vs Single Provider

There are two approaches for presenting multiple splits to DataFusion:

### Approach 1: Union of per-split plans (recommended starting point)

Each split gets its own self-contained set of providers (`FastFieldDataSource`, `InvertedIndexDataSource`, `DocumentDataSource`). A `UNION ALL` or custom merge operator at the coordinator combines results.

```
Coordinator
  MergeExec (top-K merge or agg final)
    ├─ Split A plan (search + join + partial agg)
    ├─ Split B plan (search + join + partial agg)
    └─ Split C plan (search + join + partial agg)
```

**Advantages**:
- Preserves all existing invariants (co-partitioning, doc ID locality, dynamic filters)
- Each split is a self-contained tantivy-datafusion instance — no new abstractions
- Same deploy model Quickwit already uses — nodes open their splits independently
- No shared state or distributed cache coordination between nodes
- Easy to add split pruning (timestamp ranges, etc.) at the coordinator level

**Disadvantages**:
- DataFusion can't optimize across the UNION boundary (e.g., no cross-split predicate pushdown)
- Each split plans independently — some redundant work if query shapes are identical

### Approach 2: Single provider spanning all splits

One `FastFieldDataSource` that internally maps partitions to `(split, segment, chunk)` tuples. DataFusion sees a single table with N total partitions across all splits.

**What this requires**:
- A distributed cache keyed on `(split_id, segment_ord, chunk_range)` for IO calls
- Worker nodes need to know the full split/segment topology upfront
- Co-partitioning contract becomes complex — partitions from different splits cannot be co-joined (doc IDs are split-local), so the provider must enforce that join partners are always from the same split
- New deploy architecture: coordinator assigns partitions to workers, workers cache and serve data — closer to Spark/Trino executor pools

**Advantages**:
- DataFusion can optimize the full plan globally
- Potential for better resource utilization (work-stealing across splits)
- Single physical plan to cache and reuse

**Recommendation**: Start with Approach 1 (union). It's a minimal change from the current architecture and lets us validate the DataFusion integration end-to-end with real Quickwit workloads. Approach 2 is worth exploring once we understand the performance characteristics and whether cross-split optimization actually matters in practice — for most queries, the per-split plans are identical in structure and the merge is trivial.
