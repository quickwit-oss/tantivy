# Why Tantivy Is Single-Threaded (Per Segment)

## Tantivy's Threading Model

Tantivy follows a Lucene-inspired architecture where **segments are the unit of parallelism**. Each segment is an immutable, self-contained inverted index. Writes create new segments (one per indexing thread per commit); reads never modify segments. This immutability means readers need no locks — any number of threads can search a segment concurrently without coordination.

The `Searcher::search_with_executor` method dispatches one task per segment:

```rust
// src/core/searcher.rs:208-237
/// Same as [`search(...)`](Searcher::search) but multithreaded.
///
/// The current implementation is rather naive :
/// multithreading is by splitting search into as many task
/// as there are segments.
///
/// It is powerless at making search faster if your index consists in
/// one large segment.
///
/// Also, keep in mind multithreading a single query on several
/// threads will not improve your throughput. It can actually
/// hurt it. It will however, decrease the average response time.
pub fn search_with_executor<C: Collector>(
    &self, query: &dyn Query, collector: &C,
    executor: &Executor, enabled_scoring: EnableScoring,
) -> crate::Result<C::Fruit> {
    let weight = query.weight(enabled_scoring)?;
    let segment_readers = self.segment_readers();
    let fruits = executor.map(
        |(segment_ord, segment_reader)| {
            collector.collect_segment(weight.as_ref(), segment_ord as u32, segment_reader)
        },
        segment_readers.iter().enumerate(),
    )?;
    collector.merge_fruits(fruits)
}
```

The `Executor` is either `SingleThread` (sequential loop in caller thread) or `ThreadPool(Arc<rayon::ThreadPool>)` which uses Rayon's work-stealing pool to process segments concurrently. Default is `SingleThread`. Each individual segment is always processed by **one thread** — there is no intra-segment parallelism.

## How This Maps to Quickwit

Quickwit is a distributed search engine built on tantivy. It adds a layer above segments:

```
Quickwit cluster
  └─ Nodes (leaf searchers)
       └─ Splits (~10M docs each, essentially a self-contained tantivy Index)
            └─ Segments (tantivy's native parallelism unit)
```

Parallelism comes from two levels:

1. **Splits distributed across nodes**: A query fans out to all nodes holding relevant splits. Each node processes its splits independently. Results merge at the root searcher.

2. **Segments within a split**: Within each node, tantivy's Rayon pool processes segments in parallel. With typical split sizes (~10M docs, multiple segments), this provides good utilization.

For aggregations, Quickwit uses `DistributedAggregationCollector` which returns `IntermediateAggregationResults` — a serializable intermediate form that merges across nodes before final conversion. This is the classic map-reduce pattern: each segment produces a partial result, segments merge locally, nodes merge globally.

At Quickwit scale (billions of docs, hundreds of splits, many nodes), the "one thread per segment" model works well. There are enough segments and splits to saturate all available cores.

## Where This Breaks Down: Our Benchmark

Our benchmark creates a single tantivy index with 1M docs and **one segment** (single `writer.commit()`). This is the worst case for tantivy's threading model — there's exactly one unit of work, so the Rayon pool has nothing to parallelize.

```
1M docs, 1 segment:
  tantivy native: 1 thread processes all 1M docs
  no parallelism possible
```

This is realistic for use cases where tantivy-datafusion is used as an embedded analytical engine on a single index (not a Quickwit cluster). A user might have a 10M-doc index with 1-3 segments from periodic merges.

## How Chunking Helps (And What It Costs)

### The Approach

Our `FastFieldDataSource` now splits each segment into multiple partitions by doc ID range:

```
FastFieldDataSource(1M docs, 1 segment, target_partitions=10)
  partition 0:  docs 0..100K
  partition 1:  docs 100K..200K
  ...
  partition 9:  docs 900K..1M
```

DataFusion schedules each partition on its own thread. With the `DenseOrdinalAggExec` optimization, each partition runs an independent dense-array GROUP BY on its chunk, and DataFusion's `Final` aggregate merges the 10 partial results.

### Latency Wins

Benchmarks on 1M docs, 1 segment (wall clock time):

| Benchmark | native (1 thread) | ordinal_df (chunked) |
|---|---|---|
| terms_few | 3.0ms | 3.8ms |
| terms_few_with_avg | 8.2ms | **5.7ms** |

The `terms_few_with_avg` result is striking: ordinal_df **beats native** on latency because it parallelizes across 10 threads while native is stuck on 1.

### The CPU Efficiency Problem

But latency is not the whole story. The tantivy comment warns:

> multithreading a single query on several threads will not improve your throughput. It can actually hurt it.

This applies to our chunking too. Total CPU time tells a different story:

| Benchmark | native | ordinal_df (10 threads) |
|---|---|---|
| terms_few_with_avg | 8.2ms wall = **8.2ms CPU** | 5.7ms wall ~ **57ms CPU** |

Native uses 8.2ms of compute on 1 core. Ordinal_df uses ~57ms of compute spread across 10 cores to achieve 5.7ms wall clock. That's **7x less CPU-efficient**.

Under concurrent query load (many users, many queries in flight), this matters enormously:
- With 10 cores and native tantivy: ~10 queries served in parallel, each at 8.2ms = throughput of ~1200 queries/sec
- With 10 cores and chunked ordinal: each query consumes all 10 cores for 5.7ms = throughput of ~175 queries/sec

The latency win becomes a throughput disaster. This is exactly the tradeoff tantivy's architecture is designed around: **optimize for throughput (many queries sharing cores) rather than single-query latency**.

### When Chunking Makes Sense

Chunking helps when:
- **Interactive/analytical use**: Single user running ad-hoc aggregations, cares about response time, not throughput
- **Few large segments**: Index hasn't been merged recently, or is a single-segment embedded index
- **Idle cores**: System has spare CPU capacity that would otherwise go unused

Chunking hurts when:
- **High query concurrency**: Many queries competing for cores (the Quickwit/production search scenario)
- **Many segments already**: Natural segment-level parallelism is sufficient
- **CPU-bound workloads**: Total compute matters more than per-query latency

### Why We Only Chunk for Aggregations

Chunking is disabled when `_doc_id` and `_segment_ord` are in the projected schema (join queries). These columns indicate a join with the inverted index or document provider, which requires co-partitioning — all providers must agree on 1 partition per segment with matching `Hash([_doc_id, _segment_ord])` partitioning so DataFusion skips shuffle operators.

Aggregation queries never project these columns, so chunking is safe. This is also where chunking helps most — aggregations scan the full column and benefit from parallel reduction.

## Where DataFusion Is Still Less Efficient

Even with ordinal optimizations, DataFusion's approach has inherent overhead compared to native tantivy:

### 1. Materialization Cost

Tantivy's native `AggregationSegmentCollector` reads columnar data and aggregates in a single pass — values go directly from the column store into accumulators. Never materialized as Arrow arrays.

Our approach reads fast fields into Arrow `RecordBatch`es (materialization), then DataFusion's accumulators process the Arrow arrays. For a 1M-row f64 column, that's 8MB of Arrow buffers allocated, filled, then read again by the accumulator. Native tantivy avoids this intermediate copy entirely.

### 2. Dictionary Overhead Per Chunk

Each chunk builds the **full segment dictionary** (all unique terms) even though it only reads a subset of docs. With 10 chunks of a segment with 150K unique terms, we build the same 150K-entry dictionary 10 times. The keys differ per chunk but the values array is duplicated. Native tantivy builds the dictionary once per segment.

### 3. Range Queries: CASE vs SIMD

Range aggregations are where the gap is largest:

| | native | datafusion | ordinal_df |
|---|---|---|---|
| range_6 | 4.4ms | 12.2ms | 12.4ms |

Native tantivy uses `FastFieldRangeWeight` with SIMD columnar scans (`RangeDocSet`) — directly scanning the column and identifying matching doc ranges with vectorized comparisons. One pass per range bucket but each pass is extremely cheap.

Our `translate_range_case` generates:
```sql
SELECT CASE WHEN score >= 3 AND score < 7000 THEN '3-7000'
            WHEN score >= 7000 AND score < 20000 THEN '7000-20000' ...
       END as bucket,
       count(*) as doc_count
FROM f GROUP BY bucket
```

This has two problems:
1. **CASE evaluation**: 6 branches x 2 comparisons = 12 comparisons per row, evaluated in DataFusion's expression framework (not SIMD-optimized for range patterns)
2. **String GROUP BY**: The bucket key is a Utf8 string literal, so DataFusion hashes strings for the GROUP BY. The ordinal optimization doesn't fire because the GROUP BY column is a computed CASE expression, not a `Dict(Int32, Utf8)` fast field.

## Summary

| Aspect | tantivy native | tantivy-datafusion |
|---|---|---|
| Parallelism unit | segment | chunk (sub-segment) |
| Latency (1 segment) | limited by 1 thread | better via chunking |
| CPU efficiency | optimal (1 pass, no materialization) | ~7x overhead from threading + Arrow materialization |
| Throughput | excellent (1 thread/query) | poor under concurrency |
| Range queries | SIMD columnar scans | CASE + string hash GROUP BY |
| At scale (many segments) | natural parallelism | chunking adds nothing |

The tantivy-datafusion approach trades CPU efficiency for single-query latency — useful for interactive analytics on embedded indexes, but not a replacement for tantivy's native collectors in high-throughput production search.
