# DataFusion Limitations for Tantivy Integration

## Shared Scan / Multi-Consumer Plans

### Problem

When a search request needs both **top-K scored hits** and **aggregations**, tantivy's native `MultiCollector` computes both in a single pass over the posting lists. In DataFusion, these are separate plan trees with independent scans:

```
-- TopK hits: Block-WAND pruning via topk=Some(K)
SortExec: TopK(fetch=K), expr=[_score DESC]
  HashJoinExec
    InvertedIndexDataSource(topk=Some(K))   ← scan 1
    FastFieldDataSource

-- Aggregations: needs ALL matching docs
AggregateExec: gby=[category], aggr=[...]
  HashJoinExec
    InvertedIndexDataSource(topk=None)      ← scan 2
    FastFieldDataSource
```

DataFusion currently has no mechanism for one subplan to feed multiple downstream consumers. Each `DataFrame` produces an independent physical plan tree, so forking a base DataFrame results in duplicated scans.

### Impact

- The inverted index posting lists are traversed twice (once with Block-WAND pruning for hits, once fully for aggregations)
- Fast field columns are scanned twice (though dynamic filters prune both sides)
- For expensive queries (complex BooleanQuery, many matching docs), this roughly doubles scan cost

### Workarounds

1. **Accept double scan** — for simple queries or small result sets, the overhead is negligible. Inverted index iteration is fast (posting lists are cache-friendly) and both branches benefit from dynamic filter pushdown.

2. **Materialize the base** — collect the unfiltered join result into an in-memory table, then run both TopK sort and aggregations from materialized data:
   ```rust
   let base_batches = base_df.collect().await?;
   let mem_table = MemTable::try_new(schema, vec![base_batches])?;
   ctx.register_table("base", Arc::new(mem_table))?;
   // Now both hits and aggs read from memory
   ```
   Trades memory for avoiding double scan. Good when the matching doc set is bounded.

3. **Separate the concerns** — run TopK hits through the optimized inverted index path (with Block-WAND), and aggregations through fast-field-only scans (no join needed if aggs don't require `_score`). This is often the right split since aggregations rarely need BM25 scores.

### Upstream Status

DataFusion has discussed shared/cached subplans as a future feature. Relevant tracking:
- Common subexpression elimination at the physical plan level
- `CACHE` / `MATERIALIZE` hints for intermediate results
- Multi-output execution plan nodes (similar to Spark's `persist()`)

When this lands, combined search+agg requests could share a single inverted index scan with a fan-out node feeding both the TopK sort and the aggregation pipeline.
