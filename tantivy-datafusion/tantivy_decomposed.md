# Decomposed tantivy-datafusion Architecture

Three independent providers, composed via standard DataFusion operators (joins, filters, projections). No monolithic execution node. Each provider has a single responsibility and is independently testable, serializable, and distributable.

## The Three Providers

### 1. Fast Field Provider (Table)

Columnar scan over tantivy fast fields. Returns Arrow RecordBatches. This is the primary data path for aggregations, filtering, and sorting.

```sql
SELECT _doc_id, _segment_ord, timestamp, service, status
FROM tantivy_fast
WHERE status >= 500
ORDER BY timestamp DESC
LIMIT 10
```

**What it does:**
- Iterates all alive docs in each segment (one partition per segment)
- Reads projected fast field columns into Arrow arrays
- Emits `_doc_id` and `_segment_ord` as metadata columns on every batch

**What it does NOT do:**
- No inverted index queries
- No stored field reads
- No BM25 scoring

**DataFusion handles natively on its output:**
- `WHERE` filtering on Arrow columns (no tantivy query conversion needed)
- `ORDER BY` via `SortExec`
- `GROUP BY` / aggregations via `AggregateExec`
- `LIMIT` via `GlobalLimitExec` (limit hint also passed to `scan()` for early termination)

**Arrow schema:** Derived from tantivy schema. Fast fields map to scalar Arrow types; multi-valued fields map to `List<T>`. Non-fast fields are not in this schema.

| Tantivy Type | Arrow Type | Multi-valued |
|---|---|---|
| U64 | UInt64 | List\<UInt64\> |
| I64 | Int64 | List\<Int64\> |
| F64 | Float64 | List\<Float64\> |
| Bool | Boolean | List\<Boolean\> |
| Date | Timestamp(Microsecond) | List\<Timestamp\> |
| Str | Utf8 | List\<Utf8\> |
| Bytes | Binary | List\<Binary\> |
| IpAddr | Utf8 | List\<Utf8\> |

**Implementation:** `TableProvider::scan()` returns `LazyMemoryExec` with one `LazyBatchGenerator` per segment. No custom `ExecutionPlan` needed.

```rust
impl TableProvider for TantivyFastFieldProvider {
    async fn scan(&self, ..., projection, filters, limit) -> Result<Arc<dyn ExecutionPlan>> {
        // One generator per segment
        let generators: Vec<_> = (0..num_segments)
            .map(|seg_idx| {
                Arc::new(RwLock::new(FastFieldBatchGenerator {
                    index: self.index.clone(),
                    segment_idx: seg_idx,
                    projected_schema: projected_schema.clone(),
                    limit,
                    exhausted: false,
                }))
            })
            .collect();

        Ok(Arc::new(LazyMemoryExec::try_new(projected_schema, generators)?))
    }
}
```

### 2. Inverted Index Provider (Table Function)

Runs a tantivy query through the inverted index. Returns doc IDs (and optionally BM25 scores). This is the full-text search entry point.

```sql
SELECT _doc_id, _segment_ord, _score
FROM tantivy_search('message', 'connection refused')
```

**What it does:**
- Parses the query string, builds a tantivy `Query`
- Runs `weight.for_each()` (or `weight.scorer()` for BM25) across segments
- Emits `_doc_id`, `_segment_ord` for each matching document
- Optionally emits `_score` (Float32) when BM25 scoring is requested

**What it does NOT do:**
- No fast field reads (except internally for query evaluation if tantivy needs them)
- No stored field reads
- No SQL filtering — it only evaluates the tantivy query

**Registration:**

```rust
ctx.register_udtf("tantivy_search", Arc::new(TantivySearchFunction { index }));
```

**Table function signature:**

```rust
impl TableFunctionImpl for TantivySearchFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // args[0]: field name (literal Utf8)
        // args[1]: query string (literal Utf8)
        // Returns a TableProvider that runs the inverted index search
        let field = extract_literal_string(&args[0])?;
        let query_string = extract_literal_string(&args[1])?;
        let query = build_tantivy_query(&self.index, &field, &query_string)?;

        Ok(Arc::new(InvertedIndexResultProvider {
            index: self.index.clone(),
            query,
        }))
    }
}
```

**Output schema:**

| Column | Type | Description |
|---|---|---|
| `_doc_id` | UInt32 | Document ID within segment |
| `_segment_ord` | UInt32 | Segment ordinal |
| `_score` | Float32 (nullable) | BM25 score (only when scoring enabled) |

**Limitation:** Table function args must be literals — `tantivy_search('message', 'error')` works, but `tantivy_search(col_name, query)` does not. This is fine because the field name and query string are always known at plan time.

### 3. Stored Field Provider (Table)

Reads full document content from tantivy's doc store. Takes doc addresses as input (via join), returns `_source` JSON.

```sql
SELECT _doc_id, _segment_ord, _source
FROM tantivy_stored
```

**What it does:**
- For each `(_doc_id, _segment_ord)` in the input, calls `store_reader.get(doc_id)`
- Converts the tantivy `Document` to JSON
- Emits `_source` as a Utf8 column

**When it appears in the plan:**
- Only when document content is requested (`SELECT *`, `SELECT _source`, snippet generation)
- Sits above TopK in the plan, so it only processes K rows
- Never appears for pure aggregation or fast-field-only queries

**Output schema:**

| Column | Type | Description |
|---|---|---|
| `_doc_id` | UInt32 | Document ID (passthrough) |
| `_segment_ord` | UInt32 | Segment ordinal (passthrough) |
| `_source` | Utf8 | Full document JSON |

---

## Composition via Standard DF Operators

### Pure fast-field query (no text search)

```sql
SELECT timestamp, service, COUNT(*)
FROM tantivy_fast
WHERE status >= 500
GROUP BY service
ORDER BY COUNT(*) DESC
```

**Plan:**

```
SortExec(sort=[count DESC])
  └── AggregateExec(group=[service], agg=[COUNT(*)])
        └── tantivy_fast scan (projection=[timestamp, service, status])
```

Only the fast field provider. DF handles filtering, aggregation, sorting on Arrow columns. No inverted index, no stored fields.

### Full-text search + fast field filtering

```sql
SELECT f.timestamp, f.service
FROM tantivy_fast f
JOIN tantivy_search('message', 'error') s
  ON f._doc_id = s._doc_id AND f._segment_ord = s._segment_ord
WHERE f.status >= 500
ORDER BY f.timestamp DESC
LIMIT 10
```

**Plan:**

```
GlobalLimitExec(limit=10)
  └── SortExec(sort=[timestamp DESC])
        └── HashJoinExec(ON _doc_id, _segment_ord)
              ├── tantivy_search('message', 'error')  [build side]
              └── tantivy_fast scan                    [probe side]
                     ↑ dynamic filter pushdown (doc ID membership from build)
```

The inverted index search completes first (build side), producing the doc ID membership set. DataFusion's dynamic filter pushdown pushes this set down to the fast field scan (probe side), so only matching docs are read from fast fields.

### Full-text search + document content + snippets

```sql
SELECT f.timestamp, f.service, d._source
FROM tantivy_fast f
JOIN tantivy_search('message', 'error') s
  ON f._doc_id = s._doc_id AND f._segment_ord = s._segment_ord
JOIN tantivy_stored d
  ON f._doc_id = d._doc_id AND f._segment_ord = d._segment_ord
WHERE f.status >= 500
ORDER BY f.timestamp DESC
LIMIT 10
```

**Plan:**

```
ProjectionExec(snippet(_source, 'message', 'error') AS highlighted)
  └── HashJoinExec(ON _doc_id, _segment_ord)       ← stored field join
        ├── tantivy_stored scan                      [build: only K rows]
        └── GlobalLimitExec(limit=10)
              └── SortExec(sort=[timestamp DESC])
                    └── HashJoinExec(ON _doc_id, _segment_ord)
                          ├── tantivy_search('message', 'error')
                          └── tantivy_fast scan
                                 ↑ dynamic filter pushdown
```

The stored field join sits above TopK. Only the 10 winning rows get their stored fields fetched. `snippet()` is a scalar UDF in a standard `ProjectionExec`.

---

## Dynamic Filter Pushdown: Inverted Index → Fast Fields

This uses the same mechanism as the metrics-points-and-tags partition-index dynamic filter routing.

### Analogy

| Metrics System | Tantivy |
|---|---|
| Context/tags (build side) | Inverted index search (build side) |
| Bhandle membership set | Doc ID membership set |
| Points (probe side) receives dynamic filter | Fast field scan (probe side) receives doc ID filter |
| Skip non-matching bhandle rows at scan time | Skip non-matching doc IDs at scan time |

### How it works

1. **Build side completes:** `tantivy_search` scans the inverted index, produces `(_doc_id, _segment_ord)` pairs for all matching documents.

2. **Dynamic filter constructed:** DataFusion's `HashJoinExec` builds a membership filter from the build side data:
   - **Bounds:** `_doc_id >= min_match AND _doc_id <= max_match` (cheap range pruning)
   - **Membership:** `_doc_id IN {matching_ids}` or hash table lookup (exact filtering)

3. **Pushed to probe side:** The fast field scan's `LazyBatchGenerator` receives the dynamic filter. Before reading fast field values for a doc, it checks membership. Non-matching docs are skipped entirely — their fast field values are never decoded.

4. **Per-partition alignment:** Each segment (partition) in the fast field scan receives the filter built from the corresponding segment in the inverted index search. Both providers partition by segment, so partition indices align naturally.

```
Inverted index search (partition 0, segment 0):
  matching doc_ids: {5, 42, 99, 1337}

Dynamic filter pushed to fast field scan (partition 0, segment 0):
  bounds: _doc_id >= 5 AND _doc_id <= 1337
  membership: _doc_id IN {5, 42, 99, 1337}

Fast field scan (partition 0, segment 0):
  max_doc = 2000
  Without filter: read fast fields for 2000 docs
  With filter: read fast fields for 4 docs
```

### Why this is better than the current monolithic approach

The current code converts SQL `WHERE` clauses into tantivy queries (`expr_to_tantivy.rs`), runs them through the inverted index inside a single `ExecutionPlan`, then reads fast fields. This conflates two things:

- **Fast field predicates** (`WHERE id > 5`) — don't need the inverted index at all. DF can evaluate these on Arrow columns after the fast field scan.
- **Text predicates** (`WHERE message MATCH 'error'`) — need the inverted index. Should be a separate provider.

By decomposing, fast field predicates are evaluated by DF natively (no tantivy query conversion), and text predicates go through the inverted index provider with dynamic filter pushdown connecting the result to the fast field scan.

---

## Distributed Execution

The decomposed model composes naturally with `datafusion-distributed` and Arrow Flight.

### Stage decomposition

```
Stage 0 (each worker, per-split, co-located):
  HashJoinExec(tantivy_search ⟕ tantivy_fast, ON _doc_id)
    ├── tantivy_search scan    ← inverted index, local to split
    └── tantivy_fast scan      ← fast fields, local to split
         ↑ dynamic filter pushdown
  AggregateExec(Partial) or local TopK

  → stream partial results via Arrow Flight

Stage 1 (coordinator):
  AggregateExec(Final) or SortPreservingMergeExec
  → K winner rows (with _doc_id, _segment_ord)

Stage 2 (back to workers, only for K rows):
  tantivy_stored scan (fetch _source for K doc addresses)
  → stream enriched results via Arrow Flight
```

### Why this works

1. **Co-location is natural.** Inverted index and fast field scans for the same split need the same segment data. `datafusion-distributed` sees they share the same partitioning (both partition by segment within a split) and keeps them on the same worker.

2. **Standard shuffle boundaries.** The partial→final aggregation boundary and TopK merge boundary are stock DF operators. `datafusion-distributed` knows how to split these across Arrow Flight without custom logic.

3. **Stored field fetch stays minimal.** The stored field provider is a separate stage that only runs for K winner doc addresses. The coordinator sends K doc addresses back to workers, workers fetch stored fields, stream K enriched rows back. No separate `FetchDocs` RPC — it's just another stage in the plan.

4. **Serialization is trivial.** Each scan node serializes independently via `PhysicalExtensionCodec`. Joins, aggregations, sorts are stock DF operators that serialize natively. No monolithic custom node to encode/decode.

5. **Dynamic filter pushdown crosses the join boundary.** Same mechanism as bhandle membership filtering in the metrics system. The distributed optimizer preserves this because it's a standard DF feature.

### PhysicalExtensionCodec

Each provider needs a simple codec entry:

```rust
impl PhysicalExtensionCodec for TantivyCodec {
    fn try_encode(&self, node: &dyn ExecutionPlan, buf: &mut Vec<u8>) -> Result<()> {
        // Each provider serializes its own config:
        //   FastFieldProvider: schema + projection
        //   InvertedIndexProvider: serialized tantivy query + field name
        //   StoredFieldProvider: stored column names
        //
        // The tantivy::Index is NOT serialized.
        // Workers open it from local storage/cache.
    }
}
```

---

## quickwit-datafusion Layer

At the quickwit layer, each provider gets wrapped with split-specific concerns:

| tantivy-datafusion (open source) | quickwit-datafusion (quickwit-specific) |
|---|---|
| `TantivyFastFieldProvider` — reads fast fields from a `tantivy::Index` | `QuickwitFastFieldProvider` — opens split bundles, warms caches, delegates to `TantivyFastFieldProvider` |
| `TantivySearchFunction` — runs inverted index queries on a `tantivy::Index` | `QuickwitSearchFunction` — resolves splits from metastore, handles tokenizer config |
| `TantivyStoredFieldProvider` — reads doc store from a `tantivy::Index` | `QuickwitStoredFieldProvider` — opens split bundles, handles doc-to-JSON via DocMapper |

The tantivy-datafusion providers work with any `tantivy::Index` (including `Index::create_in_ram()` for testing). The quickwit layer adds:
- Split bundle opening (`BundleDirectory`, `HotDirectory`)
- Warmup orchestration (pre-fetch byte ranges from S3)
- Cache integration (`SearcherContext`, `ByteRangeCache`)
- Split resolution from metastore (time range + tag pruning)
- Dynamic schema union across splits

---

## UDFs and the Two Entry Points for Full-Text Search

Full-text search has two entry points that produce the same physical plan. Users choose based on ergonomics.

### Entry 1: `match_query` UDF in WHERE (ergonomic)

```sql
SELECT timestamp, service
FROM my_index
WHERE match_query(message, 'error') AND status >= 500
ORDER BY timestamp DESC
LIMIT 10
```

`match_query(field, query) → Boolean` is registered as a scalar UDF, but it's a **marker** — the decompose rule recognizes it in the filter list, extracts it, and rewires the plan to use the inverted index provider via a join. The UDF itself never evaluates at the Arrow level in the normal path.

The decompose rule expands it:

```
Before rule:
  Filter(match_query(message, 'error') AND status >= 500)
    └── TableScan(my_index)

After rule:
  Filter(status >= 500)                                    ← DF evaluates on Arrow
    └── HashJoin(ON _doc_id, _segment_ord)
          ├── tantivy_search('message', 'error')           ← inverted index
          └── TableScan(my_index__fast)                    ← fast fields
```

If the rule doesn't fire (e.g., `match_query` is used on a non-tantivy table), the UDF falls back to a string-contains evaluation on the column value — correct but slow.

### Entry 2: `tantivy_search` table function (explicit)

```sql
SELECT f.timestamp, f.service, s._score
FROM my_index f
JOIN tantivy_search('message', 'error') s
  ON f._doc_id = s._doc_id AND f._segment_ord = s._segment_ord
WHERE f.status >= 500
ORDER BY s._score DESC
LIMIT 10
```

The user controls the join explicitly. Useful when:
- Direct access to `_score` is needed (e.g., `ORDER BY _score DESC`)
- Complex multi-field search across different tables
- Programmatic query building where explicit joins are clearer

Both entry points produce the same physical plan — a hash join between the inverted index result and the fast field scan with dynamic filter pushdown.

### Other UDFs

| UDF | Signature | Description |
|---|---|---|
| `match_query` | `(field: Utf8, query: Utf8) → Boolean` | Full-text search marker. Decompose rule extracts it and wires up inverted index join. Fallback: string-contains. |
| `phrase_query` | `(field: Utf8, phrase: Utf8, slop: Int32) → Boolean` | Phrase search marker. Same decompose rule extraction. |
| `snippet` | `(source: Utf8, field: Utf8, query: Utf8) → Utf8` | Generates highlighted text fragments from `_source` column. Uses tantivy's `SnippetGenerator`. Lives in `ProjectionExec`. |
| `take_first` | `(list: List<T>) → T` | Extracts first element from a multi-valued field. For explicit coercion of `List<T>` to scalar. |

---

## Unified Table with Decompose Rule

Users shouldn't need to know about the three providers. They register one table (`my_index`) and write normal SQL. A logical `OptimizerRule` decomposes the plan behind the scenes.

### User experience

```sql
-- Register once
ctx.register_table("my_index", Arc::new(TantivyUnifiedProvider::new(index)));

-- Pure fast-field query — rule emits only fast field scan
SELECT timestamp, service, COUNT(*)
FROM my_index
WHERE status >= 500
GROUP BY service

-- Full-text search — rule adds inverted index join
SELECT timestamp, service
FROM my_index
WHERE match_query(message, 'error') AND status >= 500
ORDER BY timestamp DESC
LIMIT 10

-- Document content — rule adds stored field join
SELECT timestamp, _source
FROM my_index
WHERE match_query(message, 'error')
ORDER BY timestamp DESC
LIMIT 10
```

The user always writes `FROM my_index`. The decompose rule inspects the filters and projection to decide which providers are needed.

### How the rule works

`TantivyDecomposeRule` implements `OptimizerRule` (logical plan rewriting, runs before physical planning). It matches `TableScan` nodes backed by `TantivyUnifiedProvider` and rewrites them.

**Classification:**
1. **Filters** are classified into text predicates (`match_query(...)`) and fast field predicates (everything else). Text predicates go to the inverted index provider. Fast field predicates stay for DF to evaluate on Arrow.
2. **Projection** is inspected for stored-only columns (`_source`, or fields not in fast fields). If any are present, the stored field provider is joined in.

**Rewrite cases:**

**Case 1: No text predicates, no stored columns** (most common — aggregations, analytics)

```
Input:   TableScan(my_index, filter=[status >= 500], projection=[timestamp, service])
Output:  TableScan(my_index__fast, projection=[timestamp, service])
         DF applies FilterExec(status >= 500) on Arrow output
```

Only the fast field provider. All current tests hit this path.

**Case 2: Text predicates, no stored columns** (filtered analytics)

```
Input:   Filter(match_query(message, 'error') AND status >= 500)
           └── TableScan(my_index, projection=[timestamp, service])

Output:  Filter(status >= 500)
           └── HashJoin(ON _doc_id, _segment_ord)
                 ├── TableScan(tantivy_search('message', 'error'))
                 └── TableScan(my_index__fast, projection=[_doc_id, _segment_ord, timestamp, service])
```

Inverted index joined with fast fields. DF's dynamic filter pushdown pushes the doc ID membership from the search result into the fast field scan. The `status >= 500` stays as a DF filter on Arrow.

**Case 3: Text predicates + stored columns** (search with document content)

```
Input:   Filter(match_query(message, 'error'))
           └── TableScan(my_index, projection=[timestamp, _source])

Output:  HashJoin(ON _doc_id, _segment_ord)                          ← stored enrichment
           ├── TableScan(my_index__stored, projection=[_source])
           └── HashJoin(ON _doc_id, _segment_ord)                    ← text search
                 ├── TableScan(tantivy_search('message', 'error'))
                 └── TableScan(my_index__fast, projection=[_doc_id, _segment_ord, timestamp])
```

Three-way composition. After the rule fires, DF's standard optimizer handles join ordering — it will push the stored field join above TopK so only K rows get their `_source` fetched.

**Case 4: No text predicates, stored columns** (full scan with document content)

```
Input:   TableScan(my_index, projection=[timestamp, _source], limit=10)

Output:  HashJoin(ON _doc_id, _segment_ord)
           ├── TableScan(my_index__stored, projection=[_source])
           └── TableScan(my_index__fast, projection=[_doc_id, _segment_ord, timestamp], limit=10)
```

Fast field scan + stored field enrichment. No inverted index.

### Rule implementation sketch

```rust
pub struct TantivyDecomposeRule;

impl OptimizerRule for TantivyDecomposeRule {
    fn name(&self) -> &str {
        "tantivy_decompose"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|node| {
            // Match Filter → TableScan(TantivyUnifiedProvider)
            // or bare TableScan(TantivyUnifiedProvider)
            let (scan, filters) = match &node {
                LogicalPlan::Filter(filter) => {
                    if let LogicalPlan::TableScan(scan) = filter.input().as_ref() {
                        if !is_tantivy_unified(scan) {
                            return Ok(Transformed::no(node));
                        }
                        (scan, extract_conjuncts(&filter.predicate))
                    } else {
                        return Ok(Transformed::no(node));
                    }
                }
                LogicalPlan::TableScan(scan) if is_tantivy_unified(scan) => {
                    (scan, vec![])
                }
                _ => return Ok(Transformed::no(node)),
            };

            let provider = get_tantivy_provider(scan);

            // 1. Classify filters
            let (text_preds, other_preds) = classify_predicates(&filters);

            // 2. Check projection for stored fields
            let needs_stored = scan.projection.as_ref()
                .map(|cols| cols.iter().any(|&i| provider.is_stored_only(i)))
                .unwrap_or(false);

            // 3. Build fast field scan (always present)
            let mut result = build_fast_field_scan(scan, provider);

            // 4. If text predicates exist, join with inverted index
            if !text_preds.is_empty() {
                let search_scan = build_search_scan(&text_preds, provider);
                result = build_join(result, search_scan, &["_doc_id", "_segment_ord"]);
            }

            // 5. If stored fields needed, join with stored provider
            if needs_stored {
                let stored_scan = build_stored_scan(scan, provider);
                result = build_join(result, stored_scan, &["_doc_id", "_segment_ord"]);
            }

            // 6. Re-apply non-text filters (DF evaluates on Arrow)
            if !other_preds.is_empty() {
                result = LogicalPlan::Filter(Filter::try_new(
                    conjunction(other_preds),
                    Arc::new(result),
                )?);
            }

            Ok(Transformed::yes(result))
        })
    }
}
```

### Registration

```rust
// When building the SessionContext:
let mut ctx = SessionContext::new();

// Register the decompose rule
ctx.add_optimizer_rule(Arc::new(TantivyDecomposeRule));

// Register the unified table (user-facing)
ctx.register_table("my_index", Arc::new(TantivyUnifiedProvider::new(index.clone())));

// Register internal providers (hidden from user, used by the rule)
ctx.register_table("my_index__fast", Arc::new(TantivyFastFieldProvider::new(index.clone())));
ctx.register_table("my_index__stored", Arc::new(TantivyStoredFieldProvider::new(index.clone())));
ctx.register_udtf("tantivy_search", Arc::new(TantivySearchFunction::new(index.clone())));
```

### Why this matters for incremental development

The decompose rule is additive. Each case can be implemented independently:

1. **Start with Case 1 only** (fast fields, no text search, no stored fields). This is what the current tests exercise. The rule simply rewrites `TableScan(my_index)` → `TableScan(my_index__fast)`. All existing tests pass unchanged.

2. **Add Case 2** when the inverted index provider is built. The rule starts recognizing `match_query(...)` in filters and adds the join. New tests verify text search + dynamic filter pushdown.

3. **Add Case 3/4** when the stored field provider is built. The rule starts recognizing stored columns in the projection and adds the stored field join.

At each step, earlier tests continue to pass because the rule only adds joins when text predicates or stored columns are detected. Pure fast-field queries never see the inverted index or stored field providers.

---

## What the Fast Field Provider Does NOT Do

To be explicit about what was removed from the monolithic model:

- **No `expr_to_tantivy.rs` conversion.** SQL `WHERE` clauses on fast fields are evaluated by DataFusion on Arrow columns, not converted to tantivy queries.
- **No inverted index queries inside `scan()`.** Text search is a separate provider, composed via joins.
- **No stored field reads.** Document content is a separate provider.
- **No BM25 scoring.** Scores come from the inverted index provider.
- **No custom `ExecutionPlan`.** Uses `LazyMemoryExec` from stock DataFusion.
- **No custom TopK optimizer rule.** DataFusion's built-in `SortExec` + `GlobalLimitExec` handles `ORDER BY ... LIMIT K`. The limit hint is passed to `scan()` for early termination.

---

## Implementation Status

### Done (current crate)
- [x] Fast field → Arrow conversion for all 8 types
- [x] Multi-valued field detection and `List<T>` support
- [x] `TantivyTableProvider` with projection, limit pushdown
- [x] Basic SQL: SELECT, WHERE (on fast fields), ORDER BY, GROUP BY, LIMIT

### Phase 1: Refactor to decomposed model (fast fields only)
- [ ] Remove `TantivyFastFieldExec` — replace with `LazyMemoryExec` in `scan()`
- [ ] Remove `expr_to_tantivy.rs` — let DF filter on Arrow columns
- [ ] Add `_doc_id` and `_segment_ord` metadata columns to fast field output
- [ ] Rename to `TantivyFastFieldProvider`
- [ ] Add `TantivyUnifiedProvider` (thin wrapper, delegates to fast field provider)
- [ ] Add `TantivyDecomposeRule` — Case 1 only (rewrites to fast field scan)
- [ ] Verify all 13 existing tests pass unchanged

### Phase 2: Inverted index provider
- [ ] `TantivySearchFunction` (table function) — inverted index search
- [ ] `TantivyDecomposeRule` — Case 2 (text predicates → inverted index join)
- [ ] Tests: full-text search + fast field filtering with dynamic filter pushdown

### Phase 3: Stored field provider
- [ ] `TantivyStoredFieldProvider` — stored field enrichment
- [ ] `TantivyDecomposeRule` — Cases 3 & 4 (stored columns → stored field join)
- [ ] `snippet` UDF
- [ ] `take_first` UDF
- [ ] Tests: document retrieval, snippet generation
