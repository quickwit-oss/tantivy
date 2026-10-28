# Aggregation collector contract

## Document ordering and bucket membership

- Each bucket receives strictly increasing document IDs across both collection methods, including nonconsecutive runs in `collect_multiple` and separate calls. Different buckets may receive the same document; their streams need not be ordered relative to each other.
- Range and histogram/date-histogram membership is deduplicated after bucket mapping, before counting or forwarding to children.
- Child metrics preserve raw field-value multiplicity, not repeated submissions of the same document to the same bucket.

Callers must prepare the submitted bucket IDs before collection.

## Buffered forwarding

Both buffer backends accumulate submissions across parent calls without sorting or deduplicating. For the same bucket, `[7]` followed by `[2]` or `[7]` violates the ordering contract. Downstream value loading relies on this ordering for contiguous-range decoding and missing-value handling.

Relevant code: `src/aggregation/buffered_sub_aggs.rs`, both `LowCardSubAggBuffer` and `HighCardSubAggBuffer`.

## Composite execution in multi-terms

Different values from a document can map to the same bucket key. For example, prices `[12, 15]` with histogram interval 10 and tags `["x", "y"]` produce these combinations before deduplication:

    (10, x), (10, y), (10, x), (10, y)

A source in the composite API is a named grouping rule: a field, a mapping such as terms or histogram intervals, and sort and missing-value settings. For example, grouping by product terms and price intervals of 10 can produce the key `{product: "shoe", price_band: 20}`. Composite translates these rules into a multi-terms aggregation and exposes its results through the composite API.

Multi-terms maps each grouping rule's column values to bucket sort values, which encode sort direction and missing-value placement. Terms values also encode column position. `DocumentBucketValues` buffers these values and their document IDs, deduplicating values across a grouping rule's physical columns for each document before combining values from different rules into bucket keys. Histogram and date-histogram values deduplicate after bucket mapping. Multi-terms applies exclusive pagination and stores the first page of keys.

Pagination filtering happens before candidate selection, including bounds absent from dictionaries or not exactly representable by numeric columns. Ordered expansion retains at most `size + 1` combinations per document at each stage: enough to fill the page even if the one prefix equal to `after` has no qualifying suffix. Bucket storage retains only `size` keys per parent bucket.

Relevant code: `src/aggregation/bucket/multi_terms/{mod,collector,key_order,sources,map}.rs`, the adapter in `src/aggregation/bucket/composite/mod.rs`, and `src/aggregation/agg_data.rs`.

## Terms with missing values

Terms requests ordered missing entries when it has children. A backwards linear merge preserves document order for multivalued columns, including when the missing fallback matches a real value.

Relevant code: `src/aggregation/bucket/term_agg/mod.rs`, `SegmentTermCollector::collect`.

## ValueSource output contract

Value loading relies on ascending output document IDs with each document's values grouped together. Values within a document need not be sorted. Full output is positionally aligned with input documents; its document-ID buffer is unspecified and must not be read.

Relevant code: `src/aggregation/value_source/mod.rs` and `src/aggregation/value_source/block_accessor.rs`.

## top_hits lifecycle limitation

Preparing a smaller maximum bucket ID can truncate existing bucket state and discard previously collected hits. Bucket document counts can remain populated while their hits disappear.

Relevant code: `src/aggregation/metric/top_hits.rs`, `TopHitsSegmentCollector::prepare_max_bucket`.
