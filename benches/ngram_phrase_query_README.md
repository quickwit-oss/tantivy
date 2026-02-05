# Ngram Phrase Query Benchmark

This benchmark measures the performance impact of word ngram indexing on phrase queries in Tantivy.

## What it Benchmarks

- **Phrase query performance** with and without ngram indexing
- **Different ngram configurations**:
  - No ngrams (baseline)
  - Bigrams FF only (Frequent-Frequent pairs)
  - All bigrams (FF, FR, RF patterns)
  - Bigrams + Trigrams (FFF pattern)
- **Various phrase lengths**: 2-word, 3-word, and 4-word phrases
- **Different corpus sizes**: 10K, 50K, and 100K documents

## Running the Benchmark

Run the full benchmark suite:
```bash
cargo bench --bench ngram_phrase_query
```

Run a quick test (smaller corpus):
```bash
cargo bench --bench ngram_phrase_query -- "Small"
```

## Understanding Results

### Performance Metrics
- **Throughput**: Operations per second (higher is better)
- **Latency**: Time per operation (lower is better)

### Expected Improvements
With ngram indexing enabled:
- **2-word phrases**: 10-60x faster (bigrams provide direct term lookups)
- **3-word phrases**: 10-40x faster with bigrams (position intersection still needed)
- **4-word phrases**: 15-30x faster (benefits from optimized 2-word components)

### Index Size Trade-offs
Measured by term count increase:
- **Bigrams FF**: ~5,000-6,000% more terms (only frequent-frequent pairs)
- **All bigrams**: ~5,000-6,000% more terms (all frequency patterns)
- **⚠️ With trigrams**: ~300,000%+ more terms (54x larger than bigrams!)

### ⚠️ Important Finding: Trigrams Are Often Not Worth It

Benchmark results show that **trigrams are extremely expensive** with **diminishing or negative returns**:

- **Index size**: 54x larger than bigrams (188K vs 3.5K terms)
- **Performance**: Often SLOWER than bigrams for 3-word phrases
  - Trigrams add overhead in query planning and intersection
  - Bigrams + position matching is often faster than trigram lookup
- **Recommendation**: Stick with bigrams (FF or All) for best performance/size trade-off

## Benchmark Structure

### Corpus Generation
- Documents contain 50-200 words each
- Word frequency distribution:
  - 60% frequent terms (the, a, in, of, etc.)
  - 30% medium frequency terms (world, people, time, etc.)
  - 10% rare terms (elephant, telescope, algorithm, etc.)

### Test Phrases
- **Frequent-Frequent**: "the quick", "in the"
- **Mixed frequency**: "world of", "telescope and"
- **Multi-word**: "the quick brown", "in the world of"

## Interpreting Results

Look for the speedup ratio between:
- `no_ngrams` vs `bigrams_ff` for frequent-frequent phrases (e.g., "the quick", "in the")
- `no_ngrams` vs `bigrams_all` for mixed frequency phrases  
- Compare `bigrams_*` vs `with_trigrams` to see if trigrams are worth the cost

The benchmark also prints index statistics showing:
- **Term count**: How many unique terms are indexed (including ngrams)
- **Term increase**: Percentage increase in dictionary size
- **⚠️ Watch for trigram explosion**: Trigrams often add 50-100x more terms than bigrams

**Key metric to watch**: If trigrams don't significantly outperform bigrams, they're not worth the massive index size increase.

## Example Output

```
2-word phrases — Small (10K docs)
  the_quick_no_ngrams      : 183.0 µs
  the_quick_bigrams_ff     : 14.6 µs   (12.5x faster)
  the_quick_bigrams_all    : 13.4 µs   (13.7x faster)
  
  in_the_no_ngrams         : 802.7 µs
  in_the_bigrams_ff        : 13.4 µs   (59.9x faster)

Small (10K docs) - Index Statistics:
  No ngrams:      1,234 terms, 10000 docs
  Bigrams (FF):   2,456 terms, 10000 docs (+99.0% terms)
  Bigrams (All):  3,678 terms, 10000 docs (+198.1% terms)
  With trigrams:  15,234 terms, 10000 docs (+1134.6% terms)
```

## Related Examples

- `examples/ngram_phrase_indexing.rs` - Basic ngram indexing usage
- `examples/word_ngram_search.rs` - Complete search example with ngrams

## Recommendations Based on Benchmark Results

**✅ DO use bigrams:**
- 10-60x speedup for phrase queries
- Moderate index size increase (~5,700% term growth)
- Clear performance wins across all phrase lengths

**❌ AVOID trigrams:**
- Minimal or negative performance benefit over bigrams
- Massive index explosion (54x larger than bigrams)
- Added query planning overhead often makes them slower
- Only consider if you have specific 3-word phrase patterns that dominate your queries

**Best configuration**: `WordNgramSet::new().with_ngram_ff().with_ngram_fr().with_ngram_rf()`
- Covers all bigram patterns for maximum phrase query speedup
- Reasonable index size trade-off
- No trigram overhead
