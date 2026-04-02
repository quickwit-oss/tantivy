# Storage Abstraction — Design Notes

## Problem

tantivy's query engine needs to work with pluggable `SegmentReader` implementations while preserving the monomorphized fast path that avoids `Box<dyn Postings>` vtable
overhead in tight scoring loops (`advance()`, `doc()`, `score()`) or similar cases.

## Requirements

- **Pluggable `SegmentReader`.** External crates can provide their own `SegmentReader` implementation (with their own `InvertedIndexReader`, postings types, etc.) and tantivy's query engine works with it.
- **No performance regression.** tantivy's default path (`SegmentPostings` → `TermScorer<SegmentPostings>` → block WAND) must remain monomorphized — no boxing, no vtable dispatch in scoring loops.
- **Arbitrary implementations without recompiling tantivy.** The design must not require a fixed set of implementations known at tantivy compile time. External crates depend on tantivy, not the reverse.
- **Query code is backend-agnostic.** Adding a new `SegmentReader` implementation must not require changes to `TermWeight`, `PhraseWeight`, `AutomatonWeight`, or any other query code.
- **Non-viral API.** `Searcher`, `Index`, `Weight`, and other public types are not generic over the backend. Users don't need to thread a type parameter through their code.

## Current Design

### Trait hierarchy

- **`SegmentReader`** — trait for accessing a segment's data. Returns `Arc<dyn DynInvertedIndexReader>` from `inverted_index(field)`. `TantivySegmentReader` is the default implementation.
- **`DynInvertedIndexReader`** — object-safe trait for dynamic dispatch. Returns `Box<dyn Postings>`. Used as `Arc<dyn DynInvertedIndexReader>`.
- **`InvertedIndexReader`** — typed trait with `type Postings` and `type DocSet` associated types. `TantivyInvertedIndexReader` implements this with `Postings = SegmentPostings`. There is a blanket impl of `InvertedIndexReader` for `dyn DynInvertedIndexReader` with `Postings = Box<dyn Postings>`.

### `try_downcast_and_call!` macro

The macro attempts to downcast `&dyn DynInvertedIndexReader` to `&TantivyInvertedIndexReader`. The body is compiled twice — once with the concrete reader (typed postings, monomorphized) and once with the dyn fallback (boxed postings).

```rust
try_downcast_and_call!(inverted_index.as_ref(), |reader| {
    let postings = reader.read_postings_from_terminfo(&term_info, option)?;
    TermScorer::new(postings, fieldnorm_reader, similarity_weight)
})
```

This replaced the earlier `TypedInvertedIndexReaderCb` trait + struct pattern, which required creating a struct for every call site to serve as a "generic closure."

## Rejected approaches

### Specialized methods on `DynInvertedIndexReader`

Adding methods like `build_term_scorer()`, `build_phrase_scorer()`, `fill_bitset_from_terminfo()` to `DynInvertedIndexReader` was rejected. This forces every implementor to reimplement scoring logic for each query type — a combinatorial explosion that couples the reader to every query shape. The reader should only know how to produce postings, not how to build scorers. It also prevents supporting arbitrary query types without changing the trait.

### Feature-gated types for external readers

Using `#[cfg(feature = "quickwit")]` branches in the macro to add additional downcast targets. Requires recompiling tantivy for each reader and doesn't scale to arbitrary `SegmentReader` / `InvertedIndexReader` implementations.

### Reader-side dispatch with a callback trait

A method like `fn with_typed_reader(&self, cb: &mut dyn TypedCb<R>) -> R` on `DynInvertedIndexReader` would let the reader dispatch the callback with its concrete type. But the generic `R` parameter makes the trait not object-safe. Working around this with type erasure (storing results in the callback via `Any`) is complex and fragile.

## Planned: `TypedSegmentReader` trait for external fast paths

The current `try_downcast_and_call!` hardcodes `TantivyInvertedIndexReader`. To give external crates the monomorphized fast path, the downcast target should be a **trait with associated types**, not a specific concrete struct.

```rust
trait TypedSegmentReader: SegmentReader {
    type InvertedIndexReader: InvertedIndexReader;
    // future: type FastFieldReader: ...;
    // future: type StoreReader: ...;

    fn typed_inverted_index(&self, field: Field) -> &Self::InvertedIndexReader;
}
```

The dispatch downcasts `dyn SegmentReader` (via `as_any()`) to a concrete type that implements `TypedSegmentReader`, then the body works generically through the associated types. The body is compiled once per registered concrete type but is written against the trait — it never names `TantivyInvertedIndexReader` or `SegmentPostings` directly.

- External crates implement `TypedSegmentReader` with their own associated types and get the monomorphized fast path.
- One dispatch point covers all typed sub-components (inverted index, fast fields, store reader, etc.).
- Query weight code is fully generic — adding a new backend doesn't touch any query code.
- This does **not** mean query-specific methods on `SegmentReader`. The trait provides typed access to sub-components, not knowledge of query shapes.

### Open question: downcast chain registration

The concrete type must still be known for the `Any` downcast. The dispatch needs a list of concrete types to try. Since tantivy cannot depend on external crates, this list can't live in tantivy itself.

A macro invoked by the final binary could generate the downcast chain with all `TypedSegmentReader` implementors. Not yet designed.
