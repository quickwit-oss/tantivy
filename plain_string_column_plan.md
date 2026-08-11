# Plain string and byte fast-field encoding plan

## Summary

Add a user-selectable payload encoding for string and byte fast fields:

```rust
pub enum PayloadEncoding {
    Dictionary,
    Plain,
}
```

`Dictionary` remains the default. `Plain` stores every value directly, compressed independently
with OnPair16, instead of assigning it a term ordinal in a sorted dictionary.

On the reader side, `StrColumn` and `BytesColumn` become enums. Dictionary-specific APIs remain on
dedicated `DictionaryEncoded*Column` types, while plain columns return decoded strings or byte
slices directly.

Backward compatibility with existing Tantivy schemas, columnar V1/V2 files, and existing indexes
is a required part of the feature.

## Goals

- Allow users to choose `Plain` or `Dictionary` for text fast fields.
- Allow byte fast fields to select the same encoding.
- Continue defaulting to dictionary encoding.
- Preserve random access to individual plain values.
- Make `PlainStrColumn` a lightweight UTF-8 wrapper around `PlainBytesColumn`.
- Read and merge existing V1/V2 dictionary-encoded columns without migration.
- Support optional, full, and multivalued columns, document reordering, and segment merges.
- Keep existing optimized dictionary paths when the column is dictionary encoded.

## Non-goals

- Automatically choosing an encoding based on field statistics.
- Making old Tantivy releases read the new V3 plain encoding. This is forward compatibility and
  cannot be provided by the new reader.
- Exposing artificial term ordinals for plain columns.
- Changing facet encoding. Facets depend on ordered ordinals and remain dictionary encoded.

## 1. Public encoding and schema API

### Shared encoding type

Define `PayloadEncoding` in `tantivy-columnar`, because the low-level writer, reader, merge code,
and on-disk format all need it. Re-export it from `tantivy::schema` for normal Tantivy users.

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum PayloadEncoding {
    #[default]
    Dictionary,
    Plain,
}
```

Use lowercase serde names (`"plain"` and `"dictionary"`).

### Text fields

Make `FastFieldTextOptions` public and add the encoding:

```rust
pub struct FastFieldTextOptions {
    pub tokenizer: String,
    pub encoding: PayloadEncoding,
}
```

`FastFieldTextOptions::default()` uses the raw tokenizer and dictionary encoding. Existing
`TextOptions::set_fast(tokenizer)` remains a convenience method that selects dictionary encoding.
Add an options-based setter or builder so users can select plain encoding without constructing
`TextOptions` internals.

Apply the same `FastFieldTextOptions` to string subcolumns produced by JSON fast fields.

Update `merge_fast_field_options` so an explicit plain encoding is not overwritten by the
implicit dictionary setting contributed by the `FAST` flag. Composition must remain deterministic
when tokenizer and encoding settings come from different operands.

### Byte fields

`BytesOptions` currently stores only a boolean fast-field flag. Add an encoding setting and a
method such as:

```rust
BytesOptions::set_fast_with_encoding(PayloadEncoding::Plain)
```

Keep `BytesOptions::set_fast()` and the `FAST` flag dictionary encoded by default.

Without this addition, schema-defined byte fields could never produce the proposed
`BytesColumn::Plain` variant.

### Schema serialization compatibility

Continue accepting all existing text fast-field representations:

```json
"fast": false
"fast": true
"fast": { "with_tokenizer": "raw" }
```

All of them imply `PayloadEncoding::Dictionary`. Dictionary settings should continue to serialize
using the legacy representation whenever possible. Plain encoding uses an extended object:

```json
"fast": {
  "with_tokenizer": "raw",
  "encoding": "plain"
}
```

Use equivalent custom serde for `BytesOptions`: old booleans imply dictionary encoding, while an
extended object records plain encoding. Add round-trip tests for old and new forms in both text
and JSON options.

## 2. Reader type hierarchy

Rename the current dictionary implementations to:

- `DictionaryEncodedBytesColumn`
- `DictionaryEncodedStrColumn`

Expose the logical column types as enums:

```rust
pub enum BytesColumn {
    DictionaryEncoded(DictionaryEncodedBytesColumn),
    Plain(PlainBytesColumn),
}

pub enum StrColumn {
    DictionaryEncoded(DictionaryEncodedStrColumn),
    Plain(PlainStrColumn),
}
```

`DictionaryEncodedStrColumn` remains a light wrapper around `DictionaryEncodedBytesColumn`.
`PlainStrColumn` similarly wraps `PlainBytesColumn` and is responsible only for UTF-8 conversion
and validation.

Keep these methods on the dictionary-specific types with their current behavior:

- `dictionary()`
- `ords()`
- `term_ords()`
- `ord_to_bytes()` / `ord_to_str()`
- `num_terms()`

The enums may delegate encoding-independent metadata such as:

- `num_rows()`
- `num_values()`
- `get_cardinality()`
- `column_index()`
- `payload_encoding()`

Also provide explicit downcasts:

```rust
fn as_dictionary_encoded(&self) -> Option<&DictionaryEncodedBytesColumn>;
fn as_plain(&self) -> Option<&PlainBytesColumn>;
```

The equivalent methods should exist on `StrColumn`.

### Plain value access

OnPair16 decompression needs an output buffer, so create a mutable accessor that owns both its
decode buffer and its most recently parsed block:

```rust
let mut accessor = column.accessor();

accessor.get_val(value_ord) -> io::Result<&[u8]>;
accessor.first(row_id) -> io::Result<Option<&[u8]>>;
```

`PlainStrColumnAccessor` exposes the corresponding methods returning `&str`. Returned values are
valid until the next mutable call on that accessor.

A normal `Iterator<Item = &[u8]>` cannot safely reuse a single mutable decompression buffer for a
multivalued row. Offer either:

- a callback-based `for_each_value(row_id, callback)` API, or
- an iterator over physical value ordinals plus `get_val()`.

The callback API is preferable for the common case because it does not expose ordinals as a
logical part of the plain-column API.

## 3. Plain column representation

`PlainBytesColumn` contains:

- A `ColumnIndex` mapping document rows to physical value positions.
- A range-readable `FileSlice` containing independently trained OnPair blocks.
- A resident block directory storing cumulative byte and value endpoints in separate boxed slices.

Each mutable accessor owns its most recently parsed, still-compressed block and reusable output
buffer. The underlying column remains immutable and shareable.

To read value `i`:

1. Binary-search the directory's contiguous cumulative `end_value` slice.
2. Range-read the selected block unless it is the cached block.
3. Read the block-local offsets for `i` and slice its native `u16` OnPair codes.
4. Decode only that value into the accessor's output buffer.
5. Return `&[u8]`, or validate and return `&str` through `PlainStrColumn`.

Fetching a value downloads its containing block but does not decompress the entire block or any
neighboring value. Column clones share the immutable block directory, while accessors have
independent caches.

### Validation

Opening or reading a plain column must reject:

- Unknown encoding discriminants.
- Truncated directories, blocks, or native OnPair buffers.
- Non-contiguous block byte ranges or non-increasing cumulative value ordinals.
- Non-monotonic or out-of-range offsets.
- An offsets count inconsistent with the column index/value count.
- Invalid OnPair16 tokens or model references.
- Invalid UTF-8 decoded through `PlainStrColumn`.

## 4. On-disk format and backward compatibility

Introduce columnar format `V3`.

### V1 and V2

For `ColumnType::Str` and `ColumnType::Bytes`, V1 and V2 readers always interpret the payload as
the existing dictionary layout. They do not look for an encoding byte. Existing bytes on disk are
therefore read exactly as they are today.

### V3

V3 string and byte payloads start with a stable encoding discriminant:

```text
encoding tag | encoding-specific payload
```

The one-byte encoding tags are fixed independently of the Rust enum declaration order:

```text
0 = Dictionary
1 = Plain
2..=255 = reserved; readers reject them
```

For `Dictionary`, the bytes after the tag use the V2 dictionary payload layout unchanged:

```text
0u8 | dictionary | column index and term ordinals | dictionary_num_bytes:u32 LE
```

`dictionary_num_bytes` counts only `dictionary`, excluding the encoding tag. For `Plain`, the V3
layout is:

```text
1u8
| column_index
| onpair_block_0 ... onpair_block_n
| block_directory[]
| column_index_num_bytes:u32 LE
| num_blocks:u32 LE
```

Each eight-byte block-directory entry is:

```text
block_num_bytes:u32 LE | end_value:u32 LE
```

`block_num_bytes` is the serialized size of one independently loadable block. Block addresses are
derived by checked cumulative addition, so the entire column may exceed `u32::MAX` bytes even
though an individual block may not. `end_value` is the exclusive cumulative physical value
ordinal. The final entry yields the column's value count, so it is not serialized separately. An
empty column has no blocks.

Each OnPair block is:

```text
dictionary_bytes_with_read_padding
| dictionary_offsets:u32 LE[]
| codes:u16 LE[]
| value_offsets:u32 LE[]
| dictionary_bytes_num_bytes:u32 LE
| dictionary_offsets_num_bytes:u32 LE
| codes_num_bytes:u32 LE
```

The directory determines the block's value count, so `value_offsets` must contain exactly one more
entry than the block has values. Its entries index the native code stream and its final entry equals
the number of codes. Native buffers are copied into aligned typed vectors after the block is fetched
because a `FileSlice` result does not guarantee `u16` or `u32` alignment.

Opening reads the fixed eight-byte footer and directory from the end, then eagerly opens only the
global column index. Block region lengths, cumulative addresses, native-buffer alignment, OnPair
invariants, codes, and local offsets are validated before decoding.

Blocks close after appending a complete value when they reach either
`PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD` (10 MiB of uncompressed value bytes) or
`PLAIN_BLOCK_MAX_NUM_VALUES` (262,144 values). Lowering the byte threshold reduces point-read
downloads and can improve compression when sorting makes blocks locally homogeneous, but produces
more dictionaries, directory entries, and block seeks. Raising it amortizes those costs over more
values, while increasing point-read downloads and potentially diluting sorting locality and its
compression benefit. The value-count cap independently bounds offsets for short or empty values.

V1 and V2 never consume a tag. V3 always consumes exactly one tag byte for string and byte
payloads, including dictionary payloads.

Other column types do not need an encoding tag and retain their existing V3 representation unless
the version implementation requires a uniform envelope.

### Compatibility tests

Expand `columnar/src/compat_tests.rs` fixtures so V1 and V2 include dictionary-encoded string and
byte columns for all supported cardinalities. The historical fixtures are
`v1_string_bytes.columnar` and `v2_string_bytes.columnar`. Tests must:

- Open and read old values through the new enum variants.
- Assert that old columns become `DictionaryEncoded`.
- Merge old columns with V3 dictionary columns.
- Merge old columns with V3 plain columns.
- Open existing Tantivy index compatibility fixtures and exercise their string fast fields.

No schema metadata should be required to decide how a stored column is decoded. The columnar
version and payload tag are authoritative.

## 5. Encoding-aware writer

Refactor the dictionary-only `StrOrBytesColumnWriter` into an internal enum:

```rust
enum PayloadColumnWriter {
    Dictionary(DictionaryEncodedColumnWriter),
    Plain(PlainColumnWriter),
}
```

Keep these low-level APIs source-compatible and dictionary encoded by default:

- `ColumnarWriter::record_str()`
- `ColumnarWriter::record_bytes()`
- `ColumnarWriter::record_column_type()`

Add an encoding-aware column registration method. Tantivy's `FastFieldsWriter` calls it while
walking the schema, before recording values.

The writer must reject attempts to register or record the same logical column using conflicting
encodings.

### Plain writer flow

1. Store raw input values in a dedicated contiguous per-column store rather than interning them in
   the term dictionary. Keep cumulative offsets beside the bytes so sorting and block serialization
   can borrow values directly without copying the entire column out of the memory arena.
2. Continue recording new-document/value operations in the memory arena so the existing
   cardinality and column-index builders can be reused.
3. Apply `old_to_new_row_ids` before serialization.
4. Sort values lexicographically within a row when requested.
5. Accumulate complete physical values until the 10 MiB raw-byte threshold or 262,144-value cap is
   reached.
6. Train and encode an independent native OnPair column for the block.
7. Serialize the block and append its byte length and cumulative `end_value` to the directory.
8. Serialize the index, blocks, directory, footer, and V3 encoding tag.

Include raw value storage, OnPair training structures, compressed buffers, and offsets in
`mem_usage()`.

Tantivy and `tantivy-columnar` use Rust 1.91 and depend on OnPair 0.2. The reader preserves OnPair's
native dictionary, `u16` code, and `u32` block-local offset representation and uses its validation
and random-access decoder.

## 6. Tantivy writer integration

During `FastFieldsWriter::from_schema_and_tokenizer_manager`:

- Read the text/bytes payload encoding from the field options.
- Register static string and byte columns with that encoding.
- Propagate JSON text encoding to dynamically created string subcolumns.
- Leave facet columns dictionary encoded regardless of text defaults.

Tokenizer behavior is independent of payload encoding: tokenization happens first, and each
resulting token is then recorded using the selected encoding.

Index-time sorting also runs before serialization. The plain writer should compare raw value-store
bytes directly, while the dictionary writer retains the current ordered-term-ID optimization.

## 7. Columnar and segment merging

The target encoding must be explicit when Tantivy merges segments. Replace the internal
`(String, ColumnType)` required-column description with a structure that can also carry
`PayloadEncoding`.

To preserve the existing public columnar merge API, keep `merge_columnar()` and add an extended
entry point accepting encoding overrides. The original entry point follows this policy:

- Preserve the encoding when all present input columns use the same encoding.
- Default an entirely missing required string/byte column to dictionary encoding.
- Use dictionary encoding for mixed inputs when no target was specified.

Tantivy's `IndexMerger`, which has access to the schema, always supplies the schema's target
encoding.

Implement an encoding-neutral source-value adapter used by merge code. It yields decoded bytes for
either source representation while retaining the existing optimized path where possible.

### Merge paths

- Dictionary inputs to dictionary output: retain the current streaming dictionary merge and
  ordinal remapping.
- Plain or mixed inputs to dictionary output: decode live values, rebuild the sorted dictionary,
  and emit remapped ordinals.
- Any inputs to plain output: decode live values in merge order, train a new OnPair16 model, and
  recompress them.

Both stack and shuffled merges must preserve missing rows, multivalued ranges, within-row order,
and deletion filtering.

### Index sorting across segments

The current merge sorter maps segment-local ordinals into a merged dictionary. Keep that fast path
when all sort columns are dictionary encoded.

For plain or mixed sort columns, compare decoded first values lexicographically. Start with reusable
per-segment scratch buffers; add caching or a temporary ordinal mapping only if benchmarks show
decompression during comparisons is too expensive.

## 8. Encoding-dependent consumers

Audit all direct uses of `dictionary()`, `ords()`, `term_ords()`, `ord_to_bytes()`, and
`ord_to_str()`.

### Columnar infrastructure

Update:

- `DynamicColumn::column_index`, cardinality, and value-count dispatch.
- `DynamicColumnHandle::open` and async opening.
- `open_u64_lenient`: dictionary string/byte columns may still expose ordinals, but plain columns
  must return no ordinal column and must never be interpreted as `u64` values.
- Space-usage reporting. Plain codec model bytes may be reported separately if the public API is
  extended; otherwise they remain part of the plain column footprint rather than the term-dictionary
  field.
- Columnar CLI inspection and debug formatting.

### Tantivy features

Implement encoding-specific paths for:

- Fast-field value retrieval.
- String and byte sort-key collectors.
- Index sorting and segment merge ordering.
- Top-hits and other collectors returning field values.
- String/byte range queries using direct lexicographic comparison for plain values.
- Terms aggregations using decoded byte/string keys rather than term ordinals.
- Cardinality aggregations by hashing decoded values.
- Include/exclude and regular-expression filtering over decoded strings.

Dictionary columns retain their current optimized ordinal-based implementations. Plain support
must remain correct even where it is less efficient; performance follow-ups can introduce
plain-specific caches after measurement.

Facet readers should destructure `StrColumn::DictionaryEncoded` and report data corruption if a
facet column is ever stored as plain.

## 9. Testing

### Schema tests

- Existing JSON schemas deserialize with dictionary encoding.
- Dictionary options serialize to the old shape.
- Plain text, JSON text, and bytes options round-trip.
- `FAST` composition does not overwrite explicit plain encoding.
- Default options remain dictionary encoded.

### Columnar round-trip tests

Run equivalent cases for strings and arbitrary bytes:

- Empty column.
- Empty payload value.
- Full, optional, and multivalued cardinalities.
- Duplicate and all-unique values.
- Long and non-ASCII strings.
- Arbitrary non-UTF-8 byte values.
- Document remapping.
- Sorted and unsorted values within rows.
- Direct first-value and multivalue access with scratch-buffer reuse.

### Merge tests

- Dictionary to dictionary.
- Plain to plain.
- Dictionary plus plain to each possible target encoding.
- V1/V2 dictionary plus V3 plain.
- Stack and shuffled order.
- Deleted rows.
- Missing columns and empty required columns.
- Index-sorted segment merges on plain string and byte fields.

### Corruption tests

- Unknown encoding tag.
- Truncated footer/model/payload.
- Invalid region lengths.
- Invalid or non-monotonic offsets.
- Invalid OnPair token.
- Invalid decoded UTF-8 for a string column.

### Tantivy integration tests

- Create, commit, reopen, and retrieve plain text/byte fast fields.
- Merge several segments and verify values.
- Exercise tokenizer output stored as plain.
- Exercise JSON string subcolumns.
- Run sorting, range queries, collectors, and terms/cardinality aggregations.
- Reopen and merge existing compatibility indexes.

## 10. Benchmarks

Compare `Plain` and `Dictionary` using low-cardinality, high-cardinality, and all-unique datasets.
Measure:

- Serialized size.
- Indexing throughput and peak memory.
- Random first-value access latency.
- Sequential scan throughput.
- Segment merge throughput and memory.
- Sorting and aggregation performance.

Include short strings, long strings, URLs, and arbitrary byte payloads. Record OnPair16 training
cost separately from value compression.

## Suggested implementation order

1. Add `PayloadEncoding`, schema APIs, and backward-compatible serde.
2. Introduce the reader enums and rename the existing dictionary types without changing their
   storage behavior.
3. Specify V3 and add V1/V2 string/byte compatibility fixtures.
4. Implement and test standalone `PlainBytesColumn`; wrap it with `PlainStrColumn`.
5. Implement the plain writer and low-level columnar round trips.
6. Propagate schema encoding through Tantivy's fast-field writer.
7. Reconsider byte/string column merge semantics across encodings. Decide whether merges between
   dictionary and plain columns are supported, how the target encoding is selected, and which
   combinations should return an explicit error.
8. Implement encoding-aware columnar merges and Tantivy segment merges according to that decision.
9. Revisit client usage sites such as aggregations, collectors, sorting, queries, facets, and
   fast-field value retrieval. Classify which APIs can be encoding neutral, which should retain a
   dictionary-only fast path, and which need explicit plain-column behavior or rejection.
10. Adapt ordinal-dependent queries, collectors, sorting, and aggregations according to that
    audit.
11. Complete corruption tests, compatibility-index tests, and benchmarks.
12. Improve cold OnPair block-opening performance. Profile the cost of range acquisition,
    native-buffer materialization, and validation separately; avoid eagerly copying and validating
    the entire code and value-offset streams for a point lookup; investigate retaining the block as
    `OwnedBytes`, lazily reading only the selected value's offsets and codes, borrowing dictionary
    bytes, and adding an alignment-independent zero-copy serialized view to OnPair if partial
    zero-copy is insufficient.

## Existing worktree note

There is an untracked early sketch under `columnar/src/column/plain/`. It already points toward a
`ColumnIndex + compressed payload + offsets` representation. Preserve and reconcile that work when
implementation starts rather than replacing it blindly.
