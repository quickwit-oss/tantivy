# Columnar format

This crate describes columnar format used in tantivy.

## Goals

This format is special in the following way.
- it needs to be compact
- accessing a specific column does not require to load the entire columnar. It can be done in 2 to 3 random access.
- columns of several types can be associated with the same column name.
- it needs to support columns with different types `(str, u64, i64, f64)`
and different cardinality `(required, optional, multivalued)`.
- columns, once loaded, offer cheap random access.
- it is designed to allow range queries.

# Coercion rules

Users can create a columnar by inserting rows to a `ColumnarWriter`,
and serializing it into a `Write` object.
Nothing prevents a user from recording values with different type to the same `column_name`.

In that case, `tantivy-columnar`'s behavior is as follows:
- JsonValues are grouped into 3 types (String, Number, bool).
Values that corresponds to different groups are mapped to different columns. For instance, String values are treated independently
from Number or boolean values. `tantivy-columnar` will simply emit several columns associated to a given column_name.
- Only one column for a given json value type is emitted.  If number values with different number types are recorded (e.g. u64, i64, f64),
`tantivy-columnar` will pick the first type that can represents the set of appended value, with the following prioriy order (`i64`, `u64`, `f64`).
`i64` is picked over `u64` as it is likely to  yield less change of types. Most use cases strictly requiring `u64` show the
restriction on 50% of the values (e.g. a 64-bit hash). On the other hand, a lot of use cases can show rare negative value.

# Columnar format

This columnar format may have more than one column (with different types) associated to the same `column_name` (see [Coercion rules](#coercion-rules) above).
The `(column_name, column_type)` couple however uniquely identifies a column.
That couple is serialized as a column `column_key`.  The format of that key is:
`[column_name][ZERO_BYTE][column_type_header: u8]`

```
COLUMNAR:=
    [COLUMNAR_DATA]
    [COLUMNAR_KEY_TO_DATA_INDEX]
    [COLUMNAR_FOOTER];


# Columns are sorted by their column key.
COLUMNAR_DATA:=
    [COLUMN_DATA]+;

COLUMNAR_FOOTER := [RANGE_SSTABLE_BYTES_LEN: 8 bytes little endian]

```

The columnar file starts by the actual column data, concatenated one after the other,
sorted by column key.

A sstable associates
`(column name, column_cardinality, column_type) to range of bytes.

Column name may not contain the zero byte `\0`.

Listing all columns associated to `column_name` can therefore
be done by listing all keys prefixed by
`[column_name][ZERO_BYTE]`

The associated range of bytes refer to a range of bytes

## Codecs of `str` and `bytes` columns

`str` and `bytes` columns have one of two codecs. The codec is part of the configuration: it is
never detected from the data.

- **Dictionary** (default): a sorted term dictionary (sstable) and a column of term ordinals.
- **Plain**: the values themselves, without term ordinals. Better suited to columns with a
  high cardinality.

The last byte of the column key is `column_type` for dictionary encoded and non-text columns,
and `0x10 | column_type` for plain-encoded columns (`0x13` for bytes, `0x14` for str).
Column keys must be inserted in sorted order, so a plain column is serialized after the other
columns with the same name.

The columnar footer has version 3 if, and only if, it contains at least one plain column.
Columnars without plain columns keep version 2, and older readers can still open them.

### Plain column layout

All integers are little endian.

```
PLAIN_COLUMN :=
    [BLOCK]*                                # read lazily, one block at a time
    [BLOCK_INDEX]
    [COLUMN_INDEX]                          # same format as for the other columns
    [BLOCK_INDEX_NUM_BYTES: u32]
    [COLUMN_INDEX_NUM_BYTES: u32]
    [PLAIN_FORMAT_VERSION: u8 = 1]

BLOCK_INDEX :=
    [NUM_BLOCKS: u32]
    [VALUE_STARTS: u32 * (NUM_BLOCKS + 1)]  # first value idx of each block, then num values
    [BYTE_STARTS: u64 * (NUM_BLOCKS + 1)]   # start of each block, then the blocks length
```

A block holds at most 65,536 values, and at most 1 MiB of raw bytes, unless it holds a single
larger value. Each block is self-contained: reading a value only requires reading its block.

```
ONPAIR16_BLOCK :=
    [KIND: u8 = 1]
    [CODE_BITS: u8]                         # ceil(log2(NUM_TOKENS)), in 1..=16
    [NUM_TOKENS - 1: u16]
    [DICT_BYTES_LEN: u32]
    [CODE_OFFSETS_LEN: u32]
    [NUM_CODES: u32]
    [TOKEN_LENS: 4 bits * NUM_TOKENS]       # token length - 1, token 2k in the low nibble
    [DICT_BYTES]                            # tokens concatenated, with the onpair read padding
    [CODE_OFFSETS]                          # u64 based column values, NUM_VALUES + 1 entries
    [CODES]                                 # CODE_BITS bits per code (raw u16 at 16 bits),
                                            # followed by 7 zero bytes

RAW_BLOCK :=                                # for small or incompressible blocks
    [KIND: u8 = 0]
    [RESERVED: 3 bytes]
    [OFFSETS_LEN: u32]
    [OFFSETS]                               # u64 based column values, NUM_VALUES + 1 entries
    [VALUE_BYTES]
```

Reading a value:
- doc -> value idx: the column index.
- value idx -> block: binary search in `VALUE_STARTS`.
- value idx -> codes: `CODES[CODE_OFFSETS[slot] .. CODE_OFFSETS[slot + 1]]`, with
  `slot = value idx - VALUE_STARTS[block]`.
- codes -> bytes: OnPair decoding with the dictionary of the block.

This crate exposes a columnar format for tantivy.
This format is described in README.md


The crate introduces the following concepts.

`Columnar` is an equivalent of a dataframe.
It maps `column_key` to `Column`.

A `Column<T>` associates a `RowId` (u32) to any
number of values.

This is made possible by wrapping a `ColumnIndex` and a `ColumnValue` object.
The `ColumnValue<T>` represents a mapping that associates each `RowId` to
exactly one single value.

The `ColumnIndex` then maps each RowId to a set of `RowId` in the
`ColumnValue`.

For optimization, and compression purposes, the `ColumnIndex` has three
possible representation, each for different cardinalities.

- Full

All RowId have exactly one value. The ColumnIndex is the trivial mapping.

- Optional

All RowIds can have at most one value. The ColumnIndex is the trivial mapping `ColumnRowId -> Option<ColumnValueRowId>`.

- Multivalued

All RowIds can have any number of values.
The column index is mapping values to a range.


All these objects are implemented an unit tested independently
in their own module:

- columnar
- column_index
- column_values
- column
