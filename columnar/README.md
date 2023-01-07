# Columnar format

This crate describes columnar format used in tantivy.

## Goals

This format is special in the following way.
- it needs to be compact
- it does not required to be loaded in memory.
- it is designed to fit well with quickwit's strange constraint:
we need to be able to load columns rapidly.
- columns of several types can be associated with the same column name.
- it needs to support columns with different types `(str, u64, i64, f64)`
and different cardinality `(required, optional, multivalued)`.
- columns, once loaded, offer cheap random access.

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
The `(column_name, columne_type)` couple however uniquely identifies a column.
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

