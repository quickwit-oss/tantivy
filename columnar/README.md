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

# Format

A quickwit/tantivy style sstable associated
`(column names, column_cardinality, column_type) to range of bytes.

The format of the key is:
`[column_name][ZERO_BYTE][column_type_header: u8]`

Column name may not contain the zero byte.

Listing all columns associated to `column_name` can therefore
be done by listing all keys prefixed by
`[column_name][ZERO_BYTE]`

The associated range of bytes refer to a range of bytes

