# zero to one
* merges
* full still needs a num_values
* replug u128
* add dictionary encoded stuff
* fix multivalued
* find a way to make columnar work with strict types
* plug to tantivy
    - indexing
    - aggregations
    - merge

# Perf and Size
* re-add ZSTD compression for dictionaries
no systematic monotonic mapping
consider removing multilinear
f32?
adhoc solution for bool?

add metrics helper for aggregate. sum(row_id)
review inline absence/presence
improv perf of select using PDEP
compare with roaring bitmap/elias fano etc etc.
SIMD range? (see blog post)
Add alignment?
Consider another codec to bridge the gap between few and 5k elements

# Cleanup and rationalization
in benchmark, unify percent vs ratio, f32 vs f64.
investigate if should have better errors? io::Error is overused at the moment.
rename rank/select in unit tests
Review the public API via cargo doc
go through TODOs
remove all  doc_id occurences -> row_id
use the rank & select naming in unit tests branch.
multi-linear -> blockwise
linear codec -> simply a multiplication for the index column
rename columnar to something more explicit, like column_dictionary or columnar_table

# Other
fix enhance column-cli

# Santa claus

autodetect datetime ipaddr, plug customizable tokenizer.

