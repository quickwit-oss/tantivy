#/bin/bash
valgrind --tool=cachegrind target/release/tantivy-bench -i /data/wiki-index -q ./queries.txt -n 3
valgrind --tool=callgrind target/release/tantivy-bench -i /data/wiki-index -q ./queries.txt -n 3


