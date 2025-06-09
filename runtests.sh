#! /bin/bash

cargo +stable nextest run --features quickwit,mmap,stopwords,lz4-compression,zstd-compression,failpoints --verbose --workspace
