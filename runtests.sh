#! /bin/bash

cargo +stable nextest run --features mmap,stopwords,lz4-compression,zstd-compression,failpoints --verbose --workspace
