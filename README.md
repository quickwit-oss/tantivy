![Tantivy](https://tantivy-search.github.io/logo/tantivy-logo.png)

[![Build Status](https://travis-ci.org/tantivy-search/tantivy.svg?branch=master)](https://travis-ci.org/tantivy-search/tantivy)
[![codecov](https://codecov.io/gh/tantivy-search/tantivy/branch/master/graph/badge.svg)](https://codecov.io/gh/tantivy-search/tantivy)
[![Join the chat at https://gitter.im/tantivy-search/tantivy](https://badges.gitter.im/tantivy-search/tantivy.svg)](https://gitter.im/tantivy-search/tantivy?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build status](https://ci.appveyor.com/api/projects/status/r7nb13kj23u8m9pj/branch/master?svg=true)](https://ci.appveyor.com/project/fulmicoton/tantivy/branch/master)


**Tantivy** is a **full text search engine library** written in rust.

It is closer to Lucene than to Elastic Search and Solr in the sense it is not
an off-the-shelf search engine server, but rather a crate that can be used
to build such a search engine.

Tantivy is, in fact, strongly inspired by Lucene's design.

# Features

- Full-text search
- Tiny startup time (<10ms), perfect for command line tools
- BM25 scoring (the same as lucene)
- Basic query language (`+michael +jackson`)
- Phrase queries search (\"michael jackson\"`)
- Incremental indexing
- Multithreaded indexing (indexing English Wikipedia takes < 3 minutes on my desktop)
- Mmap directory
- SIMD integer compression when the platform/CPU includes the SSE2 instruction set.
- Single valued and multivalued u64 and i64 fast fields (equivalent of doc values in Lucene)
- `&[u8]` fast fields
- LZ4 compressed document store
- Range queries
- Faceted search
- Configurable indexing (optional term frequency and position indexing
- Cheesy logo with a horse

# Non-features

- Distributed search and will not be in the scope of tantivy.


# Supported OS and compiler

Tantivy works on stable rust (>= 1.27) and supports Linux, MacOS and Windows.

# Getting started

- [tantivy's simple search example](http://fulmicoton.com/tantivy-examples/simple_search.html)
- [tantivy-cli and its tutorial](https://github.com/tantivy-search/tantivy-cli).
`tantivy-cli` is an actual command line interface that makes it easy for you to create a search engine,
index documents and search via the CLI or a small server with a REST API.
It will walk you through getting a wikipedia search engine up and running in a few minutes.
- [reference doc]
    - [For the last released version](https://docs.rs/tantivy/)
    - [For the last master branch](https://tantivy-search.github.io/tantivy/tantivy/index.html)

# Compiling

## Development

Tantivy compiles on stable rust but requires `Rust >= 1.27`.
To check out and run tests, you can simply run :

    git clone git@github.com:tantivy-search/tantivy.git
    cd tantivy
    cargo build


# Contribute

Send me an email (paul.masurel at gmail.com) if you want to contribute to tantivy.
