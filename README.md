
[![Build Status](https://travis-ci.org/tantivy-search/tantivy.svg?branch=master)](https://travis-ci.org/tantivy-search/tantivy)
[![codecov](https://codecov.io/gh/tantivy-search/tantivy/branch/master/graph/badge.svg)](https://codecov.io/gh/tantivy-search/tantivy)
[![Join the chat at https://gitter.im/tantivy-search/tantivy](https://badges.gitter.im/tantivy-search/tantivy.svg)](https://gitter.im/tantivy-search/tantivy?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build status](https://ci.appveyor.com/api/projects/status/r7nb13kj23u8m9pj/branch/master?svg=true)](https://ci.appveyor.com/project/fulmicoton/tantivy/branch/master)
[![Crates.io](https://img.shields.io/crates/v/tantivy.svg)](https://crates.io/crates/tantivy)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/fulmicoton)

![Tantivy](https://tantivy-search.github.io/logo/tantivy-logo.png)

[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/0)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/0)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/1)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/1)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/2)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/2)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/3)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/3)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/4)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/4)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/5)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/5)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/6)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/6)
[![](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/images/7)](https://sourcerer.io/fame/fulmicoton/tantivy-search/tantivy/links/7)

[![Become a patron](https://c5.patreon.com/external/logo/become_a_patron_button.png)](https://www.patreon.com/fulmicoton)


**Tantivy** is a **full text search engine library** written in rust.

It is closer to [Apache Lucene](https://lucene.apache.org/) than to [Elasticsearch](https://www.elastic.co/products/elasticsearch) and [Apache Solr](https://lucene.apache.org/solr/) in the sense it is not
an off-the-shelf search engine server, but rather a crate that can be used
to build such a search engine.

Tantivy is, in fact, strongly inspired by Lucene's design.

# Benchmark

Tantivy is typically faster than Lucene, but the results will depend on 
the nature of the queries in your workload.

The following [benchmark](https://tantivy-search.github.io/bench/) break downs 
performance for different type of queries / collection.

# Features

- Full-text search
- Configurable tokenizer. (stemming available for 17 latin languages. Third party support for Chinese ([tantivy-jieba](https://crates.io/crates/tantivy-jieba) and [cang-jie](https://crates.io/crates/cang-jie)) and [Japanese](https://crates.io/crates/tantivy-tokenizer-tiny-segmenter)
- Fast (check out the :racehorse: :sparkles: [benchmark](https://tantivy-search.github.io/bench/) :sparkles: :racehorse:)
- Tiny startup time (<10ms), perfect for command line tools
- BM25 scoring (the same as lucene)
- Natural query language `(michael AND jackson) OR "king of pop"`
- Phrase queries search (`"michael jackson"`)
- Incremental indexing
- Multithreaded indexing (indexing English Wikipedia takes < 3 minutes on my desktop)
- Mmap directory
- SIMD integer compression when the platform/CPU includes the SSE2 instruction set.
- Single valued and multivalued u64 and i64 fast fields (equivalent of doc values in Lucene)
- `&[u8]` fast fields
- Text, i64, u64, dates and hierarchical facet fields
- LZ4 compressed document store
- Range queries
- Faceted search
- Configurable indexing (optional term frequency and position indexing)
- Cheesy logo with a horse

# Non-features

- Distributed search is out of the scope of tantivy. That being said, tantivy is meant as a
library upon which one could build a distributed search. Serializable/mergeable collector state for instance, 
are within the scope of tantivy.

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

# How can I support this project?

There are many ways to support this project. 

- Use tantivy and tell us about your experience on [gitter](https://gitter.im/tantivy-search/tantivy) or by email (paul.masurel@gmail.com)
- Report bugs
- Write a blog post
- Help with documentation by asking questions or submitting PRs
- Contribute code (you can join [our gitter](https://gitter.im/tantivy-search/tantivy) )
- Talk about tantivy around you
- Drop a word on on [![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/fulmicoton) or even [![Become a patron](https://c5.patreon.com/external/logo/become_a_patron_button.png)](https://www.patreon.com/fulmicoton)

# Contributing code

We use the GitHub Pull Request workflow - reference a GitHub ticket and/or include a comprehensive commit message when opening a PR.

## Clone and build locally

Tantivy compiles on stable rust but requires `Rust >= 1.27`.
To check out and run tests, you can simply run :

```bash
    git clone https://github.com/tantivy-search/tantivy.git
    cd tantivy
    cargo build
```

## Run tests

Some tests will not run with just `cargo test` because of `fail-rs`.
To run the tests exhaustively, run `./run-tests.sh`

## Debug

You might find it useful to step through the programme with a debugger.

### A failing test

Make sure you haven't run `cargo clean` after the most recent `cargo test` or `cargo build` to guarantee that `target/` dir exists. Use this bash script to find the most name of the most recent debug build of tantivy and run it under rust-gdb.

```bash
find target/debug/ -maxdepth 1 -executable -type f -name "tantivy*" -printf '%TY-%Tm-%Td %TT %p\n' | sort -r | cut -d " " -f 3 | xargs -I RECENT_DBG_TANTIVY rust-gdb RECENT_DBG_TANTIVY
```

Now that you are in rust-gdb, you can set breakpoints on lines and methods that match your source-code and run the debug executable with flags that you normally pass to `cargo test` to like this

```bash
$gdb run --test-threads 1 --test $NAME_OF_TEST
```

### An example

By default, rustc compiles everything in the `examples/` dir in debug mode. This makes it easy for you to make examples to reproduce bugs.

```bash
rust-gdb target/debug/examples/$EXAMPLE_NAME
$ gdb run
```
