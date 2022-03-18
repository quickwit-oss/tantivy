[![Docs](https://docs.rs/tantivy/badge.svg)](https://docs.rs/crate/tantivy/)
[![Build Status](https://github.com/quickwit-oss/tantivy/actions/workflows/test.yml/badge.svg)](https://github.com/quickwit-oss/tantivy/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/quickwit-oss/tantivy/branch/main/graph/badge.svg)](https://codecov.io/gh/quickwit-oss/tantivy)
[![Join the chat at https://discord.gg/MT27AG5EVE](https://shields.io/discord/908281611840282624?label=chat%20on%20discord)](https://discord.gg/MT27AG5EVE)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Crates.io](https://img.shields.io/crates/v/tantivy.svg)](https://crates.io/crates/tantivy)

![Tantivy](https://tantivy-search.github.io/logo/tantivy-logo.png)

**Tantivy** is a **full text search engine library** written in Rust.

It is closer to [Apache Lucene](https://lucene.apache.org/) than to [Elasticsearch](https://www.elastic.co/products/elasticsearch) or [Apache Solr](https://lucene.apache.org/solr/) in the sense it is not
an off-the-shelf search engine server, but rather a crate that can be used
to build such a search engine.

Tantivy is, in fact, strongly inspired by Lucene's design.

If you are looking for an alternative to Elasticsearch or Apache Solr, check out [Quickwit](https://github.com/quickwit-oss/quickwit), our search engine built on Tantivy. 

# Benchmark

The following [benchmark](https://tantivy-search.github.io/bench/) break downs
performance for different type of queries / collection.

Your mileage WILL vary depending on the nature of queries and their load.

<img src="images/Benchmark Tantivy.png">

# Features

- Full-text search
- Configurable tokenizer (stemming available for 17 Latin languages with third party support for Chinese ([tantivy-jieba](https://crates.io/crates/tantivy-jieba) and [cang-jie](https://crates.io/crates/cang-jie)), Japanese ([lindera](https://github.com/lindera-morphology/lindera-tantivy), [Vaporetto](https://crates.io/crates/vaporetto_tantivy), and [tantivy-tokenizer-tiny-segmenter](https://crates.io/crates/tantivy-tokenizer-tiny-segmenter)) and Korean ([lindera](https://github.com/lindera-morphology/lindera-tantivy) + [lindera-ko-dic-builder](https://github.com/lindera-morphology/lindera-ko-dic-builder))
- Fast (check out the :racehorse: :sparkles: [benchmark](https://tantivy-search.github.io/bench/) :sparkles: :racehorse:)
- Tiny startup time (<10ms), perfect for command line tools
- BM25 scoring (the same as Lucene)
- Natural query language (e.g. `(michael AND jackson) OR "king of pop"`)
- Phrase queries search (e.g. `"michael jackson"`)
- Incremental indexing
- Multithreaded indexing (indexing English Wikipedia takes < 3 minutes on my desktop)
- Mmap directory
- SIMD integer compression when the platform/CPU includes the SSE2 instruction set
- Single valued and multivalued u64, i64, and f64 fast fields (equivalent of doc values in Lucene)
- `&[u8]` fast fields
- Text, i64, u64, f64, dates, and hierarchical facet fields
- LZ4 compressed document store
- Range queries
- Faceted search
- Configurable indexing (optional term frequency and position indexing)
- Cheesy logo with a horse
- JSON Field
- Aggregation Collector: range buckets, average, and stats metrics.
- LogMergePolicy with deletes
- Searcher Warmer API

## Non-features

Distributed search is out of the scope of Tantivy, but if you are looking for this feature, check out [Quickwit](https://github.com/quickwit-oss/quickwit/).


# Getting started

Tantivy works on stable Rust (>= 1.27) and supports Linux, MacOS, and Windows.

- [Tantivy's simple search example](https://tantivy-search.github.io/examples/basic_search.html)
- [tantivy-cli and its tutorial](https://github.com/quickwit-oss/tantivy-cli) - `tantivy-cli` is an actual command line interface that makes it easy for you to create a search engine,
index documents, and search via the CLI or a small server with a REST API.
It walks you through getting a wikipedia search engine up and running in a few minutes.
- [Reference doc for the last released version](https://docs.rs/tantivy/)

# How can I support this project?

There are many ways to support this project.

- Use Tantivy and tell us about your experience on [Discord](https://discord.gg/MT27AG5EVE) or by email (paul.masurel@gmail.com)
- Report bugs
- Write a blog post
- Help with documentation by asking questions or submitting PRs
- Contribute code (you can join [our Discord server](https://discord.gg/MT27AG5EVE))
- Talk about Tantivy around you

# Contributing code

We use the GitHub Pull Request workflow: reference a GitHub ticket and/or include a comprehensive commit message when opening a PR.

## Clone and build locally

Tantivy compiles on stable Rust but requires `Rust >= 1.27`.
To check out and run tests, you can simply run:

```bash
    git clone https://github.com/quickwit-oss/tantivy.git
    cd tantivy
    cargo build
```

## Run tests

Some tests will not run with just `cargo test` because of `fail-rs`.
To run the tests exhaustively, run `./run-tests.sh`.

## Debug

You might find it useful to step through the programme with a debugger.

### A failing test

Make sure you haven't run `cargo clean` after the most recent `cargo test` or `cargo build` to guarantee that the `target/` directory exists. Use this bash script to find the name of the most recent debug build of Tantivy and run it under `rust-gdb`:

```bash
find target/debug/ -maxdepth 1 -executable -type f -name "tantivy*" -printf '%TY-%Tm-%Td %TT %p\n' | sort -r | cut -d " " -f 3 | xargs -I RECENT_DBG_TANTIVY rust-gdb RECENT_DBG_TANTIVY
```

Now that you are in `rust-gdb`, you can set breakpoints on lines and methods that match your source code and run the debug executable with flags that you normally pass to `cargo test` like this:

```bash
$gdb run --test-threads 1 --test $NAME_OF_TEST
```

### An example

By default, `rustc` compiles everything in the `examples/` directory in debug mode. This makes it easy for you to make examples to reproduce bugs:

```bash
rust-gdb target/debug/examples/$EXAMPLE_NAME
$ gdb run
```
# Companies Using Tantivy 

<p align="left"><img align="center" src="data:image/png;base64,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" alt="quickwit_inc" height="30" width="auto" />&nbsp;
<img align="center" src="https://nuclia.com/wp-content/uploads/2021/12/logo.png" alt="quickwit-inc" height="30" width="auto" /> &nbsp;
<img align="center" src="https://assets.website-files.com/60d69ca3f2390194f99fa3bb/60e3028b7e285e210dab2194_sqare.png" alt="ucvzvurm2fidq1_ul0my85wa" height="30" width="auto" />
</p>

# FAQ
1. Can I use Tantivy in other languages?
- Python → [tantivy-py](https://github.com/quickwit-oss/tantivy-py)
- Ruby → [tantiny](https://github.com/baygeldin/tantiny)
2. What is an example of Tantivy use?

- One of the examples is “Tantiny”. It is a Ruby wrapper around Tantivy, which helps to search through huge static lists of geo locations + avoids long configuration of other search libraries like Sunspot. 
3. On average, how much faster is Tantivy compared to Lucene?
- According to our [benchmark](https://tantivy-search.github.io/bench/), Tantivy is approximately 2x faster than Lucene.