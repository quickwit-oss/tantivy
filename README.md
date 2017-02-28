![Tantivy](https://tantivy-search.github.io/logo/tantivy-logo.png)

[![Build Status](https://travis-ci.org/tantivy-search/tantivy.svg?branch=master)](https://travis-ci.org/tantivy-search/tantivy)
[![Coverage Status](https://coveralls.io/repos/github/tantivy-search/tantivy/badge.svg?branch=master&refresh1)](https://coveralls.io/github/tantivy-search/tantivy?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Join the chat at https://gitter.im/tantivy-search/tantivy](https://badges.gitter.im/tantivy-search/tantivy.svg)](https://gitter.im/tantivy-search/tantivy?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
![beacon for google analytics](https://ga-beacon.appspot.com/UA-88834340-1/tantivy/README)

**Tantivy** is a **full text search engine library** written in rust.

It is strongly inspired by Lucene's design.


# Features

- configurable indexing (optional term frequency and position indexing)
- tf-idf scoring
- Basic query language
- Phrase queries
- Incremental indexing
- Multithreaded indexing (indexing English Wikipedia takes 4 minutes on my desktop)
- mmap based
- optional SIMD integer compression
- u32 fast fields (equivalent of doc values in Lucene)
- LZ4 compressed document store
- Cheesy logo with a horse

Tantivy supports Linux and MacOS. Windows is not supported.

# Getting started

- [tantivy's usage example](http://fulmicoton.com/tantivy-examples/simple_search.html)
- [tantivy-cli and its tutorial](https://github.com/tantivy-search/tantivy-cli).
It will walk you through getting a wikipedia search engine up and running in a few minutes.
- [reference doc](https://tantivy-search.github.io/tantivy/tantivy/index.html).

# Compiling 

Tantivy requires Rust Nightly because it uses requires the features [`box_syntax`](https://doc.rust-lang.org/stable/book/box-syntax-and-patterns.html), [`optin_builtin_traits`](https://github.com/rust-lang/rfcs/blob/master/text/0019-opt-in-builtin-traits.md), and [`conservative_impl_trait`](https://github.com/rust-lang/rfcs/blob/master/text/1522-conservative-impl-trait.md).
By default, `tantivy` uses a git submodule called `simdcomp`.
After cloning the repository, you will need to initialize and update
the submodules. The project can then be built using `cargo`.

    git clone git@github.com:tantivy-search/tantivy.git
    cd tantivy
    cargo build


Alternatively, if you are trying to compile `tantivy` without simd compression,
you can disable this functionality. In this case, this submodule is not required
and you can compile tantivy by using the `--no-default-features` flag.

    cargo build --no-default-features 


# Contribute

Send me an email (paul.masurel at gmail.com) if you want to contribute to tantivy. 
